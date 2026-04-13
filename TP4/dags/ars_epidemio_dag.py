"""
DAG : Pipeline surveillance epidemiologique ARS Occitanie
Source : IAS OpenHealth (Grippe + Gastro-enterite) -- data.gouv.fr
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

POSTGRES_ARS_CONN_ID = "postgres_ars"

default_args = {
    "owner": "ars-occitanie",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def _get_semaine(context):
    """Extrait la semaine ISO depuis l'execution_date."""
    exec_date = context["execution_date"]
    year, week, _ = exec_date.isocalendar()
    return f"{year}-S{week:02d}"


# --- Collecte des donnees IAS ---

def collecter_donnees_ias(**context):
    """Telecharge les CSV IAS et retourne le chemin du fichier JSON cree."""
    semaine = _get_semaine(context)
    archive_path = Variable.get("archive_base_path", default_var="/data/ars")
    output_dir = f"{archive_path}/raw"

    sys.path.insert(0, "/opt/airflow/scripts")
    from collecte_ias import (
        DATASETS_IAS, telecharger_csv_ias, filtrer_semaine,
        agreger_semaine, sauvegarder_donnees,
    )

    resultats = {}
    for syndrome, url in DATASETS_IAS.items():
        try:
            rows_all = telecharger_csv_ias(url)
            rows_sem = filtrer_semaine(rows_all, semaine)
            resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)
            logger.info("Collecte %s OK : %d jours", syndrome, resultats[syndrome]["nb_jours"])
        except Exception as e:
            logger.error("Erreur collecte %s : %s", syndrome, e)
            raise

    return sauvegarder_donnees(resultats, semaine, output_dir)


# --- Archivage et verification ---

def archiver_local(**context):
    """Copie le fichier brut dans la structure d'archivage partitionnee."""
    semaine = _get_semaine(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]

    chemin_source = context["task_instance"].xcom_pull(
        task_ids="collecte.collecter_donnees_sursaud"
    )
    if not chemin_source or not os.path.exists(chemin_source):
        raise FileNotFoundError(f"Fichier source introuvable : {chemin_source}")

    archive_dir = f"/data/ars/raw/{annee}/{num_sem}"
    os.makedirs(archive_dir, exist_ok=True)
    chemin_dest = f"{archive_dir}/sursaud_{semaine}.json"
    shutil.copy2(chemin_source, chemin_dest)
    logger.info("Archive creee : %s", chemin_dest)
    return chemin_dest


def verifier_archive(**context):
    """Verifie que le fichier d'archive existe et n'est pas vide."""
    semaine = _get_semaine(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]
    chemin = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"

    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Archive manquante : {chemin}")
    taille = os.path.getsize(chemin)
    if taille == 0:
        raise ValueError(f"Archive vide : {chemin}")
    logger.info("Archive valide : %s (%d octets)", chemin, taille)
    return True


# --- Calcul des indicateurs epidemiques ---

def calculer_indicateurs_epidemiques(**context):
    """Calcule z-score, R0 et classification pour chaque syndrome."""
    semaine = _get_semaine(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]

    chemin_brut = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    with open(chemin_brut, "r", encoding="utf-8") as f:
        donnees_brutes = json.load(f)

    sys.path.insert(0, "/opt/airflow/scripts")
    from calcul_indicateurs import (
        calculer_zscore, classifier_statut_ias,
        classifier_statut_zscore, classifier_statut_final,
        calculer_r0_simplifie,
    )

    seuil_alerte_z = float(Variable.get("seuil_alerte_zscore", default_var="1.5"))
    seuil_urgence_z = float(Variable.get("seuil_urgence_zscore", default_var="3.0"))

    hook = PostgresHook(postgres_conn_id=POSTGRES_ARS_CONN_ID)
    indicateurs_resultats = []
    syndromes_data = donnees_brutes.get("syndromes", {})

    for syndrome, data in syndromes_data.items():
        valeur_ias = data.get("valeur_ias")
        if valeur_ias is None:
            logger.warning("Pas de valeur IAS pour %s semaine %s", syndrome, semaine)
            continue

        seuil_min = data.get("seuil_min")
        seuil_max = data.get("seuil_max")

        # z-score par rapport a l'historique des 5 saisons
        historique = data.get("historique", {})
        hist_values = [v for v in historique.values() if v is not None]
        z_score = calculer_zscore(valeur_ias, hist_values)

        # R0 : on recupere les 4 dernieres semaines depuis la base
        try:
            rows = hook.get_records(
                """SELECT valeur_ias FROM donnees_hebdomadaires
                   WHERE syndrome = %s ORDER BY semaine DESC LIMIT 4""",
                parameters=(syndrome,)
            )
            series_r0 = [r[0] for r in reversed(rows)] + [valeur_ias]
        except Exception:
            series_r0 = [valeur_ias]

        duree_inf = 5 if syndrome == "GRIPPE" else 3
        r0 = calculer_r0_simplifie(series_r0, duree_inf)

        statut_ias = classifier_statut_ias(valeur_ias, seuil_min, seuil_max)
        statut_z = classifier_statut_zscore(z_score, seuil_alerte_z, seuil_urgence_z)
        statut_final = classifier_statut_final(statut_ias, statut_z)

        indicateurs_resultats.append({
            "semaine": semaine,
            "syndrome": syndrome,
            "valeur_ias": valeur_ias,
            "z_score": z_score,
            "r0_estime": r0,
            "nb_saisons_reference": len(hist_values),
            "statut": statut_final,
            "statut_ias": statut_ias,
            "statut_zscore": statut_z,
            "seuil_min": seuil_min,
            "seuil_max": seuil_max,
        })

        logger.info(
            "%s %s : IAS=%.1f z=%.2f R0=%s -> %s",
            semaine, syndrome, valeur_ias,
            z_score if z_score else 0,
            r0, statut_final,
        )

    # sauvegarde des indicateurs en JSON
    indic_dir = "/data/ars/indicateurs"
    os.makedirs(indic_dir, exist_ok=True)
    indic_path = f"{indic_dir}/indicateurs_{semaine}.json"
    with open(indic_path, "w", encoding="utf-8") as f:
        json.dump(indicateurs_resultats, f, ensure_ascii=False, indent=2)

    logger.info("%d indicateurs calcules pour %s", len(indicateurs_resultats), semaine)
    return indic_path


# --- Insertion PostgreSQL (UPSERT) ---

def inserer_donnees_postgres(**context):
    """Insere donnees hebdomadaires + indicateurs dans PostgreSQL."""
    semaine = _get_semaine(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]

    with open(f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json", "r") as f:
        donnees_brutes = json.load(f)
    with open(f"/data/ars/indicateurs/indicateurs_{semaine}.json", "r") as f:
        indicateurs = json.load(f)

    hook = PostgresHook(postgres_conn_id=POSTGRES_ARS_CONN_ID)

    sql_donnees = """
        INSERT INTO donnees_hebdomadaires
            (semaine, syndrome, valeur_ias, seuil_min_saison, seuil_max_saison, nb_jours_donnees)
        VALUES (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(seuil_min)s, %(seuil_max)s, %(nb_jours)s)
        ON CONFLICT (semaine, syndrome) DO UPDATE SET
            valeur_ias       = EXCLUDED.valeur_ias,
            seuil_min_saison = EXCLUDED.seuil_min_saison,
            seuil_max_saison = EXCLUDED.seuil_max_saison,
            nb_jours_donnees = EXCLUDED.nb_jours_donnees,
            updated_at       = CURRENT_TIMESTAMP;
    """

    sql_indic = """
        INSERT INTO indicateurs_epidemiques
            (semaine, syndrome, valeur_ias, z_score, r0_estime,
             nb_saisons_reference, statut, statut_ias, statut_zscore)
        VALUES (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(z_score)s, %(r0_estime)s,
                %(nb_saisons_reference)s, %(statut)s, %(statut_ias)s, %(statut_zscore)s)
        ON CONFLICT (semaine, syndrome) DO UPDATE SET
            valeur_ias           = EXCLUDED.valeur_ias,
            z_score              = EXCLUDED.z_score,
            r0_estime            = EXCLUDED.r0_estime,
            nb_saisons_reference = EXCLUDED.nb_saisons_reference,
            statut               = EXCLUDED.statut,
            statut_ias           = EXCLUDED.statut_ias,
            statut_zscore        = EXCLUDED.statut_zscore,
            updated_at           = CURRENT_TIMESTAMP;
    """

    nb_inserted = 0
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for syndrome, data in donnees_brutes.get("syndromes", {}).items():
                if data.get("valeur_ias") is not None:
                    cur.execute(sql_donnees, {
                        "semaine": semaine,
                        "syndrome": syndrome,
                        "valeur_ias": data["valeur_ias"],
                        "seuil_min": data.get("seuil_min"),
                        "seuil_max": data.get("seuil_max"),
                        "nb_jours": data.get("nb_jours", 0),
                    })
                    nb_inserted += 1

            for indic in indicateurs:
                cur.execute(sql_indic, indic)
                nb_inserted += 1

            conn.commit()

    logger.info("%d enregistrements inseres/mis a jour pour %s", nb_inserted, semaine)


# --- Evaluation de la situation epidemique (branching) ---

def evaluer_situation_epidemique(**context):
    """Determine le chemin d'execution selon la situation la plus severe."""
    semaine = _get_semaine(context)
    hook = PostgresHook(postgres_conn_id=POSTGRES_ARS_CONN_ID)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT statut, COUNT(*) AS nb_syndromes
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                GROUP BY statut
            """, (semaine,))
            resultats = {row[0]: row[1] for row in cur.fetchall()}

    nb_urgence = resultats.get("URGENCE", 0)
    nb_alerte = resultats.get("ALERTE", 0)

    context["task_instance"].xcom_push(key="nb_urgence", value=nb_urgence)
    context["task_instance"].xcom_push(key="nb_alerte", value=nb_alerte)
    context["task_instance"].xcom_push(key="situation_globale",
        value="URGENCE" if nb_urgence > 0 else "ALERTE" if nb_alerte > 0 else "NORMAL")

    logger.info("Semaine %s: %d URGENCE, %d ALERTE", semaine, nb_urgence, nb_alerte)

    if nb_urgence > 0:
        return "declencher_alerte_ars"
    elif nb_alerte > 0:
        return "envoyer_bulletin_surveillance"
    else:
        return "confirmer_situation_normale"


def declencher_alerte_ars(**context):
    logger.critical("ALERTE ARS DECLENCHEE -- Situation URGENCE en Occitanie")


def envoyer_bulletin_surveillance(**context):
    logger.warning("Bulletin de surveillance envoye -- Situation ALERTE en Occitanie")


def confirmer_situation_normale(**context):
    logger.info("Situation epidemiologique normale en Occitanie")


# --- Generation du rapport hebdomadaire ---

def generer_rapport_hebdomadaire(**context):
    """Genere un rapport JSON et l'enregistre dans PostgreSQL."""
    semaine = _get_semaine(context)
    hook = PostgresHook(postgres_conn_id=POSTGRES_ARS_CONN_ID)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ie.syndrome, ie.valeur_ias, ie.z_score,
                       ie.r0_estime, ie.statut, ie.statut_ias, ie.statut_zscore
                FROM indicateurs_epidemiques ie
                WHERE ie.semaine = %s
                ORDER BY ie.statut DESC
            """, (semaine,))
            indicateurs = cur.fetchall()

    statuts = [row[4] for row in indicateurs]
    if "URGENCE" in statuts:
        situation_globale = "URGENCE"
    elif "ALERTE" in statuts:
        situation_globale = "ALERTE"
    else:
        situation_globale = "NORMAL"

    recommandations = {
        "URGENCE": [
            "Activation du plan de reponse epidemique regional",
            "Notification immediate a Sante Publique France",
        ],
        "ALERTE": [
            "Surveillance renforcee des indicateurs pour les 48h suivantes",
            "Envoi d'un bulletin de surveillance aux partenaires de sante",
        ],
        "NORMAL": [
            "Maintien de la surveillance standard",
            "Prochain point epidemiologique dans 7 jours",
        ],
    }

    rapport = {
        "semaine": semaine,
        "region": "Occitanie",
        "code_region": "76",
        "date_generation": datetime.utcnow().isoformat(),
        "situation_globale": situation_globale,
        "nb_departements_surveilles": 13,
        "indicateurs": [
            {
                "syndrome": row[0],
                "valeur_ias": row[1],
                "z_score": row[2],
                "r0_estime": row[3],
                "statut": row[4],
            }
            for row in indicateurs
        ],
        "recommandations": recommandations.get(situation_globale, []),
        "genere_par": "ars_epidemio_dag v1.0",
        "pipeline_version": "2.8",
    }

    # sauvegarde JSON sur le volume
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]
    local_path = f"/data/ars/rapports/{annee}/{num_sem}/rapport_{semaine}.json"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    # insertion dans la table rapports_ars
    hook2 = PostgresHook(postgres_conn_id=POSTGRES_ARS_CONN_ID)
    with hook2.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rapports_ars
                    (semaine, situation_globale, nb_depts_alerte, nb_depts_urgence,
                     rapport_json, chemin_local)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (semaine) DO UPDATE SET
                    situation_globale = EXCLUDED.situation_globale,
                    nb_depts_alerte   = EXCLUDED.nb_depts_alerte,
                    nb_depts_urgence  = EXCLUDED.nb_depts_urgence,
                    rapport_json      = EXCLUDED.rapport_json,
                    chemin_local      = EXCLUDED.chemin_local,
                    updated_at        = CURRENT_TIMESTAMP
            """, (
                semaine, situation_globale,
                0, 0,
                json.dumps(rapport, ensure_ascii=False),
                local_path,
            ))
            conn.commit()

    logger.info("Rapport %s genere -- Statut : %s -- %s", semaine, situation_globale, local_path)


# --- Definition du DAG ---

with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline surveillance epidemiologique ARS Occitanie",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose"],
) as dag:

    init_base = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id=POSTGRES_ARS_CONN_ID,
        sql="sql/init_ars_epidemio.sql",
        autocommit=True,
    )

    with TaskGroup("collecte") as tg_collecte:
        collecter_sursaud = PythonOperator(
            task_id="collecter_donnees_sursaud",
            python_callable=collecter_donnees_ias,
            provide_context=True,
        )

    with TaskGroup("persistance_brute") as tg_archive:
        archiver = PythonOperator(
            task_id="archiver_local",
            python_callable=archiver_local,
            provide_context=True,
        )
        verifier = PythonOperator(
            task_id="verifier_archive",
            python_callable=verifier_archive,
            provide_context=True,
        )
        archiver >> verifier

    with TaskGroup("traitement") as tg_traitement:
        calculer_indic = PythonOperator(
            task_id="calculer_indicateurs_epidemiques",
            python_callable=calculer_indicateurs_epidemiques,
            provide_context=True,
        )

    with TaskGroup("persistance_operationnelle") as tg_persist:
        inserer_pg = PythonOperator(
            task_id="inserer_donnees_postgres",
            python_callable=inserer_donnees_postgres,
            provide_context=True,
        )

    evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique,
        provide_context=True,
    )

    alerte_ars = PythonOperator(
        task_id="declencher_alerte_ars",
        python_callable=declencher_alerte_ars,
        provide_context=True,
    )
    bulletin = PythonOperator(
        task_id="envoyer_bulletin_surveillance",
        python_callable=envoyer_bulletin_surveillance,
        provide_context=True,
    )
    normale = PythonOperator(
        task_id="confirmer_situation_normale",
        python_callable=confirmer_situation_normale,
        provide_context=True,
    )

    generer_rapport = PythonOperator(
        task_id="generer_rapport_hebdomadaire",
        python_callable=generer_rapport_hebdomadaire,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        provide_context=True,
    )

    # enchainement
    init_base >> tg_collecte >> tg_archive >> tg_traitement >> tg_persist
    tg_persist >> evaluer >> [alerte_ars, bulletin, normale] >> generer_rapport
