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

# -- Constantes ---------------------------------------------------------------
POSTGRES_ARS_CONN_ID = "postgres_ars"

# -- Configuration du DAG -----------------------------------------------------
default_args = {
    "owner": "ars-occitanie",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def _get_semaine(context: dict) -> str:
    """Extrait la semaine ISO depuis l'execution_date du context Airflow."""
    exec_date = context["execution_date"]
    year, week, _ = exec_date.isocalendar()
    return f"{year}-S{week:02d}"


# =============================================================================
# TACHE 1 : init_base_donnees (via PostgresOperator + fichier SQL)
# =============================================================================
# Defini directement dans le DAG ci-dessous


# =============================================================================
# TACHE 2 : collecter_donnees_sursaud
# =============================================================================
def collecter_donnees_ias(**context) -> str:
    """Telecharge les CSV IAS et retourne le chemin du fichier JSON cree."""
    semaine: str = _get_semaine(context)
    archive_path: str = Variable.get("archive_base_path", default_var="/data/ars")
    output_dir: str = f"{archive_path}/raw"

    sys.path.insert(0, "/opt/airflow/scripts")
    from collecte_ias import (
        DATASETS_IAS, telecharger_csv_ias, filtrer_semaine,
        agreger_semaine, sauvegarder_donnees,
    )

    resultats: dict = {}
    for syndrome, url in DATASETS_IAS.items():
        try:
            rows_all = telecharger_csv_ias(url)
            rows_sem = filtrer_semaine(rows_all, semaine)
            resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)
            logger.info("Collecte %s OK : %d jours", syndrome, resultats[syndrome]["nb_jours"])
        except Exception as e:
            logger.error("Erreur collecte %s : %s", syndrome, e)
            raise

    chemin: str = sauvegarder_donnees(resultats, semaine, output_dir)
    return chemin


# =============================================================================
# TACHE 3 : archiver_local + verifier_archive
# =============================================================================
def archiver_local(**context) -> str:
    """Copie le fichier brut dans la structure d'archivage partitionnee."""
    semaine: str = _get_semaine(context)
    annee: str = semaine.split("-")[0]
    num_sem: str = semaine.split("-")[1]

    chemin_source: str = context["task_instance"].xcom_pull(
        task_ids="collecte.collecter_donnees_sursaud"
    )
    if not chemin_source or not os.path.exists(chemin_source):
        raise FileNotFoundError(f"Fichier source introuvable : {chemin_source}")

    archive_dir: str = f"/data/ars/raw/{annee}/{num_sem}"
    os.makedirs(archive_dir, exist_ok=True)
    chemin_dest: str = f"{archive_dir}/sursaud_{semaine}.json"
    shutil.copy2(chemin_source, chemin_dest)
    logger.info("Archive creee : %s", chemin_dest)
    return chemin_dest


def verifier_archive(**context) -> bool:
    """Verifie que le fichier d'archive existe et n'est pas vide."""
    semaine: str = _get_semaine(context)
    annee: str = semaine.split("-")[0]
    num_sem: str = semaine.split("-")[1]
    chemin: str = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"

    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Archive manquante : {chemin}")
    taille: int = os.path.getsize(chemin)
    if taille == 0:
        raise ValueError(f"Archive vide : {chemin}")
    logger.info("Archive valide : %s (%d octets)", chemin, taille)
    return True


# =============================================================================
# TACHE 4 : calculer_indicateurs_epidemiques
# =============================================================================
def calculer_indicateurs_epidemiques(**context) -> str:
    """Calcule z-score, R0 et classification pour chaque syndrome."""
    semaine: str = _get_semaine(context)
    annee: str = semaine.split("-")[0]
    num_sem: str = semaine.split("-")[1]

    # Lire les donnees brutes
    chemin_brut: str = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    with open(chemin_brut, "r", encoding="utf-8") as f:
        donnees_brutes: dict = json.load(f)

    sys.path.insert(0, "/opt/airflow/scripts")
    from calcul_indicateurs import (
        calculer_zscore, classifier_statut_ias,
        classifier_statut_zscore, classifier_statut_final,
        calculer_r0_simplifie,
    )

    seuil_alerte_z: float = float(Variable.get("seuil_alerte_zscore", default_var="1.5"))
    seuil_urgence_z: float = float(Variable.get("seuil_urgence_zscore", default_var="3.0"))

    # Recuperer les 4 dernieres valeurs IAS depuis PostgreSQL pour le R0
    hook = PostgresHook(postgres_conn_id=POSTGRES_ARS_CONN_ID)

    indicateurs_resultats: list = []
    syndromes_data: dict = donnees_brutes.get("syndromes", {})

    for syndrome, data in syndromes_data.items():
        valeur_ias: Optional[float] = data.get("valeur_ias")
        if valeur_ias is None:
            logger.warning("Pas de valeur IAS pour %s semaine %s", syndrome, semaine)
            continue

        seuil_min: Optional[float] = data.get("seuil_min")
        seuil_max: Optional[float] = data.get("seuil_max")

        # Z-score depuis l'historique des saisons
        historique: dict = data.get("historique", {})
        hist_values: list = [v for v in historique.values() if v is not None]
        z_score: Optional[float] = calculer_zscore(valeur_ias, hist_values)

        # R0 : recuperer les 4 dernieres valeurs IAS depuis la base
        try:
            rows = hook.get_records(
                """SELECT valeur_ias FROM donnees_hebdomadaires
                   WHERE syndrome = %s ORDER BY semaine DESC LIMIT 4""",
                parameters=(syndrome,)
            )
            series_r0: list = [r[0] for r in reversed(rows)] + [valeur_ias]
        except Exception:
            series_r0 = [valeur_ias]

        duree_inf: int = 5 if syndrome == "GRIPPE" else 3
        r0: Optional[float] = calculer_r0_simplifie(series_r0, duree_inf)

        # Classification
        statut_ias: str = classifier_statut_ias(valeur_ias, seuil_min, seuil_max)
        statut_z: str = classifier_statut_zscore(z_score, seuil_alerte_z, seuil_urgence_z)
        statut_final: str = classifier_statut_final(statut_ias, statut_z)

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

    # Sauvegarder les indicateurs
    indic_dir: str = "/data/ars/indicateurs"
    os.makedirs(indic_dir, exist_ok=True)
    indic_path: str = f"{indic_dir}/indicateurs_{semaine}.json"
    with open(indic_path, "w", encoding="utf-8") as f:
        json.dump(indicateurs_resultats, f, ensure_ascii=False, indent=2)

    logger.info("%d indicateurs calcules pour %s", len(indicateurs_resultats), semaine)
    return indic_path


# =============================================================================
# TACHE 5 : inserer_donnees_postgres
# =============================================================================
def inserer_donnees_postgres(**context) -> None:
    """Insere donnees hebdomadaires + indicateurs dans PostgreSQL (UPSERT)."""
    semaine: str = _get_semaine(context)
    annee: str = semaine.split("-")[0]
    num_sem: str = semaine.split("-")[1]

    # Lire les fichiers JSON
    with open(f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json", "r") as f:
        donnees_brutes: dict = json.load(f)

    with open(f"/data/ars/indicateurs/indicateurs_{semaine}.json", "r") as f:
        indicateurs: list = json.load(f)

    hook = PostgresHook(postgres_conn_id=POSTGRES_ARS_CONN_ID)

    # UPSERT donnees_hebdomadaires
    sql_donnees: str = """
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

    # UPSERT indicateurs_epidemiques
    sql_indic: str = """
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

    nb_inserted: int = 0
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Inserer les donnees hebdomadaires par syndrome
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

            # Inserer les indicateurs
            for indic in indicateurs:
                cur.execute(sql_indic, indic)
                nb_inserted += 1

            conn.commit()

    logger.info("%d enregistrements inseres/mis a jour pour %s", nb_inserted, semaine)


# =============================================================================
# TACHE 6 : evaluer_situation_epidemique (BranchPythonOperator)
# =============================================================================
def evaluer_situation_epidemique(**context) -> str:
    """Determine le chemin d'execution selon la situation la plus severe."""
    semaine: str = _get_semaine(context)
    hook = PostgresHook(postgres_conn_id=POSTGRES_ARS_CONN_ID)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT statut, COUNT(*) AS nb_syndromes
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                GROUP BY statut
            """, (semaine,))
            resultats: dict = {row[0]: row[1] for row in cur.fetchall()}

    nb_urgence: int = resultats.get("URGENCE", 0)
    nb_alerte: int = resultats.get("ALERTE", 0)

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


def declencher_alerte_ars(**context) -> None:
    """Action en cas d'URGENCE epidemique."""
    logger.critical("ALERTE ARS DECLENCHEE -- Situation URGENCE en Occitanie")


def envoyer_bulletin_surveillance(**context) -> None:
    """Action en cas d'ALERTE epidemique."""
    logger.warning("Bulletin de surveillance envoye -- Situation ALERTE en Occitanie")


def confirmer_situation_normale(**context) -> None:
    """Action quand la situation est normale."""
    logger.info("Situation epidemiologique normale en Occitanie")


# =============================================================================
# TACHE 7 : generer_rapport_hebdomadaire
# =============================================================================
def generer_rapport_hebdomadaire(**context) -> None:
    """Genere un rapport JSON et l'enregistre dans PostgreSQL."""
    semaine: str = _get_semaine(context)
    hook = PostgresHook(postgres_conn_id=POSTGRES_ARS_CONN_ID)

    # Lire les indicateurs depuis PostgreSQL
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

    statuts: list = [row[4] for row in indicateurs]
    if "URGENCE" in statuts:
        situation_globale: str = "URGENCE"
    elif "ALERTE" in statuts:
        situation_globale = "ALERTE"
    else:
        situation_globale = "NORMAL"

    recommandations_map: dict = {
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

    rapport: dict = {
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
        "recommandations": recommandations_map.get(situation_globale, []),
        "genere_par": "ars_epidemio_dag v1.0",
        "pipeline_version": "2.8",
    }

    # Sauvegarder le rapport en JSON
    annee: str = semaine.split("-")[0]
    num_sem: str = semaine.split("-")[1]
    local_path: str = f"/data/ars/rapports/{annee}/{num_sem}/rapport_{semaine}.json"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    # Inserer dans rapports_ars
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


# =============================================================================
# DEFINITION DU DAG
# =============================================================================
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

    # Etape 3 : Init base PostgreSQL
    init_base = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id=POSTGRES_ARS_CONN_ID,
        sql="sql/init_ars_epidemio.sql",
        autocommit=True,
    )

    # Etape 4 : Collecte des donnees IAS
    with TaskGroup("collecte") as tg_collecte:
        collecter_sursaud = PythonOperator(
            task_id="collecter_donnees_sursaud",
            python_callable=collecter_donnees_ias,
            provide_context=True,
        )

    # Etape 5 : Archivage et verification
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

    # Etape 6 : Calcul des indicateurs
    with TaskGroup("traitement") as tg_traitement:
        calculer_indic = PythonOperator(
            task_id="calculer_indicateurs_epidemiques",
            python_callable=calculer_indicateurs_epidemiques,
            provide_context=True,
        )

    # Etape 7 : Insertion PostgreSQL
    with TaskGroup("persistance_operationnelle") as tg_persist:
        inserer_pg = PythonOperator(
            task_id="inserer_donnees_postgres",
            python_callable=inserer_donnees_postgres,
            provide_context=True,
        )

    # Etape 8 : Evaluation et branchement
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

    # Etape 9 : Rapport hebdomadaire
    generer_rapport = PythonOperator(
        task_id="generer_rapport_hebdomadaire",
        python_callable=generer_rapport_hebdomadaire,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        provide_context=True,
    )

    # Enchainement des taches
    init_base >> tg_collecte >> tg_archive >> tg_traitement >> tg_persist
    tg_persist >> evaluer >> [alerte_ars, bulletin, normale] >> generer_rapport
