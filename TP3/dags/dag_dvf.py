"""
DAG : Pipeline DVF -- Data Lake (HDFS) vers Data Warehouse (PostgreSQL)
Source : Demandes de Valeurs Foncieres -- data.gouv.fr
"""
from __future__ import annotations

import io
import logging
import os
import tempfile
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DVF_URL = "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres/20260405-002251/valeursfoncieres-2023.txt.zip"
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : telechargement -> HDFS raw -> PostgreSQL curated",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():

    # --- Verification des sources ---
    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        """
        Verifie la disponibilite de l'API data.gouv.fr et du cluster HDFS.
        """
        statuts = {}

        # 1. Verifier l'API data.gouv.fr
        try:
            resp = requests.get(DVF_URL, stream=True, timeout=15, allow_redirects=True)
            statuts["dvf_api"] = resp.status_code == 200
            resp.close()
        except requests.RequestException as e:
            logger.error("Erreur acces DVF : %s", e)
            statuts["dvf_api"] = False

        # 2. Verifier HDFS via WebHDFS
        try:
            resp = requests.get(
                f"{WEBHDFS_BASE_URL}/?op=LISTSTATUS&user.name={WEBHDFS_USER}",
                timeout=10,
            )
            statuts["hdfs"] = resp.status_code == 200
        except requests.RequestException:
            statuts["hdfs"] = False

        # 3. Logger les statuts
        logger.info("Statut API DVF : %s", statuts["dvf_api"])
        logger.info("Statut HDFS    : %s", statuts["hdfs"])

        # 4. Lever une exception si une source critique est inaccessible
        if not statuts["dvf_api"]:
            raise AirflowException("L'API data.gouv.fr est inaccessible !")
        if not statuts["hdfs"]:
            raise AirflowException("Le cluster HDFS est inaccessible !")

        statuts["timestamp"] = datetime.now().isoformat()
        return statuts

    # --- Telechargement du CSV DVF ---
    @task(task_id="telecharger_dvf")
    def telecharger_dvf(statuts: dict) -> str:
        """
        Telecharge le fichier CSV DVF depuis data.gouv.fr en streaming.
        Retourne le chemin local du fichier telecharge.
        """
        # 1. Construire les noms de fichiers (avec suffixe unique pour eviter conflits)
        annee = 2023
        suffix = datetime.now().strftime("%H%M%S")
        zip_path = os.path.join(tempfile.gettempdir(), f"dvf_{annee}_{suffix}.zip")
        local_path = os.path.join(tempfile.gettempdir(), f"dvf_{annee}.csv")

        # 2. Telecharger en streaming (le fichier zip peut faire > 100 Mo)
        logger.info("Telechargement DVF depuis %s ...", DVF_URL)
        resp = requests.get(DVF_URL, stream=True, timeout=600)
        resp.raise_for_status()

        taille_totale = 0
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                taille_totale += len(chunk)
                if taille_totale % (50 * 1024 * 1024) < 8192:
                    logger.info("Telechargement en cours : %.1f Mo", taille_totale / (1024 * 1024))

        logger.info("ZIP telecharge : %.2f Mo", os.path.getsize(zip_path) / (1024 * 1024))

        # 3. Extraire le CSV du ZIP
        with zipfile.ZipFile(zip_path, "r") as zf:
            fichiers = zf.namelist()
            logger.info("Contenu du ZIP : %s", fichiers)
            # Prendre le premier fichier txt du zip
            txt_file = [f for f in fichiers if f.endswith(".txt")][0]
            with zf.open(txt_file) as src, open(local_path, "wb") as dst:
                dst.write(src.read())

        # Supprimer le zip
        os.remove(zip_path)

        # 4. Verifier que le fichier n'est pas vide
        taille = os.path.getsize(local_path)
        if taille < 1000:
            raise AirflowException(f"Fichier telecharge trop petit : {taille} octets")

        logger.info("CSV extrait : %s (%.2f Mo)", local_path, taille / (1024 * 1024))
        return local_path

    # --- Upload vers HDFS ---
    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        """
        Uploade le fichier CSV local vers HDFS via l'API WebHDFS.
        Upload en 2 etapes : PUT NameNode (307) -> PUT DataNode (201).
        """
        annee = 2023  # Annee du dataset DVF telecharge
        hdfs_filename = f"dvf_{annee}.csv"
        hdfs_file_path = f"{HDFS_RAW_PATH}/{hdfs_filename}"

        # 1. Creer le repertoire HDFS si necessaire
        mkdirs_url = f"{WEBHDFS_BASE_URL}{HDFS_RAW_PATH}/?op=MKDIRS&user.name={WEBHDFS_USER}"
        resp = requests.put(mkdirs_url)
        resp.raise_for_status()
        logger.info("Repertoire HDFS cree/existe : %s", HDFS_RAW_PATH)

        # 2. Initier l'upload (etape 1 : PUT NameNode -> 307 Redirect)
        create_url = (
            f"{WEBHDFS_BASE_URL}{hdfs_file_path}"
            f"?op=CREATE&user.name={WEBHDFS_USER}&overwrite=true"
        )
        resp = requests.put(create_url, allow_redirects=False)
        if resp.status_code != 307:
            raise AirflowException(
                f"Etape 1 upload : attendu 307, recu {resp.status_code}"
            )
        redirect_url = resp.headers["Location"]
        logger.info("Redirection DataNode recue")

        # 3. Envoyer le fichier vers le DataNode (etape 2)
        with open(local_path, "rb") as f:
            resp2 = requests.put(
                redirect_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
            )
        if resp2.status_code != 201:
            raise AirflowException(
                f"Etape 2 upload : attendu 201, recu {resp2.status_code}"
            )
        logger.info("Fichier uploade dans HDFS : %s", hdfs_file_path)

        # 4. Nettoyer le fichier temporaire local
        os.remove(local_path)
        logger.info("Fichier local supprime : %s", local_path)

        return hdfs_file_path

    # --- Traitement des donnees ---
    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_path: str) -> dict:
        """
        Lit le CSV depuis HDFS, filtre les appartements parisiens,
        calcule le prix au m2 et agrege par arrondissement.
        """
        # 1. Lire le CSV depuis HDFS
        open_url = (
            f"{WEBHDFS_BASE_URL}{hdfs_path}"
            f"?op=OPEN&user.name={WEBHDFS_USER}"
        )
        resp = requests.get(open_url, allow_redirects=True)
        resp.raise_for_status()

        # Lire en DataFrame pandas
        df = pd.read_csv(
            io.BytesIO(resp.content),
            sep="|",
            decimal=",",
            low_memory=False,
        )
        logger.info("CSV lu depuis HDFS : %d lignes, %d colonnes", len(df), len(df.columns))

        # 2. Renommer les colonnes (minuscules, espaces -> underscores)
        df.columns = [
            col.strip().lower().replace(" ", "_").replace("'", "")
            for col in df.columns
        ]
        logger.info("Colonnes disponibles : %s", list(df.columns))

        # 3. Convertir les types
        # code_postal est lu comme float (75008.0) -> on le convertit proprement en str
        df["code_postal"] = (
            df["code_postal"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )
        df["valeur_fonciere"] = pd.to_numeric(df["valeur_fonciere"], errors="coerce")
        df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")

        nb_avant = len(df)
        logger.info("Exemples code_postal : %s", df["code_postal"].dropna().unique()[:10].tolist())
        logger.info("Exemples nature_mutation : %s", df["nature_mutation"].dropna().unique()[:5].tolist())
        logger.info("Exemples type_local : %s", df["type_local"].dropna().unique()[:5].tolist())

        # 4. Filtrer les donnees
        # - Nature mutation = Vente
        df = df[df["nature_mutation"] == "Vente"]
        logger.info("Apres filtre Vente : %d lignes", len(df))
        # - Type local = Appartement
        df = df[df["type_local"] == "Appartement"]
        logger.info("Apres filtre Appartement : %d lignes", len(df))
        # - Paris uniquement (codes postaux 75001 a 75020 + 75116)
        paris_codes = [f"750{i:02d}" for i in range(1, 21)] + ["75116"]
        df = df[df["code_postal"].isin(paris_codes)]
        logger.info("Apres filtre Paris : %d lignes", len(df))
        # - Surface entre 9 et 500 m2
        df = df[(df["surface_reelle_bati"] >= 9) & (df["surface_reelle_bati"] <= 500)]
        # - Prix > 10 000 EUR
        df = df[df["valeur_fonciere"] > 10000]

        logger.info("Filtrage : %d -> %d lignes", nb_avant, len(df))

        if len(df) == 0:
            raise AirflowException("Aucune donnee apres filtrage ! Verifier les filtres.")

        # 5. Calculer le prix au m2
        df = df[df["surface_reelle_bati"] > 0].copy()
        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]

        # Extraire annee et mois de la date de mutation
        df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
        df["annee_mutation"] = df["date_mutation"].dt.year
        df["mois_mutation"] = df["date_mutation"].dt.month

        # 6. Extraire l'arrondissement depuis le code postal
        # 75001 -> 1, 75016 -> 16, 75116 -> 16
        def extraire_arrondissement(cp):
            cp = str(cp)
            if cp == "75116":
                return 16
            return int(cp[3:])

        df["arrondissement"] = df["code_postal"].apply(extraire_arrondissement)

        # 7. Agreger par arrondissement
        agg = df.groupby("code_postal").agg(
            arrondissement=("arrondissement", "first"),
            prix_m2_moyen=("prix_m2", "mean"),
            prix_m2_median=("prix_m2", "median"),
            prix_m2_min=("prix_m2", "min"),
            prix_m2_max=("prix_m2", "max"),
            nb_transactions=("prix_m2", "count"),
            surface_moyenne=("surface_reelle_bati", "mean"),
        ).reset_index()

        # Prendre la derniere annee/mois present dans les donnees
        annee = int(df["annee_mutation"].mode().iloc[0])
        mois = int(df["mois_mutation"].mode().iloc[0])
        agg["annee"] = annee
        agg["mois"] = mois

        agregats = agg.to_dict(orient="records")
        logger.info("Agregation : %d arrondissements", len(agregats))

        # 8. Statistiques globales Paris
        stats_globales = {
            "annee": annee,
            "mois": mois,
            "nb_transactions_total": int(len(df)),
            "prix_m2_median_paris": float(df["prix_m2"].median()),
            "prix_m2_moyen_paris": float(df["prix_m2"].mean()),
            "arrdt_plus_cher": int(
                agg.loc[agg["prix_m2_median"].idxmax(), "arrondissement"]
            ),
            "arrdt_moins_cher": int(
                agg.loc[agg["prix_m2_median"].idxmin(), "arrondissement"]
            ),
            "surface_mediane": float(df["surface_reelle_bati"].median()),
        }
        logger.info("Stats globales Paris : %s", stats_globales)

        return {"agregats": agregats, "stats_globales": stats_globales}

    # --- Insertion PostgreSQL ---
    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> int:
        """
        Insere les donnees agregees dans PostgreSQL avec UPSERT.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        agregats = resultats.get("agregats", [])
        stats_globales = resultats.get("stats_globales", {})

        # Requete UPSERT pour prix_m2_arrondissement
        upsert_query = """
            INSERT INTO prix_m2_arrondissement
                (code_postal, arrondissement, annee, mois,
                 prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max,
                 nb_transactions, surface_moyenne, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (code_postal, annee, mois) DO UPDATE SET
                prix_m2_moyen    = EXCLUDED.prix_m2_moyen,
                prix_m2_median   = EXCLUDED.prix_m2_median,
                prix_m2_min      = EXCLUDED.prix_m2_min,
                prix_m2_max      = EXCLUDED.prix_m2_max,
                nb_transactions  = EXCLUDED.nb_transactions,
                surface_moyenne  = EXCLUDED.surface_moyenne,
                updated_at       = NOW();
        """

        # Inserer chaque arrondissement
        nb_inseres = 0
        for row in agregats:
            params = (
                str(row["code_postal"]),
                int(row["arrondissement"]),
                int(row["annee"]),
                int(row["mois"]),
                float(row["prix_m2_moyen"]),
                float(row["prix_m2_median"]),
                float(row["prix_m2_min"]),
                float(row["prix_m2_max"]),
                int(row["nb_transactions"]),
                float(row["surface_moyenne"]),
            )
            hook.run(upsert_query, parameters=params)
            nb_inseres += 1

        logger.info("%d arrondissements inseres/mis a jour", nb_inseres)

        # Inserer les statistiques globales dans stats_marche
        if stats_globales:
            stats_query = """
                INSERT INTO stats_marche
                    (annee, mois, nb_transactions_total,
                     prix_m2_median_paris, prix_m2_moyen_paris,
                     arrdt_plus_cher, arrdt_moins_cher,
                     surface_mediane, date_calcul)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (annee, mois) DO UPDATE SET
                    nb_transactions_total  = EXCLUDED.nb_transactions_total,
                    prix_m2_median_paris   = EXCLUDED.prix_m2_median_paris,
                    prix_m2_moyen_paris    = EXCLUDED.prix_m2_moyen_paris,
                    arrdt_plus_cher        = EXCLUDED.arrdt_plus_cher,
                    arrdt_moins_cher       = EXCLUDED.arrdt_moins_cher,
                    surface_mediane        = EXCLUDED.surface_mediane,
                    date_calcul            = NOW();
            """
            stats_params = (
                int(stats_globales["annee"]),
                int(stats_globales["mois"]),
                int(stats_globales["nb_transactions_total"]),
                float(stats_globales["prix_m2_median_paris"]),
                float(stats_globales["prix_m2_moyen_paris"]),
                int(stats_globales["arrdt_plus_cher"]),
                int(stats_globales["arrdt_moins_cher"]),
                float(stats_globales["surface_mediane"]),
            )
            hook.run(stats_query, parameters=stats_params)
            logger.info("Stats globales inserees dans stats_marche")

        return nb_inseres

    # --- Rapport final ---
    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        """
        Genere un rapport avec le classement des arrondissements
        par prix median au m2 decroissant.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Requete de ranking
        now = datetime.now()
        query = """
            SELECT
                arrondissement,
                prix_m2_median,
                prix_m2_moyen,
                nb_transactions,
                surface_moyenne
            FROM prix_m2_arrondissement
            WHERE annee = %s AND mois = %s
            ORDER BY prix_m2_median DESC
            LIMIT 20;
        """

        # On cherche les donnees du mois le plus recent disponible
        # D'abord on essaie annee/mois courant, sinon on prend le dernier disponible
        records = hook.get_records(query, parameters=(now.year, now.month))

        if not records:
            # Prendre le dernier mois disponible
            fallback = hook.get_first(
                "SELECT annee, mois FROM prix_m2_arrondissement ORDER BY annee DESC, mois DESC LIMIT 1"
            )
            if fallback:
                records = hook.get_records(query, parameters=(fallback[0], fallback[1]))

        # Formater le rapport en tableau
        header = (
            f"\n{'Arrondissement':>15} | {'Median (EUR/m2)':>16} | "
            f"{'Moyen (EUR/m2)':>15} | {'Transactions':>13} | {'Surface moy':>12}"
        )
        separator = "-" * len(header)
        rapport = f"\n=== CLASSEMENT DES ARRONDISSEMENTS PARISIENS ===\n"
        rapport += header + "\n" + separator + "\n"

        for row in records:
            arrdt, median, moyen, nb_tx, surface = row
            rapport += (
                f"{arrdt:>14}e | {float(median):>16,.0f} | "
                f"{float(moyen):>15,.0f} | {int(nb_tx):>13} | {float(surface):>11.1f}\n"
            )

        rapport += separator + "\n"
        rapport += f"Total : {nb_inseres} arrondissements mis a jour\n"

        logger.info(rapport)
        return rapport

    # enchainement
    t_verif = verifier_sources()
    t_download = telecharger_dvf(t_verif)
    t_hdfs = stocker_hdfs_raw(t_download)
    t_traiter = traiter_donnees(t_hdfs)
    t_pg = inserer_postgresql(t_traiter)
    t_rapport = generer_rapport(t_pg)

    chain(t_verif, t_download, t_hdfs, t_traiter, t_pg, t_rapport)


pipeline_dvf()
