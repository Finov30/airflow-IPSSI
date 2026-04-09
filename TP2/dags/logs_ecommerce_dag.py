# DAG Logs E-Commerce — TP Jour 2
# Pipeline qui genere des logs, les met dans HDFS, les analyse,
# et decide si on alerte l'equipe Ops ou pas selon le taux d'erreur

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pendulum
import subprocess
import logging
import os
import requests
import re

local_tz = pendulum.timezone("Europe/Paris")

# Adresse du namenode pour les appels WebHDFS (API REST de HDFS)
WEBHDFS_URL = "http://namenode:9870/webhdfs/v1"
HDFS_USER = "root"

# Si plus de 5% de requetes en erreur -> on alerte
SEUIL_ERREUR_PCT = 5.0

# default_args = parametres communs a toutes les taches (cf cours Jour 2 - slide 3)
default_args = {
    "owner": "ecommerce-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# =============================================
# Etape 3 — Generer les logs journaliers
# =============================================
# On utilise context["ds"] pour avoir la date logique du DAG (pas date.today())
# Ca rend la tache idempotente : si on la relance, elle genere les memes logs
# Le return stocke le chemin dans XCom automatiquement 
def generer_logs_journaliers(**context):
    execution_date = context["ds"]  # format YYYY-MM-DD
    fichier_sortie = f"/tmp/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"

    # subprocess.run lance le script comme si on tapait la commande dans le terminal
    result = subprocess.run(
        ["python3", script_path, execution_date, "1000", fichier_sortie],
        check=True, capture_output=True, text=True,
    )
    logging.info(result.stdout)

    taille = os.path.getsize(fichier_sortie)
    logging.info(f"Fichier genere : {fichier_sortie} ({taille} octets)")

    # le return pousse automatiquement la valeur dans XCom (cle "return_value")
    return fichier_sortie


# =============================================
# Etape 4 — Uploader le fichier vers HDFS
# =============================================
# On utilise WebHDFS (API REST) car le conteneur Airflow n'a pas le client hdfs
# L'upload se fait en 2 temps :
#   1) On demande au namenode de creer le fichier -> il repond avec l'adresse du datanode
#   2) On envoie les donnees directement au datanode
def uploader_vers_hdfs(**context):
    execution_date = context["ds"]
    fichier_local = f"/tmp/access_{execution_date}.log"
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"

    logging.info(f"Upload de {fichier_local} vers HDFS:{chemin_hdfs}")

    # 1) Demande au namenode (il redirige vers un datanode avec un code 307)
    resp = requests.put(
        f"{WEBHDFS_URL}{chemin_hdfs}",
        params={"op": "CREATE", "user.name": HDFS_USER, "overwrite": "true"},
        allow_redirects=False,
        timeout=30,
    )

    # 2) On suit la redirection et on envoie le fichier au datanode
    if resp.status_code == 307:
        datanode_url = resp.headers["Location"]
        with open(fichier_local, "rb") as f:
            resp2 = requests.put(datanode_url, data=f, timeout=60)
            resp2.raise_for_status()
        logging.info(f"[OK] Upload termine : {chemin_hdfs}")
    else:
        raise ValueError(f"Erreur WebHDFS : status {resp.status_code}")


# =============================================
# Etape 5 — Sensor : verifier que le fichier est dans HDFS
# =============================================
# Check si le fichier est dans HDFS avec WebHDFS GETFILESTATUS
# Si le fichier n'existe pas -> on leve une erreur et Airflow retry
def verifier_fichier_hdfs(**context):
    execution_date = context["ds"]
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"

    resp = requests.get(
        f"{WEBHDFS_URL}{chemin_hdfs}",
        params={"op": "GETFILESTATUS", "user.name": HDFS_USER},
        timeout=10,
    )

    if resp.status_code == 200:
        taille = resp.json()["FileStatus"]["length"]
        logging.info(f"[OK] Fichier dans HDFS : {chemin_hdfs} ({taille} bytes)")
    else:
        raise ValueError(f"Fichier pas encore dans HDFS : {chemin_hdfs}")


# =============================================
# Etape 6 — Analyser les logs depuis HDFS
# =============================================
# On lit le fichier avec WebHDFS (op=OPEN) et on analyse :
# - les status codes (200, 404, 500...)
# - le top 5 des URLs visitees
# - le taux d'erreur (nombre de 4xx+5xx / total)
# On sauve le taux dans un fichier pour que le BranchPythonOperator le lise apres
def analyser_logs_hdfs(**context):
    execution_date = context["ds"]
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"

    # Lire le fichier depuis HDFS via l'API REST
    resp = requests.get(
        f"{WEBHDFS_URL}{chemin_hdfs}",
        params={"op": "OPEN", "user.name": HDFS_USER},
        timeout=30,
    )
    resp.raise_for_status()
    lignes = resp.text.strip().split("\n")

    logging.info(f"Total lignes : {len(lignes)}")

    # On parcourt chaque ligne de log et on extrait le status code et l'URL
    status_counts = {}
    url_counts = {}
    erreurs = 0

    for ligne in lignes:
        # regex pour extraire l'URL et le status code du log Apache
        match = re.search(r'"[A-Z]+ ([^ ]+) HTTP/[0-9.]+" (\d+)', ligne)
        if match:
            url = match.group(1)
            status = int(match.group(2))

            status_counts[status] = status_counts.get(status, 0) + 1
            url_counts[url] = url_counts.get(url, 0) + 1

            if status >= 400:  # 4xx et 5xx = erreurs
                erreurs += 1

    # Affichage dans les logs Airflow
    logging.info("=== STATUS CODES ===")
    for code, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        logging.info(f"  {code}: {count}")

    logging.info("=== TOP 5 URLS ===")
    for url, count in sorted(url_counts.items(), key=lambda x: -x[1])[:5]:
        logging.info(f"  {count:>4} {url}")

    total = len(lignes)
    taux = (erreurs / total) * 100 if total > 0 else 0
    logging.info(f"=== TAUX ERREUR : {erreurs}/{total} = {taux:.2f}% ===")

    # On sauve dans un fichier car le BranchPythonOperator en a besoin
    with open(f"/tmp/taux_erreur_{execution_date}.txt", "w") as f:
        f.write(f"{erreurs} {total}")

    return {"erreurs": erreurs, "total": total, "taux_pct": round(taux, 2)}


# =============================================
# Etape 7 — BranchPythonOperator 
# =============================================
# Le BranchPythonOperator retourne le task_id de la branche a suivre
# Les autres branches sont mises en "skipped"
# Ici : si taux > 5% -> alerte, sinon -> tout est ok
def brancher_selon_taux_erreur(**context):
    execution_date = context["ds"]

    # On lit le taux calcule par la tache precedente
    with open(f"/tmp/taux_erreur_{execution_date}.txt", "r") as f:
        erreurs, total = f.read().strip().split()
    erreurs, total = int(erreurs), int(total)
    taux_pct = (erreurs / total) * 100 if total > 0 else 0

    logging.info(f"Taux d'erreur : {erreurs}/{total} = {taux_pct:.2f}%")

    # On retourne le task_id de la branche a executer
    if taux_pct > SEUIL_ERREUR_PCT:
        logging.warning(f"Taux {taux_pct:.2f}% > seuil {SEUIL_ERREUR_PCT}% -> ALERTE")
        return "alerter_equipe_ops"
    else:
        logging.info(f"Taux {taux_pct:.2f}% <= seuil -> OK")
        return "archiver_rapport_ok"


# Branche 1 : on alerte l'equipe Ops (en vrai ce serait un message Slack ou PagerDuty)
def alerter_equipe_ops(**context):
    logging.warning(
        f"[ALERTE] Taux d'erreur anormal pour les logs du {context['ds']}. "
        "Verifiez les serveurs web."
    )


# Branche 2 : tout va bien, on le note dans les logs
def archiver_rapport_ok(**context):
    logging.info(f"[OK] Taux d'erreur normal pour les logs du {context['ds']}.")


# =============================================
# Etape 8 — Archiver les logs dans HDFS (raw -> processed)
# =============================================
# On deplace le fichier de la zone "raw" vers "processed"
# avec WebHDFS op=RENAME (equivalent de hdfs dfs -mv)
# trigger_rule="none_failed_min_one_success" = cette tache s'execute
# quelle que soit la branche choisie (Trigger Rules)
def archiver_logs_hdfs(**context):
    execution_date = context["ds"]
    source = f"/data/ecommerce/logs/raw/access_{execution_date}.log"
    destination = f"/data/ecommerce/logs/processed/access_{execution_date}.log"

    logging.info(f"Deplacement : {source} -> {destination}")

    resp = requests.put(
        f"{WEBHDFS_URL}{source}",
        params={"op": "RENAME", "destination": destination, "user.name": HDFS_USER},
        timeout=10,
    )
    resp.raise_for_status()

    if resp.json().get("boolean"):
        logging.info("[OK] Fichier archive dans processed/")
    else:
        raise ValueError("Echec du deplacement HDFS")


# =============================================
# Definition du DAG 
# =============================================
# schedule = expression cron "0 2 * * *" = chaque nuit a 2h 
# catchup=False = on ne rattrape pas les jours passes
with DAG(
    dag_id="logs_ecommerce_dag",
    default_args=default_args,
    description="Pipeline ingestion et analyse de logs e-commerce dans HDFS",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["ecommerce", "logs", "hdfs"],
) as dag:

    # Etape 3
    t_generer = PythonOperator(
        task_id="generer_logs_journaliers",
        python_callable=generer_logs_journaliers,
    )

    # Etape 4
    t_upload = PythonOperator(
        task_id="uploader_vers_hdfs",
        python_callable=uploader_vers_hdfs,
    )

    # Etape 5
    t_sensor = PythonOperator(
        task_id="hdfs_file_sensor",
        python_callable=verifier_fichier_hdfs,
    )

    # Etape 6
    t_analyser = PythonOperator(
        task_id="analyser_logs_hdfs",
        python_callable=analyser_logs_hdfs,
    )

    # Etape 7 - BranchPythonOperator 
    t_branch = BranchPythonOperator(
        task_id="brancher_selon_taux_erreur",
        python_callable=brancher_selon_taux_erreur,
    )

    t_alerte = PythonOperator(
        task_id="alerter_equipe_ops",
        python_callable=alerter_equipe_ops,
    )

    t_archive_ok = PythonOperator(
        task_id="archiver_rapport_ok",
        python_callable=archiver_rapport_ok,
    )

    # Etape 8 - trigger_rule pour que ca s'execute apres n'importe quelle branche
    t_archiver = PythonOperator(
        task_id="archiver_logs_hdfs",
        python_callable=archiver_logs_hdfs,
        trigger_rule="none_failed_min_one_success",
    )

    # Dependances 
    # t1 -> t2 -> t3 -> t4 -> t5 -> [branche alerte OU branche ok] -> t8
    (
        t_generer
        >> t_upload
        >> t_sensor
        >> t_analyser
        >> t_branch
        >> [t_alerte, t_archive_ok]
        >> t_archiver
    )
