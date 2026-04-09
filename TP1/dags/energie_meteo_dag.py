from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import requests
import logging
import json
from datetime import date

# On utilise la timezone de Paris car RTE travaille en heure francaise
local_tz = pendulum.timezone("Europe/Paris")

# Dictionnaire des 5 regions avec leurs coordonnees GPS
# On s'en sert pour appeler l'API meteo avec les bonnes positions
REGIONS = {
    "Île-de-France":        {"lat": 48.8566, "lon": 2.3522},
    "Occitanie":            {"lat": 43.6047, "lon": 1.4442},
    "Nouvelle-Aquitaine":   {"lat": 44.8378, "lon": -0.5792},
    "Auvergne-Rhône-Alpes": {"lat": 45.7640, "lon": 4.8357},
    "Hauts-de-France":      {"lat": 50.6292, "lon": 3.0573},
}

# Arguments par defaut appliques a toutes les taches du DAG
default_args = {
    "owner": "rte-data-team",
    "depends_on_past": False,       # chaque run est independant du precedent
    "email_on_failure": False,       # pas d'envoi de mail si une tache plante
    "retries": 2,                    # on retente 2 fois si ca echoue
    "retry_delay": timedelta(minutes=5),  # 5 min d'attente entre chaque retry
    "sla": timedelta(minutes=90),    # (Exo 1) SLA global : tout doit finir en 90 min max
}


# (Exo 1) Cette fonction est appelee automatiquement par Airflow quand un SLA est depasse
# Elle loggue quelles taches sont en retard pour alerter l'equipe
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    taches_en_retard = [t.task_id for t in blocking_task_list]
    logging.warning(f"[ALERTE SLA] Taches en retard : {taches_en_retard}")
    for sla in slas:
        logging.warning(f"[ALERTE SLA] Tache {sla.task_id} a depasse son SLA de {sla.timedelta}")


# ========== TACHE 1 : Verification des APIs ==========
# On teste que les 2 APIs repondent avant de lancer les collectes
# Si une API est down, on leve une erreur et le pipeline s'arrete
def verifier_apis(**context):
    apis = {
        "Open-Meteo": (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=48.8566&longitude=2.3522"
            "&daily=sunshine_duration&timezone=Europe/Paris&forecast_days=1"
        ),
        "éCO2mix": (
            "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
            "/eco2mix-regional-cons-def/records?limit=1&timezone=Europe%2FParis"
        ),
    }
    # On boucle sur chaque API et on verifie le status code
    for nom, url in apis.items():
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                raise ValueError(f"API {nom} a retourne le status {response.status_code}")
            logging.info(f"API {nom} : OK")
        except requests.exceptions.RequestException as e:
            raise ValueError(f"API {nom} indisponible : {e}")

    logging.info("Toutes les APIs sont dispo.")


# ========== TACHE 2 : Collecte meteo ==========
# Pour chaque region, on appelle Open-Meteo pour recuperer :
# - la duree d'ensoleillement (en heures)
# - la vitesse max du vent (en km/h)
# Le return stocke le resultat dans XCom pour les taches suivantes
def collecter_meteo_regions(**context):
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    resultats = {}

    for region, coords in REGIONS.items():
        # Parametres de la requete API
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "daily": "sunshine_duration,wind_speed_10m_max",
            "timezone": "Europe/Paris",
            "forecast_days": 1,  # on veut juste aujourd'hui
        }
        response = requests.get(BASE_URL, params=params, timeout=15)
        response.raise_for_status()  # leve une erreur si status != 200
        data = response.json()

        # L'API renvoie l'ensoleillement en secondes, on divise par 3600 pour avoir des heures
        ensoleillement_h = round(data["daily"]["sunshine_duration"][0] / 3600, 2)
        vent_kmh = data["daily"]["wind_speed_10m_max"][0]

        resultats[region] = {"ensoleillement_h": ensoleillement_h, "vent_kmh": vent_kmh}
        logging.info(f"{region} : soleil={ensoleillement_h}h, vent={vent_kmh} km/h")

    # Le return pousse automatiquement le dict dans XCom
    return resultats


# ========== TACHE 3 : Collecte production electrique ==========
# On recupere les donnees de production solaire et eolienne depuis l'API eCO2mix
# L'API retourne plusieurs enregistrements par region (un par heure)
# On fait la moyenne pour avoir une valeur journaliere en MW
def collecter_production_electrique(**context):
    BASE_URL = (
        "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
        "/eco2mix-regional-cons-def/records"
    )
    params = {"limit": 100, "timezone": "Europe/Paris"}

    response = requests.get(BASE_URL, params=params, timeout=15)
    response.raise_for_status()
    results = response.json()["results"]

    # On prepare un dict pour accumuler les valeurs horaires de chaque region
    accumulation = {region: {"solaire": [], "eolien": []} for region in REGIONS}

    for enregistrement in results:
        lib_region = enregistrement.get("libelle_region", "")
        # On ne garde que les regions qui nous interessent
        if lib_region in REGIONS:
            # L'API peut retourner None si pas de donnee, on met 0 dans ce cas
            solaire_raw = enregistrement.get("solaire")
            eolien_raw = enregistrement.get("eolien")
            solaire = float(solaire_raw) if solaire_raw is not None else 0.0
            eolien = float(eolien_raw) if eolien_raw is not None else 0.0
            accumulation[lib_region]["solaire"].append(solaire)
            accumulation[lib_region]["eolien"].append(eolien)

    # On calcule la moyenne par region
    production = {}
    for region, valeurs in accumulation.items():
        sol = valeurs["solaire"]
        eol = valeurs["eolien"]
        production[region] = {
            "solaire_mw": round(sum(sol) / len(sol), 2) if sol else 0.0,
            "eolien_mw": round(sum(eol) / len(eol), 2) if eol else 0.0,
        }
        logging.info(f"{region} : solaire={production[region]['solaire_mw']} MW, eolien={production[region]['eolien_mw']} MW")

    return production


# ========== TACHE 4 : Analyse de correlation ==========
# On recupere les resultats des taches 2 et 3 via XCom (xcom_pull)
# On applique les regles metier de RTE pour detecter les anomalies :
#   - soleil > 6h mais production solaire faible = probleme
#   - vent > 30 km/h mais production eolienne faible = probleme
def analyser_correlation(**context):
    ti = context["ti"]
    # xcom_pull recupere les donnees retournees par les taches precedentes
    donnees_meteo = ti.xcom_pull(task_ids="collecter_meteo_regions")
    donnees_production = ti.xcom_pull(task_ids="collecter_production_electrique")

    alertes = {}
    for region in REGIONS:
        meteo = donnees_meteo.get(region, {})
        production = donnees_production.get(region, {})
        alertes_region = []

        ensoleillement = meteo.get("ensoleillement_h", 0)
        vent = meteo.get("vent_kmh", 0)
        solaire = production.get("solaire_mw", 0)
        eolien = production.get("eolien_mw", 0)

        # Regle 1 : beaucoup de soleil mais peu de production solaire
        if ensoleillement > 6 and solaire <= 1000:
            alertes_region.append(
                f"ALERTE SOLAIRE : {ensoleillement:.1f}h de soleil mais seulement {solaire:.0f} MW produits"
            )

        # Regle 2 : beaucoup de vent mais peu de production eolienne
        if vent > 30 and eolien <= 2000:
            alertes_region.append(
                f"ALERTE ÉOLIEN : vent à {vent:.1f} km/h mais seulement {eolien:.0f} MW produits"
            )

        # Regle 3 (bonus) : production solaire sans ensoleillement = donnees suspectes
        if solaire > 0 and ensoleillement == 0:
            alertes_region.append(
                f"ANOMALIE : {solaire:.0f} MW solaire mesures sans ensoleillement"
            )

        alertes[region] = {
            "alertes": alertes_region,
            "ensoleillement_h": ensoleillement,
            "vent_kmh": vent,
            "solaire_mw": solaire,
            "eolien_mw": eolien,
            "statut": "ALERTE" if alertes_region else "OK",
        }

    nb_alertes = sum(1 for r in alertes.values() if r["statut"] == "ALERTE")
    logging.warning(f"{nb_alertes} region(s) en alerte sur {len(REGIONS)}.")
    return alertes


# ========== TACHE 5 : Generation du rapport ==========
# On affiche un tableau dans les logs Airflow pour voir les resultats rapidement
# Et on sauvegarde un fichier JSON dans /tmp pour les equipes metier
def generer_rapport_energie(**context):
    ti = context["ti"]
    analyse = ti.xcom_pull(task_ids="analyser_correlation")
    today = date.today().isoformat()

    # Affichage du tableau dans les logs Airflow
    print("\n" + "=" * 80)
    print(f"  RAPPORT ENERGIE & METEO — RTE — {today}")
    print("=" * 80)
    print(f"{'Region':<25} {'Soleil (h)':>10} {'Vent (km/h)':>12} {'Solaire (MW)':>13} {'Eolien (MW)':>12} {'Statut':>8}")
    print("-" * 80)
    for region, data in analyse.items():
        print(f"{region:<25} {data['ensoleillement_h']:>10.1f} {data['vent_kmh']:>12.1f} {data['solaire_mw']:>13.0f} {data['eolien_mw']:>12.0f} {data['statut']:>8}")
    print("=" * 80 + "\n")

    # Construction du rapport JSON
    rapport = {
        "date": today,
        "source": "RTE eCO2mix + Open-Meteo",
        "pipeline": "energie_meteo_dag",
        "regions": analyse,
        "resume": {
            "nb_regions_analysees": len(analyse),
            "nb_alertes": sum(1 for r in analyse.values() if r["statut"] == "ALERTE"),
            "regions_en_alerte": [r for r, d in analyse.items() if d["statut"] == "ALERTE"],
        },
    }

    # Sauvegarde du fichier JSON
    chemin = f"/tmp/rapport_energie_{today}.json"
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    logging.info(f"Rapport sauvegarde : {chemin}")
    return chemin  # le chemin est stocke dans XCom


# ========== DEFINITION DU DAG ==========
with DAG(
    dag_id="energie_meteo_dag",
    default_args=default_args,
    description="Correlation meteo / production energetique — RTE",
    schedule="0 6 * * *",                              # tous les jours a 6h du matin
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),  # date de debut du DAG
    catchup=False,                                      # on ne rattrape pas les runs passes
    tags=["rte", "energie", "meteo", "open-data"],
    sla_miss_callback=sla_miss_callback,                # (Exo 1) callback si SLA depasse
) as dag:

    t1 = PythonOperator(task_id="verifier_apis", python_callable=verifier_apis)
    t2 = PythonOperator(task_id="collecter_meteo_regions", python_callable=collecter_meteo_regions)
    t3 = PythonOperator(task_id="collecter_production_electrique", python_callable=collecter_production_electrique)
    t4 = PythonOperator(task_id="analyser_correlation", python_callable=analyser_correlation)
    # (Exo 1) SLA de 45 min specifique a cette tache : le rapport doit etre pret vite
    t5 = PythonOperator(task_id="generer_rapport_energie", python_callable=generer_rapport_energie, sla=timedelta(minutes=45))

    # Dependances : t1 d'abord, puis t2 et t3 en parallele, puis t4, puis t5
    t1 >> [t2, t3] >> t4 >> t5
