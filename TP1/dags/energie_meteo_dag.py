from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import requests
import logging
import json
from datetime import date

local_tz = pendulum.timezone("Europe/Paris")

# Les 5 regions qu'on veut surveiller
REGIONS = {
    "Île-de-France":        {"lat": 48.8566, "lon": 2.3522},
    "Occitanie":            {"lat": 43.6047, "lon": 1.4442},
    "Nouvelle-Aquitaine":   {"lat": 44.8378, "lon": -0.5792},
    "Auvergne-Rhône-Alpes": {"lat": 45.7640, "lon": 4.8357},
    "Hauts-de-France":      {"lat": 50.6292, "lon": 3.0573},
}

default_args = {
    "owner": "rte-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def verifier_apis(**context):
    """Teste que les 2 APIs repondent bien avant de lancer le pipeline."""
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
    for nom, url in apis.items():
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                raise ValueError(f"API {nom} a retourne le status {response.status_code}")
            logging.info(f"API {nom} : OK")
        except requests.exceptions.RequestException as e:
            raise ValueError(f"API {nom} indisponible : {e}")

    logging.info("Toutes les APIs sont dispo.")


def collecter_meteo_regions(**context):
    """Recupere ensoleillement et vent pour chaque region via Open-Meteo."""
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    resultats = {}

    for region, coords in REGIONS.items():
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "daily": "sunshine_duration,wind_speed_10m_max",
            "timezone": "Europe/Paris",
            "forecast_days": 1,
        }
        response = requests.get(BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        # sunshine_duration est en secondes, on convertit en heures
        ensoleillement_h = round(data["daily"]["sunshine_duration"][0] / 3600, 2)
        vent_kmh = data["daily"]["wind_speed_10m_max"][0]

        resultats[region] = {"ensoleillement_h": ensoleillement_h, "vent_kmh": vent_kmh}
        logging.info(f"{region} : soleil={ensoleillement_h}h, vent={vent_kmh} km/h")

    return resultats


def collecter_production_electrique(**context):
    """Recupere la production solaire et eolienne par region via eCO2mix."""
    BASE_URL = (
        "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
        "/eco2mix-regional-cons-def/records"
    )
    params = {"limit": 100, "timezone": "Europe/Paris"}

    response = requests.get(BASE_URL, params=params, timeout=15)
    response.raise_for_status()
    results = response.json()["results"]

    # On accumule les valeurs par region pour faire la moyenne
    accumulation = {region: {"solaire": [], "eolien": []} for region in REGIONS}

    for enregistrement in results:
        lib_region = enregistrement.get("libelle_region", "")
        if lib_region in REGIONS:
            solaire_raw = enregistrement.get("solaire")
            eolien_raw = enregistrement.get("eolien")
            solaire = float(solaire_raw) if solaire_raw is not None else 0.0
            eolien = float(eolien_raw) if eolien_raw is not None else 0.0
            accumulation[lib_region]["solaire"].append(solaire)
            accumulation[lib_region]["eolien"].append(eolien)

    # Moyenne par region
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


def analyser_correlation(**context):
    """Compare meteo et production pour detecter des anomalies."""
    ti = context["ti"]
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

        # Beaucoup de soleil mais peu de production solaire
        if ensoleillement > 6 and solaire <= 1000:
            alertes_region.append(
                f"ALERTE SOLAIRE : {ensoleillement:.1f}h de soleil mais seulement {solaire:.0f} MW produits"
            )

        # Beaucoup de vent mais peu de production eolienne
        if vent > 30 and eolien <= 2000:
            alertes_region.append(
                f"ALERTE ÉOLIEN : vent à {vent:.1f} km/h mais seulement {eolien:.0f} MW produits"
            )

        # Production solaire sans soleil = donnees suspectes
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


def generer_rapport_energie(**context):
    """Affiche un tableau recapitulatif dans les logs et sauvegarde un JSON."""
    ti = context["ti"]
    analyse = ti.xcom_pull(task_ids="analyser_correlation")
    today = date.today().isoformat()

    # Tableau dans les logs
    print("\n" + "=" * 80)
    print(f"  RAPPORT ENERGIE & METEO — RTE — {today}")
    print("=" * 80)
    print(f"{'Region':<25} {'Soleil (h)':>10} {'Vent (km/h)':>12} {'Solaire (MW)':>13} {'Eolien (MW)':>12} {'Statut':>8}")
    print("-" * 80)
    for region, data in analyse.items():
        print(f"{region:<25} {data['ensoleillement_h']:>10.1f} {data['vent_kmh']:>12.1f} {data['solaire_mw']:>13.0f} {data['eolien_mw']:>12.0f} {data['statut']:>8}")
    print("=" * 80 + "\n")

    # Rapport JSON
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

    chemin = f"/tmp/rapport_energie_{today}.json"
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    logging.info(f"Rapport sauvegarde : {chemin}")
    return chemin


# --- DAG ---
with DAG(
    dag_id="energie_meteo_dag",
    default_args=default_args,
    description="Correlation meteo / production energetique — RTE",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["rte", "energie", "meteo", "open-data"],
) as dag:

    t1 = PythonOperator(task_id="verifier_apis", python_callable=verifier_apis)
    t2 = PythonOperator(task_id="collecter_meteo_regions", python_callable=collecter_meteo_regions)
    t3 = PythonOperator(task_id="collecter_production_electrique", python_callable=collecter_production_electrique)
    t4 = PythonOperator(task_id="analyser_correlation", python_callable=analyser_correlation)
    t5 = PythonOperator(task_id="generer_rapport_energie", python_callable=generer_rapport_energie)

    t1 >> [t2, t3] >> t4 >> t5
