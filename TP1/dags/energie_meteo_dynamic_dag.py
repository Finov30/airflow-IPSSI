# Exercice 2 — Dynamic Task Mapping
# Au lieu de boucler sur les regions dans une seule tache,
# on utilise .expand() pour creer automatiquement une tache par region.
# Avantage : si RTE ajoute des regions, on modifie juste la Variable Airflow,
# pas besoin de toucher au code.

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pendulum
import requests
import logging
import json
from datetime import date

local_tz = pendulum.timezone("Europe/Paris")

default_args = {
    "owner": "rte-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# @dag remplace le "with DAG(...)" classique, c'est le style TaskFlow API
@dag(
    dag_id="energie_meteo_dynamic_dag",
    default_args=default_args,
    description="Version dynamic mapping du pipeline energie/meteo",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["rte", "energie", "meteo", "dynamic-mapping"],
)
def energie_meteo_dynamic():

    @task()
    def charger_config_regions() -> list:
        """
        Charge la liste des regions depuis la Variable Airflow 'regions_energie'.
        Si la variable n'existe pas, on utilise les 5 regions par defaut.
        Retourne une liste de dicts, chaque dict = une region.
        """
        from airflow.models import Variable

        # Regions par defaut si la Variable Airflow n'est pas definie
        default_regions = json.dumps([
            {"nom": "Île-de-France", "lat": 48.8566, "lon": 2.3522, "seuil": 800},
            {"nom": "Occitanie", "lat": 43.6047, "lon": 1.4442, "seuil": 800},
            {"nom": "Nouvelle-Aquitaine", "lat": 44.8378, "lon": -0.5792, "seuil": 800},
            {"nom": "Auvergne-Rhône-Alpes", "lat": 45.7640, "lon": 4.8357, "seuil": 800},
            {"nom": "Hauts-de-France", "lat": 50.6292, "lon": 3.0573, "seuil": 800},
        ])
        # Variable.get lit la variable depuis l'UI Airflow (Admin > Variables)
        regions = json.loads(Variable.get("regions_energie", default_var=default_regions))

        # Bonus : on peut exclure certaines regions via une autre Variable
        exclues = json.loads(Variable.get("regions_exclues", default_var="[]"))
        regions = [r for r in regions if r["nom"] not in exclues]

        logging.info(f"{len(regions)} regions chargees")
        return regions

    @task()
    def extraire_meteo_region(region: dict) -> dict:
        """
        Recupere la meteo pour UNE seule region.
        Grace a .expand(), cette tache sera executee N fois (une par region).
        Dans l'UI Airflow on verra des sous-instances [0], [1], [2]...
        """
        params = {
            "latitude": region["lat"],
            "longitude": region["lon"],
            "daily": "sunshine_duration,wind_speed_10m_max",
            "timezone": "Europe/Paris",
            "forecast_days": 1,
        }
        response = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        # Conversion secondes -> heures pour l'ensoleillement
        ensoleillement_h = round(data["daily"]["sunshine_duration"][0] / 3600, 2)
        vent_kmh = data["daily"]["wind_speed_10m_max"][0]

        logging.info(f"{region['nom']} : soleil={ensoleillement_h}h, vent={vent_kmh} km/h")
        return {
            "nom": region["nom"],
            "seuil": region.get("seuil", 800),
            "ensoleillement_h": ensoleillement_h,
            "vent_kmh": vent_kmh,
        }

    @task()
    def collecter_production() -> dict:
        """
        Recupere la production electrique de toutes les regions en un seul appel API.
        On accumule les valeurs horaires et on fait la moyenne par region.
        """
        url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/records"
        response = requests.get(url, params={"limit": 100, "timezone": "Europe/Paris"}, timeout=15)
        response.raise_for_status()
        results = response.json()["results"]

        # On accumule les valeurs par region
        accumulation = {}
        for rec in results:
            region = rec.get("libelle_region", "")
            if region not in accumulation:
                accumulation[region] = {"solaire": [], "eolien": []}
            solaire_raw = rec.get("solaire")
            eolien_raw = rec.get("eolien")
            accumulation[region]["solaire"].append(float(solaire_raw) if solaire_raw is not None else 0.0)
            accumulation[region]["eolien"].append(float(eolien_raw) if eolien_raw is not None else 0.0)

        # Moyenne par region
        production = {}
        for region, val in accumulation.items():
            production[region] = {
                "solaire_mw": round(sum(val["solaire"]) / len(val["solaire"]), 2) if val["solaire"] else 0.0,
                "eolien_mw": round(sum(val["eolien"]) / len(val["eolien"]), 2) if val["eolien"] else 0.0,
            }
        return production

    @task()
    def analyser_et_generer_rapport(meteos: list, production: dict) -> str:
        """
        Recoit la liste des resultats meteo (une entree par region, grace au mapping)
        et les donnees de production. Compare et genere le rapport.
        """
        today = date.today().isoformat()
        alertes = {}

        # On boucle sur chaque resultat meteo (un par region mappee)
        for meteo in meteos:
            region = meteo["nom"]
            prod = production.get(region, {"solaire_mw": 0, "eolien_mw": 0})
            alertes_region = []

            # Meme regles que le DAG principal
            if meteo["ensoleillement_h"] > 6 and prod["solaire_mw"] <= meteo.get("seuil", 1000):
                alertes_region.append(
                    f"ALERTE SOLAIRE : {meteo['ensoleillement_h']:.1f}h de soleil mais {prod['solaire_mw']:.0f} MW"
                )
            if meteo["vent_kmh"] > 30 and prod["eolien_mw"] <= 2000:
                alertes_region.append(
                    f"ALERTE EOLIEN : vent {meteo['vent_kmh']:.1f} km/h mais {prod['eolien_mw']:.0f} MW"
                )

            alertes[region] = {
                "alertes": alertes_region,
                "ensoleillement_h": meteo["ensoleillement_h"],
                "vent_kmh": meteo["vent_kmh"],
                "solaire_mw": prod["solaire_mw"],
                "eolien_mw": prod["eolien_mw"],
                "statut": "ALERTE" if alertes_region else "OK",
            }

        # Affichage du tableau dans les logs
        print("\n" + "=" * 80)
        print(f"  RAPPORT ENERGIE & METEO (DYNAMIC) — {today}")
        print("=" * 80)
        print(f"{'Region':<25} {'Soleil (h)':>10} {'Vent (km/h)':>12} {'Solaire (MW)':>13} {'Eolien (MW)':>12} {'Statut':>8}")
        print("-" * 80)
        for region, data in alertes.items():
            print(f"{region:<25} {data['ensoleillement_h']:>10.1f} {data['vent_kmh']:>12.1f} {data['solaire_mw']:>13.0f} {data['eolien_mw']:>12.0f} {data['statut']:>8}")
        print("=" * 80 + "\n")

        # Sauvegarde du rapport JSON
        rapport = {
            "date": today,
            "source": "RTE eCO2mix + Open-Meteo",
            "pipeline": "energie_meteo_dynamic_dag",
            "regions": alertes,
            "resume": {
                "nb_regions": len(alertes),
                "nb_alertes": sum(1 for r in alertes.values() if r["statut"] == "ALERTE"),
                "regions_en_alerte": [r for r, d in alertes.items() if d["statut"] == "ALERTE"],
            },
        }
        chemin = f"/tmp/rapport_energie_dynamic_{today}.json"
        with open(chemin, "w", encoding="utf-8") as f:
            json.dump(rapport, f, ensure_ascii=False, indent=2)

        logging.info(f"Rapport sauvegarde : {chemin}")
        return chemin

    # ========== FLUX DU DAG ==========
    # 1. On charge la liste des regions
    regions = charger_config_regions()
    # 2. .expand() cree UNE instance de extraire_meteo_region par region
    #    -> dans l'UI on verra extraire_meteo_region[0], [1], [2], [3], [4]
    meteos = extraire_meteo_region.expand(region=regions)
    # 3. On collecte la production en parallele
    production = collecter_production()
    # 4. On analyse tout et on genere le rapport
    analyser_et_generer_rapport(meteos=meteos, production=production)


# On appelle la fonction pour enregistrer le DAG dans Airflow
energie_meteo_dynamic()
