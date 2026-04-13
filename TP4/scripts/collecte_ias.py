#!/usr/bin/env python3
"""
Script de collecte des donnees IAS (Indicateurs Avances Sanitaires)
Grippe et Gastro-enterite -- OpenHealth / data.gouv.fr
"""
import csv
import io
import json
import logging
import os
from datetime import datetime, date
from typing import Optional

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATASETS_IAS = {
    "GRIPPE": "https://www.data.gouv.fr/api/1/datasets/r/35f46fbb-7a97-46b3-a93c-35a471033447",
    "GEA": "https://www.data.gouv.fr/api/1/datasets/r/6c415be9-4ebf-4af5-b0dc-9867bb1ec0e3",
}

# Les CSV utilisent les anciens codes region (avant reforme 2016)
# Occitanie = Midi-Pyrenees (73) + Languedoc-Roussillon (91)
# On prend la moyenne des deux pour couvrir toute l'Occitanie
COLS_OCCITANIE = ["Loc_Reg73", "Loc_Reg91"]


def get_semaine_iso(reference_date: Optional[date] = None) -> str:
    """Retourne la semaine ISO au format YYYY-SXX."""
    if reference_date is None:
        reference_date = date.today()
    year, week, _ = reference_date.isocalendar()
    return f"{year}-S{week:02d}"


def telecharger_csv_ias(url: str) -> list:
    """Telecharge et parse un CSV IAS depuis data.gouv.fr."""
    logger.info("Telechargement : %s", url)
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    content = response.content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(content), delimiter=";")
    rows = []
    for row in reader:
        cleaned = {
            k: v.replace(",", ".") if v not in ("NA", "", None) else None
            for k, v in row.items()
        }
        rows.append(cleaned)

    logger.info("%d lignes recuperees", len(rows))
    return rows


def filtrer_semaine(rows: list, semaine: str) -> list:
    """Filtre les lignes correspondant a la semaine ISO demandee."""
    annee_cible = int(semaine[:4])
    num_sem_cible = int(semaine.split("S")[1])

    filtered = []
    for row in rows:
        periode = row.get("PERIODE")
        if not periode:
            continue
        try:
            d = datetime.strptime(periode, "%d-%m-%Y").date()
            iso_year, iso_week, _ = d.isocalendar()
            if iso_year == annee_cible and iso_week == num_sem_cible:
                filtered.append(row)
        except ValueError:
            continue

    logger.info("%d jours pour la semaine %s", len(filtered), semaine)
    return filtered


def agreger_semaine(rows: list, syndrome: str, semaine: str) -> dict:
    """Agrege les lignes d'une semaine en une valeur IAS hebdomadaire."""
    saisons_cols = [
        "Sais_2023_2024", "Sais_2022_2023", "Sais_2021_2022",
        "Sais_2020_2021", "Sais_2019_2020",
    ]

    valeurs_ias: list = []
    min_saison_vals: list = []
    max_saison_vals: list = []
    historique: dict = {col: [] for col in saisons_cols}

    for row in rows:
        # Moyenne des 2 anciennes regions pour couvrir l'Occitanie
        vals_reg = []
        for col in COLS_OCCITANIE:
            v = row.get(col)
            if v is not None:
                try:
                    vals_reg.append(float(v))
                except ValueError:
                    pass
        if vals_reg:
            valeurs_ias.append(sum(vals_reg) / len(vals_reg))

        for col in saisons_cols:
            v = row.get(col)
            if v is not None:
                try:
                    historique[col].append(float(v))
                except ValueError:
                    pass

        for field, dest in [("MIN_Saison", min_saison_vals), ("MAX_Saison", max_saison_vals)]:
            v = row.get(field)
            if v is not None:
                try:
                    dest.append(float(v))
                except ValueError:
                    pass

    def safe_mean(lst: list) -> Optional[float]:
        return round(sum(lst) / len(lst), 3) if lst else None

    return {
        "semaine": semaine,
        "syndrome": syndrome,
        "valeur_ias": safe_mean(valeurs_ias),
        "seuil_min": safe_mean(min_saison_vals),
        "seuil_max": safe_mean(max_saison_vals),
        "nb_jours": len(valeurs_ias),
        "historique": {col: safe_mean(vals) for col, vals in historique.items()},
    }


def sauvegarder_donnees(donnees: dict, semaine: str, output_dir: str) -> str:
    """Sauvegarde les donnees IAS agregees en JSON."""
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"ias_{semaine}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "semaine": semaine,
            "collecte_le": datetime.utcnow().isoformat(),
            "source": "IAS_OpenHealth_data.gouv.fr",
            "syndromes": donnees,
        }, f, ensure_ascii=False, indent=2)
    logger.info("Donnees sauvegardees : %s", output_path)
    return output_path


if __name__ == "__main__":
    semaine = os.environ.get("SEMAINE_CIBLE", get_semaine_iso())
    output_dir = os.environ.get("OUTPUT_DIR", "/data/ars/raw")

    resultats = {}
    for syndrome, url in DATASETS_IAS.items():
        rows_all = telecharger_csv_ias(url)
        rows_sem = filtrer_semaine(rows_all, semaine)
        resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)

    chemin = sauvegarder_donnees(resultats, semaine, output_dir)
    print(f"COLLECTE_OK:{chemin}")
