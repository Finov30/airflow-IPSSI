# TP Note Airflow -- Data Platform Sante Publique ARS Occitanie

## Auteur
- Nom : ABID
- Prenom : Samuel
- Formation : Master 2 Data Engineering -- IPSSI Montpellier
- Date : 2026-04-13

## Prerequis
- Docker Desktop >= 4.0
- Docker Compose >= 2.0

## Instructions de deploiement

### 1. Demarrage de la stack
```bash
cd TP4
docker compose up -d
docker compose ps
```

### 2. Configuration des connexions et variables Airflow
La connexion `postgres_ars` est configuree automatiquement via la variable d'environnement `AIRFLOW_CONN_POSTGRES_ARS` dans le docker-compose.yaml.

Creer les variables dans l'UI Airflow (Admin > Variables) :
- `archive_base_path` = `/data/ars`
- `seuil_alerte_zscore` = `1.5`
- `seuil_urgence_zscore` = `3.0`

### 3. Demarrage du pipeline
1. Aller sur http://localhost:8080 (admin / admin)
2. Activer le DAG `ars_epidemio_dag`
3. Cliquer sur "Trigger DAG"
4. Suivre l'execution dans la vue Graph

## Architecture des donnees

### Partitionnement des fichiers
```
/data/ars/
  raw/                      # Donnees brutes IAS
    <annee>/
      <semaine>/
        sursaud_YYYY-SXX.json
  indicateurs/              # Indicateurs calcules
    indicateurs_YYYY-SXX.json
  rapports/                 # Rapports hebdomadaires
    <annee>/
      <semaine>/
        rapport_YYYY-SXX.json
```

### Schema PostgreSQL (base ars_epidemio)
- `syndromes` : referentiel des syndromes surveilles (GRIPPE, GEA)
- `departements` : 13 departements d'Occitanie
- `donnees_hebdomadaires` : valeurs IAS aggregees par semaine et syndrome
- `indicateurs_epidemiques` : z-score, R0, classification par semaine
- `rapports_ars` : rapports JSON generes

### Flux du DAG
```
init_base_donnees
  -> collecte (telecharger IAS Grippe + GEA)
    -> persistance_brute (archiver + verifier)
      -> traitement (calculer z-score, R0, classification)
        -> persistance_operationnelle (UPSERT PostgreSQL)
          -> evaluer_situation_epidemique (BranchPythonOperator)
            -> URGENCE : declencher_alerte_ars
            -> ALERTE  : envoyer_bulletin_surveillance
            -> NORMAL  : confirmer_situation_normale
          -> generer_rapport_hebdomadaire (TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
```

## Decisions techniques
- **CeleryExecutor** : utilise pour la scalabilite avec Redis comme broker
- **2 PostgreSQL** : un pour les metadonnees Airflow, un dedie aux donnees ARS (separation des preoccupations)
- **Volume Docker nomme** (`ars-data`) : persistance des archives brutes et rapports
- **UPSERT** (ON CONFLICT DO UPDATE) : idempotence garantie sur toutes les insertions
- **BranchPythonOperator** : routage dynamique selon le niveau d'alerte
- **TaskGroups** : organisation logique des taches (collecte, archivage, traitement, persistance)

## Difficultes rencontrees et solutions
- Les CSV IAS utilisent `;` comme separateur et `,` comme decimal : gere dans le parsing
- Les colonnes Sais_20XX_20YY contiennent des `NA` : converties en None
- Le z-score necessite au moins 3 saisons de reference : protection implementee
