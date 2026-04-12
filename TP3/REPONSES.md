# TP Jour 3 -- Pipeline Immobilier DVF : HDFS + PostgreSQL -- Reponses

## Checkpoint Jour 3

### Q1 -- Quel est l'atout principal du KubernetesExecutor ?

L'atout principal du KubernetesExecutor est l'**isolation totale** : chaque tache s'execute dans son propre pod Kubernetes avec sa propre image Docker. Cela signifie que :
- Chaque tache a ses propres dependances (pas de conflits entre taches)
- Les ressources (CPU, RAM) sont allouees et liberees dynamiquement pour chaque tache
- Le cluster **scale a zero** quand il n'y a pas de taches en attente, ce qui reduit les couts

Contrairement au CeleryExecutor ou les workers tournent en permanence et partagent le meme environnement, le KubernetesExecutor ne consomme des ressources que pendant l'execution effective des taches.

---

### Q2 -- Dans quels cas preferer CeleryExecutor ?

On prefere CeleryExecutor dans ces cas :
1. **Latence faible requise** : les workers Celery sont toujours en marche, donc les taches demarrent instantanement (pas de temps de lancement de pod)
2. **Beaucoup de petites taches frequentes** : si on a des centaines de taches courtes qui s'executent toutes les minutes, le temps de creation/destruction des pods K8s serait un goulot d'etranglement
3. **Pas de cluster Kubernetes disponible** : CeleryExecutor fonctionne avec un simple broker Redis/RabbitMQ, plus simple a mettre en place
4. **Taches avec des dependances homogenes** : si toutes les taches utilisent les memes librairies Python, l'isolation par pod n'est pas necessaire

---

### Q3 -- 3 champs essentiels pour un KubernetesPodOperator

1. **image** : l'image Docker a utiliser pour le pod (ex: `python:3.11-slim`). C'est l'environnement d'execution de la tache.
2. **cmds / arguments** : la commande a executer dans le conteneur. Sans ca, le pod ne fait rien.
3. **container_resources** : les requests et limits en CPU/memoire (ex: `requests={"cpu": "500m", "memory": "512Mi"}`). Indispensable pour eviter les OOM kills et garantir la stabilite du cluster.

Autres champs importants : `namespace`, `name`, `get_logs=True`, `is_delete_operator_pod=True`.

---

### Q4 -- Comment stocker les logs en production sur K8s ?

Sur Kubernetes, les pods workers sont **ephemeres** : ils sont detruits apres l'execution de la tache. Les logs stockes localement dans le pod disparaissent avec lui.

La solution est d'utiliser le **remote logging** : on configure Airflow pour envoyer les logs vers un stockage distant persistant :

- **S3** (AWS) : `remote_base_log_folder = s3://mon-bucket/airflow-logs/`
- **GCS** (Google Cloud) : `remote_base_log_folder = gs://mon-bucket/airflow-logs/`
- **Azure Blob Storage** : meme principe

Configuration dans `values.yaml` du Helm chart :
```yaml
config:
  logging:
    remote_logging: 'True'
    remote_base_log_folder: 's3://my-airflow-logs/prod'
    remote_log_conn_id: 'aws_default'
```

Les logs restent consultables normalement via l'interface web d'Airflow, qui les recupere depuis le stockage distant.

---

## Description du pipeline

### Architecture

Le pipeline suit un flux ETL lineaire :

```
data.gouv.fr (CSV DVF) --> Airflow --> HDFS (zone raw) --> pandas (traitement) --> PostgreSQL (zone curated)
```

- **Zone Raw (HDFS)** : stockage des fichiers CSV bruts, source de verite immuable
- **Zone Curated (PostgreSQL)** : donnees filtrees et agregees, consommees par les dashboards

### Taches du DAG

1. **verifier_sources** : verifie que l'API data.gouv.fr et HDFS repondent
2. **telecharger_dvf** : telecharge le CSV DVF en streaming (gros fichier > 500 Mo)
3. **stocker_hdfs_raw** : upload vers HDFS via l'API WebHDFS (double PUT : NameNode -> DataNode)
4. **traiter_donnees** : lit depuis HDFS, filtre Paris/Appartements, calcule prix/m2, agrege par arrondissement
5. **inserer_postgresql** : UPSERT des agregats dans les tables `prix_m2_arrondissement` et `stats_marche`
6. **generer_rapport** : affiche le classement des arrondissements par prix median au m2

### Commandes de demarrage

```bash
# Creer les repertoires
mkdir -p dags logs plugins sql

# Initialiser Airflow
docker compose up airflow-init

# Demarrer la stack
docker compose up -d

# Verifier les services
docker compose ps
```

### Acces

| Interface         | URL                    | Identifiants   |
|-------------------|------------------------|----------------|
| Airflow Web UI    | http://localhost:8080   | admin / admin  |
| HDFS NameNode UI  | http://localhost:9870   | --             |
| PostgreSQL        | localhost:5432          | airflow/airflow|
