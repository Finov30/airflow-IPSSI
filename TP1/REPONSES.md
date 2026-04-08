# TP Jour 1 — Reponses

## Q1 — Docker Executor

Il existe 3 types d'executors dans Airflow :

- **LocalExecutor** : execute les taches en parallele sur une seule machine. Simple a configurer, ideal pour le dev ou les petits projets. C'est celui qu'on utilise dans ce TP.

- **CeleryExecutor** : utilise un systeme de file d'attente (Redis ou RabbitMQ) pour distribuer les taches sur plusieurs machines (workers). On peut ajouter des workers si la charge augmente. Bien pour la production avec une charge reguliere.

- **KubernetesExecutor** : chaque tache tourne dans son propre conteneur (pod) sur un cluster Kubernetes. Le pod est cree au besoin puis detruit. Tres scalable mais plus complexe a mettre en place.

Pour RTE :
- En dev : LocalExecutor (comme ici)
- En prod avec charge normale : CeleryExecutor
- En prod avec beaucoup de DAGs et besoin de scalabilite : KubernetesExecutor

---

## Q2 — Volumes Docker et persistance des DAGs

**Bind mount** (`./dags:/opt/airflow/dags`) : le dossier de notre PC est directement lie au dossier dans le conteneur. Si on modifie un fichier sur notre PC, le conteneur le voit tout de suite.

**Volume nomme** (`postgres-db-volume`) : c'est Docker qui gere le stockage en interne. On n'y accede pas directement depuis notre PC.

Si on supprime le mapping `./dags:/opt/airflow/dags`, le conteneur ne voit plus nos DAGs. Il faudrait les copier a la main dans le conteneur a chaque fois.

En production avec plusieurs workers (machines), le bind mount ne marche plus car chaque machine a son propre disque. Il faut soit un dossier partage en reseau (NFS), soit un systeme qui synchronise les DAGs depuis Git (git-sync).

---

## Q3 — Idempotence et catchup

**catchup=True** : si on active le DAG aujourd'hui avec un `start_date` au 1er janvier 2024, Airflow va essayer de rattraper tous les jours manques, soit ~830 executions d'un coup. Ca va surcharger le systeme et spammer les APIs. Avec **catchup=False**, seule la derniere execution est lancee.

**Idempotence** : ca veut dire que si on relance le meme DAG Run 2 fois, on obtient le meme resultat sans doublons. C'est important car si une tache plante et qu'on la relance, on ne veut pas dupliquer les donnees.

Pour rendre les fonctions `collecter_*` idempotentes :
- Utiliser la date logique du DAG (`context["logical_date"]`) au lieu de `date.today()`
- Ecraser le fichier de rapport s'il existe deja (c'est deja le cas avec notre nommage par date)

---

## Q4 — Timezone et donnees temps-reel

`timezone=Europe/Paris` est important car RTE travaille en heure francaise. Sans ca, Airflow utilise UTC par defaut et les donnees seraient decalees de 1h (hiver) ou 2h (ete) par rapport a la realite francaise.

**Probleme au passage a l'heure d'ete** (dernier dimanche de mars) : on passe de 02:00 a 03:00 directement. Si la timezone est mal geree :
- Le DAG prevu a 06:00 pourrait ne pas se declencher ou se declencher 2 fois
- Les donnees de l'API entre 02:00 et 03:00 pourraient etre manquantes (cette heure n'existe pas ce jour-la)
- Le rapport du jour aurait 23h de donnees au lieu de 24h

C'est pour ca qu'on utilise `pendulum.timezone("Europe/Paris")` dans le DAG et `timezone=Europe/Paris` dans les requetes API.
