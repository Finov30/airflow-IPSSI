# TP Jour 2 — Logs E-Commerce + HDFS — Reponses

## Q1 — HDFS vs systeme de fichiers local

3 avantages de HDFS pour 50 Go/jour de logs :

1. **Distribution** : HDFS repartit les fichiers sur plusieurs machines (DataNodes). Un disque local est limite a la capacite d'un seul serveur. Avec HDFS, on ajoute des DataNodes pour avoir plus d'espace.

2. **Replication** : HDFS copie chaque fichier sur plusieurs DataNodes (facteur 3 en prod). Si un disque tombe en panne, les donnees sont toujours accessibles sur les autres copies. Un NFS ou un disque local n'a pas cette securite.

3. **Localite des donnees** : Quand on lance un traitement Spark ou MapReduce, HDFS envoie le calcul la ou se trouvent les donnees (au lieu de deplacer les donnees vers le calcul). Ca evite de transferer des Go sur le reseau, ce qui est beaucoup plus rapide.

---

## Q2 — NameNode, point de defaillance unique (SPOF)

Si le NameNode tombe :
- Les DataNodes continuent de stocker les donnees mais personne ne sait ou elles sont (le NameNode gere l'index)
- Les clients ne peuvent plus lire ni ecrire dans HDFS
- Les donnees ne sont pas perdues, mais inaccessibles

Pour resoudre ca, Hadoop propose le **HDFS NameNode HA** (Haute Disponibilite) :
- On a 2 NameNodes : un **actif** et un **standby** (en attente)
- Les **JournalNodes** (au moins 3) synchronisent les modifications entre les 2 NameNodes
- Si le NameNode actif tombe, le standby prend le relais automatiquement
- Le JournalNode garde un journal des modifications du filesystem, le standby le lit en continu pour rester a jour

---

## Q3 — HdfsSensor poke vs reschedule

- **Mode poke** : le sensor occupe un slot de worker en permanence pendant qu'il attend. Il verifie toutes les X secondes mais ne libere pas le worker entre les verifications. Simple mais gaspille des ressources.

- **Mode reschedule** : le sensor verifie, et s'il ne trouve pas le fichier, il libere le worker et se replanifie plus tard. Ca economise les slots de workers.

En pratique :
- **poke** : ok pour des attentes courtes (< 5 min) ou en dev
- **reschedule** : recommande en prod avec CeleryExecutor, surtout si on a beaucoup de sensors

Scenario problematique : si on a 10 DAGs avec chacun un sensor en mode poke, et seulement 10 slots de workers, tous les slots sont bloques par les sensors et aucune vraie tache ne peut s'executer. Le scheduler est completement bloque.

---

## Q4 — Replication HDFS facteur 3

Quand on ecrit un bloc de 128 Mo avec un facteur de replication de 3 :

1. Le client contacte le NameNode qui lui donne la liste de 3 DataNodes
2. Le client envoie le bloc au **DataNode 1**
3. Le DataNode 1 le transmet au **DataNode 2** (pipeline)
4. Le DataNode 2 le transmet au **DataNode 3**
5. Les confirmations remontent dans l'ordre inverse : DN3 -> DN2 -> DN1 -> Client

Au total : **3 copies** sur **3 DataNodes differents**. L'ecriture se fait en pipeline (pas en parallele) pour economiser la bande passante du client.

Pour la coherence en lecture : HDFS garantit que **le fichier n'est visible qu'une fois l'ecriture terminee** (apres le close). Si quelqu'un essaie de lire pendant l'ecriture, il ne voit que les blocs deja valides, pas le bloc en cours d'ecriture.
