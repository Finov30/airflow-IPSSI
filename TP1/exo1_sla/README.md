# Exercice 1 — SLA et alertes de delai

Les modifications sont dans `../dags/energie_meteo_dag.py` :

- `default_args["sla"] = 90 min` : SLA global sur toutes les taches
- `sla=timedelta(minutes=45)` sur la tache `generer_rapport_energie`
- Fonction `sla_miss_callback` : loggue `[ALERTE SLA]` quand un SLA est depasse
- Le callback est branche via `sla_miss_callback=sla_miss_callback` dans le DAG

Pour tester : mettre le SLA a 1 seconde, trigger le DAG, puis aller dans Browse > SLA Misses.
