# Exercice 2 — Dynamic Task Mapping avec .expand()

Le DAG est dans `../dags/energie_meteo_dynamic_dag.py`.

Difference avec le DAG principal :
- Les regions sont chargees depuis une Variable Airflow (pas en dur dans le code)
- `.expand(region=regions)` cree automatiquement une tache par region
- Si on ajoute des regions dans la Variable, le DAG s'adapte sans modifier le code

Pour tester :
1. Dans l'UI : Admin > Variables > ajouter `regions_energie` (JSON)
2. Trigger le DAG `energie_meteo_dynamic_dag`
3. Dans la vue Graph, la tache `extraire_meteo_region` a des sous-instances [0], [1], [2]...

Bonus : ajouter une variable `regions_exclues` = ["Occitanie"] pour filtrer.
