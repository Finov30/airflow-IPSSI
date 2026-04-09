# ============================================================
# DAG Exercice Jour 1 — Premier workflow avec 3 taches
# But : creer un DAG avec BashOperator et PythonOperator
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# default_args : parametres par defaut pour toutes les taches
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,  # bonus : pas de mail si ca plante
}


# Fonction appelee par la tache 2 (PythonOperator)
# Elle retourne la date du jour — la valeur retournee est stockee dans XCom
def get_current_date():
    today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Date du jour : {today}")
    return today


# Definition du DAG
with DAG(
    dag_id='exercice_jour1',
    default_args=default_args,
    description='TP Jour 1 - Premier workflow Airflow',
    schedule='@daily',                    # une fois par jour
    start_date=datetime(2025, 1, 1),
    catchup=False,                        # pas de rattrapage des jours passes
    tags=['tp', 'jour1'],                 # bonus : tags pour filtrer dans l'UI
) as dag:

    # Tache 1 : affiche le debut du workflow (commande bash)
    t1 = BashOperator(
        task_id='debut_workflow',
        bash_command='echo "Debut du workflow"',
    )

    # Tache 2 : recupere et affiche la date du jour (fonction Python)
    t2 = PythonOperator(
        task_id='date_du_jour',
        python_callable=get_current_date,
    )

    # Tache 3 : affiche la fin du workflow (commande bash)
    t3 = BashOperator(
        task_id='fin_workflow',
        bash_command='echo "Fin du workflow"',
    )

    # Ordre d'execution : t1 -> t2 -> t3
    t1 >> t2 >> t3
