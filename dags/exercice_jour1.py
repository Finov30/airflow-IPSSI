from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# default_args avec email_on_failure=False (bonus)
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


# Fonction Python pour la tache 2
def get_current_date():
    today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Date du jour : {today}")
    return today


# Definition du DAG avec tags (bonus)
with DAG(
    dag_id='exercice_jour1',
    default_args=default_args,
    description='TP Jour 1 - Premier workflow Airflow',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['tp', 'jour1'],
) as dag:

    # Tache 1 : Affiche "Debut du workflow" (BashOperator)
    t1 = BashOperator(
        task_id='debut_workflow',
        bash_command='echo "Debut du workflow"',
    )

    # Tache 2 : Retourne la date du jour (PythonOperator)
    t2 = PythonOperator(
        task_id='date_du_jour',
        python_callable=get_current_date,
    )

    # Tache 3 : Affiche "Fin du workflow" (BashOperator)
    t3 = BashOperator(
        task_id='fin_workflow',
        bash_command='echo "Fin du workflow"',
    )

    # Dependances
    t1 >> t2 >> t3
