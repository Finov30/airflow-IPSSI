# ============================================================
# DAG Hello World — Premier DAG Airflow (Slide 13-14)
# But : comprendre la structure de base d'un DAG
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# default_args = arguments communs a toutes les taches du DAG
# Pas besoin de les repeter dans chaque tache
default_args = {
    'owner': 'admin',          # qui est responsable de ce DAG
    'retries': 1,              # si une tache echoue, on retente 1 fois
    'retry_delay': timedelta(minutes=5),  # attendre 5 min avant de retenter
}

# "with DAG(...) as dag:" = context manager pour definir le DAG
# Tout ce qui est indente dedans fait partie de ce DAG
with DAG(
    dag_id='hello_world',               # nom unique du DAG dans Airflow
    default_args=default_args,
    description='Premier DAG Hello World',
    schedule_interval='@daily',          # s'execute une fois par jour
    start_date=datetime(2025, 1, 1),     # a partir de quand le DAG est actif
    catchup=False,                       # ne pas rattraper les jours passes
) as dag:

    # --- Tache 1 : BashOperator ---
    # Execute une commande bash (comme dans le terminal)
    hello_bash = BashOperator(
        task_id='hello_bash',
        bash_command='echo "Hello World depuis BashOperator!"',
    )

    # --- Tache 2 : PythonOperator ---
    # Execute une fonction Python
    def say_hello():
        print("Hello World depuis PythonOperator!")

    hello_python = PythonOperator(
        task_id='hello_python',
        python_callable=say_hello,  # on passe la fonction (sans les parentheses)
    )

    # --- Tache 3 : BashOperator ---
    # Affiche la date de fin d'execution
    show_date = BashOperator(
        task_id='show_date',
        bash_command='echo "Execution terminee le $(date)"',
    )

    # --- Dependances ---
    # >> veut dire "doit s'executer avant"
    # hello_bash -> hello_python -> show_date (dans cet ordre)
    hello_bash >> hello_python >> show_date
