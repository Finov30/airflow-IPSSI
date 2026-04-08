from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 1. default_args : arguments appliques a toutes les taches du DAG
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Definition du DAG avec context manager
with DAG(
    dag_id='hello_world',
    default_args=default_args,
    description='Premier DAG Hello World',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # 3. Declaration des taches

    # Tache 1 : BashOperator - affiche Hello World
    hello_bash = BashOperator(
        task_id='hello_bash',
        bash_command='echo "Hello World depuis BashOperator!"',
    )

    # Tache 2 : PythonOperator - affiche un message via Python
    def say_hello():
        print("Hello World depuis PythonOperator!")

    hello_python = PythonOperator(
        task_id='hello_python',
        python_callable=say_hello,
    )

    # Tache 3 : BashOperator - affiche la date
    show_date = BashOperator(
        task_id='show_date',
        bash_command='echo "Execution terminee le $(date)"',
    )

    # 4. Dependances avec l'operateur >> (slide 14)
    hello_bash >> hello_python >> show_date
