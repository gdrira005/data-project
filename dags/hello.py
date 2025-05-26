from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello1():
    print('Hello, World1 !')

def say_hello2():
    print('Hello, World2 !')

def say_hello3():
    print('Hello, World3 !')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    dag_id='hello_world',
    default_args=default_args,
    description='Un DAG qui affiche Hello World1 puis 2 puis 3',
    start_date=datetime(2025, 4, 21),
    schedule_interval=None,
    catchup=False,
    tags=['exemple']
) as dag:

    hello_task1 = PythonOperator(
        task_id='say_hello1',
        python_callable=say_hello1
    )

    hello_task2 = PythonOperator(
        task_id='say_hello2',
        python_callable=say_hello2
    )

    hello_task3 = PythonOperator(
        task_id='say_hello3',
        python_callable=say_hello3
    )

    
    hello_task1 >> hello_task2 >> hello_task3
