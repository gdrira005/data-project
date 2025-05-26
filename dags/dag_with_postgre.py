from datetime import datetime, timedelta # Ce n'est pas obligatoire
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator 
from sqlalchemy import create_engine # Ce n'est pas obligatoire
import pandas as pd # Ce n'est pas obligatoire


#fonction pour la connexion et il faut l'appeler apres avec pythonOperator
def fonction():
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/test")
    query = """
        SELECT AVG(col2) FROM table1 GROUP BY col1
    """
    result = pd.read_sql(query, engine)
    print(result)


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    dag_id='dag_with_postgres_operator',
    default_args=default_args,
    description='Un DAG avec lecture de base de données PostgreSQL',
    start_date=datetime(2025, 5, 6),
    schedule_interval=None,
    catchup=False,
    tags=['exemple']
) as dag:

    task1 = PostgresOperator(
        task_id='lecture_bdd_postgres',
        postgres_conn_id='postgres_localhost',
        sql="select 	cq.type_defaut, SUM(cq.nb_defauts)AS total_defauts from controles_qualite as cq group by cq.type_defaut "
    )

    task1
