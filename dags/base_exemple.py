from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd  # déplacé ici car utilisé dans la fonction

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Fonction Python pour lire un fichier CSV déjà présent dans le dossier dags/
def download_kaggle_dataset():
    path = "/usr/local/airflow/dags/TurkiyeStudentEvaluation.csv"
    df = pd.read_csv(path)
    print("Aperçu du fichier :", df.head())

with DAG(
    dag_id='dag_with_postgres_and_kaggle',
    default_args=default_args,
    description='Un DAG avec lecture PostgreSQL et traitement de données CSV',
    start_date=datetime(2025, 5, 1),
    schedule=None,
    catchup=False,
    tags=['exemple']
) as dag:

    telechargement_kaggle = PythonOperator(
        task_id='lecture_csv_local',
        python_callable=download_kaggle_dataset
    )

    telechargement_kaggle
