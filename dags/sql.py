from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

def lire_defauts_depuis_postgres(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    sql = """
     WITH t1 AS (
        SELECT id, "surveyDefaultResponse_id", "surveyQuestion_id"
        FROM "surveyQuestionResponse_surveyquestionresponse"
    ),
    t2 AS (
        SELECT id, response
        FROM "surveyDefaultResponse_surveydefaultresponse"
    ),
    t3 AS (
        SELECT id AS question_id, code, question
        FROM "surveyQuestions_surveyquestions"
    )
    SELECT t1.id, t2.response, t3.code, t3.question
    FROM t1
    JOIN t2 ON t1."surveyDefaultResponse_id" = t2.id
    JOIN t3 ON t1."surveyQuestion_id" = t3.question_id
    GROUP BY t3.question, t1.id, t2.response, t3.code
    ORDER BY t1.id;
    """
    result = hook.get_records(sql)
    print("=== Résultat SQL ===")
    for row in result:
        print(row)
    return result  # stocké dans XCom automatiquement

with DAG(
    dag_id='dag_sql_postgres_resultats_visibles',
    default_args=default_args,
    description='DAG pour lire et afficher les résultats SQL depuis PostgreSQL',
    start_date=datetime(2025, 5, 6),
    schedule_interval=None,
    catchup=False,
    tags=['exemple']
) as dag:

    task1 = PythonOperator(
        task_id='lire_defauts',
        python_callable=lire_defauts_depuis_postgres,
        provide_context=True  # Pour que kwargs['ti'] fonctionne si besoin
    )

    task1

