from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import time

# Paramètres du DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Fonction d'extraction
def extraire_donnees(**kwargs):
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
        SELECT id AS question_id, code, question, campaign_id
        FROM "surveyQuestions_surveyquestions"
    ),
    t4 AS (
        SELECT id AS cmp_id,
               resource,
               CONCAT(resource, ' version ', ROW_NUMBER() OVER (PARTITION BY resource ORDER BY id)) AS type_version
        FROM "campaign_campaign"
    )
    SELECT t1.id, t2.response, t3.code, t3.question, t4.cmp_id, t4.resource, t4.type_version
    FROM t1
    JOIN t2 ON t1."surveyDefaultResponse_id" = t2.id
    JOIN t3 ON t1."surveyQuestion_id" = t3.question_id
    JOIN t4 ON t3.campaign_id = t4.cmp_id
    ORDER BY t1.id;
    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=['id', 'response', 'code', 'question', 'cmp_id', 'resource', 'type_version'])
    kwargs['ti'].xcom_push(key='extracted_data_detailed', value=df.to_dict(orient='records'))

# Fonction de transformation
def transformer_donnees(**kwargs):
    extracted_data_dict = kwargs['ti'].xcom_pull(key='extracted_data_detailed', task_ids='extraire_donnees')
    extracted_data = pd.DataFrame(extracted_data_dict)

    relevant_responses = ['Agree', 'Disagree', 'Strongly agree', 'Strongly disagree']
    filtered_data = extracted_data[extracted_data['response'].isin(relevant_responses)].copy()
    filtered_data['question'] = filtered_data['question'].str.strip()

    count_data = filtered_data.groupby(['cmp_id', 'resource', 'type_version', 'question', 'response']).size().unstack(fill_value=0)
    count_data['positive_response'] = count_data.get('Strongly agree', 0) + count_data.get('Agree', 0)
    count_data['negative_response'] = count_data.get('Strongly disagree', 0) + count_data.get('Disagree', 0)
    count_data['total_responses'] = count_data['positive_response'] + count_data['negative_response']
    count_data = count_data.reset_index()
    count_data = count_data[count_data['total_responses'] > 0]

    
    count_data['satisfaction_rate'] = (count_data['positive_response'] / count_data['total_responses'] * 100).round(2)
    count_data['satisfaction_rate_display'] = count_data['satisfaction_rate'].astype(str) + '%'

    final_columns = ['cmp_id', 'resource', 'type_version', 'question', 'Strongly agree', 'Agree', 'Disagree',
                     'Strongly disagree', 'positive_response', 'negative_response', 'total_responses',
                      'satisfaction_rate', 'satisfaction_rate_display']

    for col in final_columns:
        if col not in count_data.columns:
            count_data[col] = 0

    count_data = count_data[final_columns]
    print("✅ Colonnes dans count_data avant push :", count_data.columns)
    kwargs['ti'].xcom_push(key='transformed_data', value=count_data.to_dict(orient='records'))

# Fonction de sauvegarde en CSV
def charger_donnees(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transformer_donnees')
    transformed_data = pd.DataFrame(df_dict)
    print("✅ Colonnes dans le DataFrame chargé pour CSV :", transformed_data.columns)

    dags_folder = '/usr/local/airflow/dags'
    output_dir = os.path.join(dags_folder, 'data')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    output_file_path = os.path.join(output_dir, f'transformed_data_{timestamp}.csv')
    transformed_data.to_csv(output_file_path, index=False)
    print(f"✅ Fichier CSV généré avec succès : {output_file_path}")

# Fonction de chargement PostgreSQL
def charger_donnees_dans_postgres(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transformer_donnees')
    transformed_data = pd.DataFrame(df_dict)
    transformed_data = transformed_data.astype({
        "Strongly agree": int,
        "Agree": int,
        "Disagree": int,
        "Strongly disagree": int,
        "positive_response": int,
        "negative_response": int,
        "total_responses": int,
        "satisfaction_rate": float
    })

    hook = PostgresHook(postgres_conn_id='postgres_target')
    conn = hook.get_conn()
    cursor = conn.cursor()

    table_name = 'fact_survey_3'
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.commit()

    create_table_sql = f"""
    CREATE TABLE {table_name} (
        cmp_id INT,
        resource TEXT,
        type_version TEXT,
        question TEXT,
        "Strongly agree" INT DEFAULT 0,
        "Agree" INT DEFAULT 0,
        "Disagree" INT DEFAULT 0,
        "Strongly disagree" INT DEFAULT 0,
        positive_response INT DEFAULT 0,
        negative_response INT DEFAULT 0,
        total_responses INT DEFAULT 0,
       
        satisfaction_rate FLOAT DEFAULT 0
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

    for _, row in transformed_data.iterrows():
        insert_sql = f"""
        INSERT INTO {table_name} (
            cmp_id, resource, type_version, question, "Strongly agree", "Agree", "Disagree", "Strongly disagree",
            positive_response, negative_response, total_responses, satisfaction_rate
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_sql, (
            int(row['cmp_id']),
            str(row['resource']),
            str(row['type_version']),
            str(row['question']),
            int(row['Strongly agree']),
            int(row['Agree']),
            int(row['Disagree']),
            int(row['Strongly disagree']),
            int(row['positive_response']),
            int(row['negative_response']),
            int(row['total_responses']),
            float(row['satisfaction_rate'])
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Données insérées dans '{table_name}' avec succès.")

# Définition du DAG
with DAG(
    dag_id='indicateur_3',
    default_args=default_args,
    description='Processus ETL pour les données d\'enquête avec campagne',
    start_date=datetime(2025, 5, 6),
    schedule_interval='@daily',
    catchup=False,
    tags=[ 'survey', 'satisfaction_rate']
) as dag:

    task1 = PythonOperator(
        task_id='extraire_donnees',
        python_callable=extraire_donnees
    )

    task2 = PythonOperator(
        task_id='transformer_donnees',
        python_callable=transformer_donnees
    )

    task3 = PythonOperator(
        task_id='charger_donnees',
        python_callable=charger_donnees
    )

    task4 = PythonOperator(
        task_id='charger_donnees_postgres',
        python_callable=charger_donnees_dans_postgres
    )

    task1 >> task2 >> task3 >> task4
