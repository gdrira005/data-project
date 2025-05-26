from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os
import time

# Paramètres de configuration par défaut
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Extraction des données depuis PostgreSQL
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
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    
    columns = ['id', 'response', 'code', 'question']
    data = pd.DataFrame(results, columns=columns)
    
    kwargs['ti'].xcom_push(key='extracted_data', value=data)

# Transformation des données
def transformer_donnees(**kwargs):
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extraire_donnees')
    
    relevant_responses = ['Agree', 'Disagree', 'Strongly agree', 'Strongly disagree', 
                          'True sometimes', 'Somewhat', 'Yes']
    
    # Filtrer les réponses pertinentes
    filtered_data = extracted_data[extracted_data['response'].isin(relevant_responses)].copy()
    
    # Nettoyer la colonne 'question' : enlever espaces en début/fin, mettre en minuscules
    filtered_data['clean_question'] = filtered_data['question'].str.strip().str.lower()
    
    # Grouper par question nettoyée et réponse et compter
    count_data = filtered_data.groupby(['clean_question', 'response']).size().unstack(fill_value=0)
    
    # Somme des réponses pour chaque question nettoyée
    count_data['total_responses'] = count_data.sum(axis=1)
    
    # Remettre la colonne 'question' d'origine la plus fréquente par groupe pour garder un texte propre
    # Ici on récupère la question la plus fréquente (originale) par clean_question
    question_map = (filtered_data.groupby('clean_question')['question']
                    .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]))
    
    count_data = count_data.reset_index()
    count_data = count_data.merge(question_map, left_on='clean_question', right_index=True)
    
    # Remplacer 'clean_question' par 'question' originale
    count_data = count_data.drop(columns=['clean_question'])
    count_data = count_data.rename(columns={'question': 'question'})
    
    kwargs['ti'].xcom_push(key='transformed_data', value=count_data)

# Charger les données dans un fichier CSV
def charger_donnees(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transformer_donnees')
    
    dags_folder = '/usr/local/airflow/dags'
    output_dir = os.path.join(dags_folder, 'data')
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    output_file_path = os.path.join(output_dir, f'transformed_data_{timestamp}.csv')

    transformed_data.to_csv(output_file_path, index=False)
    
    print(f"✅ Les données transformées ont été sauvegardées dans : {output_file_path}")

# Nouvelle fonction pour insérer les données dans PostgreSQL
def charger_donnees_dans_postgres(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transformer_donnees')
    
    hook = PostgresHook(postgres_conn_id='postgres_target')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    table_name = 'fact_survey'
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        question TEXT PRIMARY KEY,
        "Agree" INT DEFAULT 0,
        "Disagree" INT DEFAULT 0,
        "Strongly agree" INT DEFAULT 0,
        "Strongly disagree" INT DEFAULT 0,
        "True sometimes" INT DEFAULT 0,
        "Somewhat" INT DEFAULT 0,
        "Yes" INT DEFAULT 0,
        total_responses INT DEFAULT 0
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    
    expected_cols = ["question", "Agree", "Disagree", "Strongly agree", "Strongly disagree",
                     "True sometimes", "Somewhat", "Yes", "total_responses"]
    
    for col in expected_cols:
        if col not in transformed_data.columns:
            transformed_data[col] = 0
    
    for _, row in transformed_data.iterrows():
        insert_sql = f"""
        INSERT INTO {table_name} (question, "Agree", "Disagree", "Strongly agree", "Strongly disagree",
            "True sometimes", "Somewhat", "Yes", total_responses)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (question) DO UPDATE SET
            "Agree" = EXCLUDED."Agree",
            "Disagree" = EXCLUDED."Disagree",
            "Strongly agree" = EXCLUDED."Strongly agree",
            "Strongly disagree" = EXCLUDED."Strongly disagree",
            "True sometimes" = EXCLUDED."True sometimes",
            "Somewhat" = EXCLUDED."Somewhat",
            "Yes" = EXCLUDED."Yes",
            total_responses = EXCLUDED.total_responses;
        """
        cursor.execute(insert_sql, (
            row['question'], row['Agree'], row['Disagree'], row['Strongly agree'], row['Strongly disagree'],
            row['True sometimes'], row['Somewhat'], row['Yes'], row['total_responses']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Les données transformées ont été insérées dans la table '{table_name}' de PostgreSQL.")

# Définir le DAG
with DAG(
    dag_id='ETL_process_donnees_survey',
    default_args=default_args,
    description='Processus ETL pour les données d\'enquête',
    start_date=datetime(2025, 5, 6),
    schedule_interval='@daily',
    catchup=False,
    tags=['ETL', 'survey']
) as dag:

    task1 = PythonOperator(
        task_id='extraire_donnees',
        python_callable=extraire_donnees,
        provide_context=True
    )

    task2 = PythonOperator(
        task_id='transformer_donnees',
        python_callable=transformer_donnees,
        provide_context=True
    )

    task3 = PythonOperator(
        task_id='charger_donnees',
        python_callable=charger_donnees,
        provide_context=True
    )
    
    task4 = PythonOperator(
        task_id='charger_donnees_postgres',
        python_callable=charger_donnees_dans_postgres,
        provide_context=True
    )

    task1 >> task2 >> task3 >> task4
