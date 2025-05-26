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
        where question like'The orientation program for new students' 
or question like 'Academic Advising'
or question like 'Registration Process' 
or question like 'Grade Appeal and Academic and Non-academic Concerns Process' 
or question like 'Citrine Ambassador Program (EAP)' --Effat
or question like 'Peer Tutoring Services'
or question like 'Enhancement Program (ESP) Services' 
or question like 'Center of Communication and Rhetoric (CCR)'
or question like 'Center of Excellence in Writing and Speaking (CEWS)'
or question like 'Academic and Career Counseling Services ' 
or question like 'Personal Counseling Services' 

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
    
    relevant_responses = ['Agree', 'Disagree', 'Strongly agree', 'Strongly disagree', 'True sometimes']
    
    filtered_data = extracted_data[extracted_data['response'].isin(relevant_responses)].copy()
    filtered_data['clean_question'] = filtered_data['question'].str.strip().str.lower()

    count_data = filtered_data.groupby(['clean_question', 'response']).size().unstack(fill_value=0)

    # Ajouter les colonnes de regroupement
    count_data['positive_response'] = count_data.get('Strongly agree', 0) + count_data.get('Agree', 0)
    count_data['negative_response'] = count_data.get('Strongly disagree', 0) + count_data.get('Disagree', 0)
    count_data['neutral_response'] = count_data.get('True sometimes', 0)
    count_data['total_responses'] = count_data.sum(axis=1)

    # Réassocier avec la question d’origine
    question_map = (filtered_data.groupby('clean_question')['question']
                    .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]))
    
    count_data = count_data.reset_index()
    count_data = count_data.merge(question_map, left_on='clean_question', right_index=True)
    count_data = count_data.drop(columns=['clean_question'])

    # Garder seulement les colonnes utiles
    final_columns = ['question', 'Strongly agree', 'Agree', 'Disagree', 'Strongly disagree', 'True sometimes',
                     'positive_response', 'negative_response', 'neutral_response', 'total_responses']
    for col in final_columns:
        if col not in count_data.columns:
            count_data[col] = 0

    count_data = count_data[final_columns]
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

    # Conversion des colonnes vers des types natifs Python
    transformed_data = transformed_data.astype({
        "Strongly agree": int,
        "Agree": int,
        "Disagree": int,
        "Strongly disagree": int,
        "True sometimes": int,
        "positive_response": int,
        "negative_response": int,
        "neutral_response": int,
        "total_responses": int
    })

    # Calcul global des totaux
    total_positive = int(transformed_data['positive_response'].sum())
    total_negative = int(transformed_data['negative_response'].sum())

    hook = PostgresHook(postgres_conn_id='postgres_target')
    conn = hook.get_conn()
    cursor = conn.cursor()

    table_name = 'fact_survey_condition'

    # Supprimer la table existante
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.commit()

    # Recréer la table avec les colonnes nécessaires
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        question TEXT PRIMARY KEY,
        "Strongly agree" INT DEFAULT 0,
        "Agree" INT DEFAULT 0,
        "Disagree" INT DEFAULT 0,
        "Strongly disagree" INT DEFAULT 0,
        "True sometimes" INT DEFAULT 0,
        positive_response INT DEFAULT 0,
        negative_response INT DEFAULT 0,
        neutral_response INT DEFAULT 0,
        total_responses INT DEFAULT 0,
        total_positive INT DEFAULT 0,
        total_negative INT DEFAULT 0
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

    # Insérer les données ligne par ligne
    for _, row in transformed_data.iterrows():
        insert_sql = f"""
        INSERT INTO {table_name} (
            question, "Strongly agree", "Agree", "Disagree", "Strongly disagree",
            "True sometimes", positive_response, negative_response, neutral_response,
            total_responses, total_positive, total_negative
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_sql, (
            str(row['question']),
            int(row['Strongly agree']),
            int(row['Agree']),
            int(row['Disagree']),
            int(row['Strongly disagree']),
            int(row['True sometimes']),
            int(row['positive_response']),
            int(row['negative_response']),
            int(row['neutral_response']),
            int(row['total_responses']),
            total_positive,
            total_negative
        ))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Données insérées dans '{table_name}' avec succès.")


# Définir le DAG
with DAG(
    dag_id='indicateur_1',
    default_args=default_args,
    description='Processus ETL pour les données d\'enquête',
    start_date=datetime(2025, 5, 6),
    schedule_interval='@daily',
    catchup=False,
    tags=[ 'survey', 'pie_chart']
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
