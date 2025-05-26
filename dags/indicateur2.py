from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import re
from collections import Counter

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# --- Étape 1 : Extraire les commentaires avec type ---
def extraire_commentaires(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    sql = """
    SELECT feed_back, evaluation_type
    FROM "surveyFeedback_surveyfeedback"
    WHERE feed_back IS NOT NULL AND feed_back != '';
    """
    df = pd.read_sql(sql, hook.get_conn())
    df['feed_back'] = df['feed_back'].str.strip()
    kwargs['ti'].xcom_push(key='commentaires', value=df)

# --- Étape 2 : Transformer et compter les mots par type ---
def compter_mots(**kwargs):
    df = kwargs['ti'].xcom_pull(key='commentaires', task_ids='extraire_commentaires')
    stopwords = set([
        'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you',
        'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself',
        'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them',
        'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this',
        'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been',
        'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing',
        'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until',
        'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between',
        'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to',
        'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again',
        'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why',
        'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other',
        'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than',
        'too', 'very', 'can', 'will', 'just', 'don', 'should', 'now'
    ])

    word_freq_by_type = []

    for eval_type in df['evaluation_type'].unique():
        subset = df[df['evaluation_type'] == eval_type]
        all_text = ' '.join(subset['feed_back'].tolist()).lower()
        words = re.findall(r'\b\w+\b', all_text)
        words = [word for word in words if word not in stopwords]
        counter = Counter(words)
        for word, count in counter.items():
            word_freq_by_type.append({'evaluation_type': eval_type, 'word': word, 'count': count})

    result_df = pd.DataFrame(word_freq_by_type)
    kwargs['ti'].xcom_push(key='word_frequencies', value=result_df)

# --- Étape 3 : Charger dans PostgreSQL ---
def charger_mots_postgres(**kwargs):
    import psycopg2
    df = kwargs['ti'].xcom_pull(key='word_frequencies', task_ids='compter_mots')
    hook = PostgresHook(postgres_conn_id='postgres_target')
    conn = hook.get_conn()
    cursor = conn.cursor()

    table_name = 'wordcloud_words'

    # 🔄 Recréation propre de la table
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    cursor.execute(f"""
    CREATE TABLE {table_name} (
        id SERIAL PRIMARY KEY,
        evaluation_type TEXT,
        word TEXT,
        count INT
    );
    """)

    for index, row in df.iterrows():
        try:
            evaluation_type = str(row['evaluation_type']) if pd.notnull(row['evaluation_type']) else 'Unknown'
            word = str(row['word']) if pd.notnull(row['word']) else ''
            count = int(row['count']) if pd.notnull(row['count']) else 0

            if word.strip():  # éviter les mots vides
                cursor.execute(f"""
                    INSERT INTO {table_name} (evaluation_type, word, count)
                    VALUES (%s, %s, %s);
                """, (evaluation_type, word, count))
        except Exception as e:
            print(f"❌ Erreur à la ligne {index}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Mots insérés avec succès dans {table_name}.")

# --- DAG ---
with DAG(
    dag_id='indicateur_2',
    default_args=default_args,
    description='Extrait, compte et charge les mots des commentaires pour Word Cloud, par type',
    start_date=datetime(2025, 5, 6),
    schedule_interval='@daily',
    catchup=False,
    tags=['survey','wordcloud']
) as dag:

    t1 = PythonOperator(
        task_id='extraire_commentaires',
        python_callable=extraire_commentaires,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='compter_mots',
        python_callable=compter_mots,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id='charger_mots_postgres',
        python_callable=charger_mots_postgres,
        provide_context=True
    )

    t1 >> t2 >> t3
