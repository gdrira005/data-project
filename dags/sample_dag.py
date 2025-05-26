
# Importation des modules nécessaires
from datetime import datetime, timedelta  # Pour gérer les dates et délais
from airflow import DAG                   # Pour créer un DAG Airflow
from airflow.operators.python import PythonOperator  # Pour exécuter une fonction Python

# Définition de la fonction qui sera exécutée par la tâche
def say_hello():
    print("Hello, world!")  # Ceci s'affichera dans les logs Airflow

# Arguments par défaut pour toutes les tâches du DAG
default_args = {
    'owner': 'airflow',  # Propriétaire du DAG (utilisé à titre informatif)
    'retries': 1,  # Nombre de fois où la tâche sera retentée si elle échoue
    'retry_delay': timedelta(seconds=30)  # Délai entre chaque tentative (ici 30 secondes)
}

# Définition du DAG (workflow)
with DAG(
    dag_id='hello_world_every_minute',  # Identifiant unique du DAG
    default_args=default_args,          # Utilise les arguments définis ci-dessus
    description='Un DAG qui affiche Hello World chaque minute',
    start_date=datetime(2025, 4, 21),   # Date de début du DAG (attention : pas dans le futur)
    schedule_interval='* * * * *',      # Planification : chaque minute
    #schedule_interval=None,            # Aucune planification automatique

   #  Minute   Heure  JourMois  Mois  JourSemaine
   #    *        *       *       *        *

    catchup=False,                      # Ne pas rattraper les exécutions manquées
    tags=['exemple']                    # Tag pour organisation dans l'interface Airflow
) as dag:

    # Définition de la tâche principale
    hello_task = PythonOperator(
        task_id='say_hello',        # Identifiant de la tâche
        python_callable=say_hello   # Fonction à exécuter
    )

    # Ici, on exécute la tâche (pas de dépendances car il n’y a qu’une tâche)
    hello_task
