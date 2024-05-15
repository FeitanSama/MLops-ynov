from __future__ import annotations
import os
import re
import json
import time
from datetime import datetime, timedelta
import logging
import typing as t
import requests
import pandas as pd
from db_utils import Database
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Chemin du fichier de résultats
try:
    DATA_PATH = "/opt/airflow/data/"
except:
    DATA_PATH = "/Users/guill/Desktop/MLops-ynov/data/"

DOWNLOADED_FILES_PATH = os.path.join(DATA_PATH)
URL_FILE = os.path.join(DATA_PATH, "api", "url.json")
RESULTS_FILE = os.path.join(DATA_PATH, "api", "results.json")

# Initialisation de la connexion à la base de données PostgreSQL
def init_db_connection():
    db_host = "mlops-ynov.postgres.database.azure.com"
    db_port = 5432
    db_user = "azureuser"
    db_password = "mlops-ynov@2024"
    db_name = "postgres"

    # Initialise la connexion à la base de données et retourne l'objet de connexion
    db = Database(host=db_host, port=db_port, user=db_user, password=db_password, database=db_name)
    return db

# Fonction pour renommer les colonnes
def rename_columns(columns: t.List[str]) -> t.List[str]:
    columns = [col.lower() for col in columns]

    rgxs = [
        (r"[°|/|']", "_"),
        (r"²", "2"),
        (r"[(|)]", ""),
        (r"é|è", "e"),
        (r"â", "a"),
        (r"^_", "dpe_"),
        (r"_+", "_"),
    ]
    for rgx in rgxs:
        columns = [re.sub(rgx[0], rgx[1], col) for col in columns]

    return columns

# Chargement des données dans la base de données PostgreSQL
def save_postgresdb():
    assert os.path.isfile(RESULTS_FILE)

    # Lire la sortie de l'appel API précédent
    with open(RESULTS_FILE, encoding="utf-8") as file:
        data = json.load(file)

    data = pd.DataFrame(data["results"])

    # Renommer les colonnes
    new_columns = rename_columns(data.columns)
    data.columns = new_columns
    data = data.astype(str).replace("nan", "")

    # Connecter à PostgreSQL
    db = init_db_connection()

    # Insérer les données dans la base de données
    db.insert_data(data, table_name="dpe_logement")

    # Fermer la connexion à la base de données
    db.close()

# Définition du DAG
with DAG(
    "ademe_data",
    default_args={
        "depends_on_past": False,
        "email": ["guillaume.dupuy@ynov.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    description="Get data from ADEME API, save to postgres, then clean up",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ademe"],
) as dag:

    # Tâche pour charger les données dans PostgreSQL
    save_postgresdb = PythonOperator(
        task_id="save_postgresdb",
        python_callable=save_postgresdb,
    )

    # Définir les dépendances entre les tâches
    save_postgresdb # Peut être lié à d'autres tâches selon votre workflow
