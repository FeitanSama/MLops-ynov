"""
Training the model
"""
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from sklearn.metrics import precision_score, recall_score
from sklearn.model_selection import train_test_split, GridSearchCV, KFold
from sklearn.ensemble import RandomForestClassifier
import mlflow
import logging
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from sqlalchemy import create_engine
import os
from airflow.models import Variable
import random


class Database:
    """Database class"""
    def __init__(self):
        """Init Context"""
        try:
            pg_password = Variable.get("AZURE_PG_PASSWORD")
        except:
            pg_password = os.environ.get("AZURE_PG_PASSWORD")

        db_params = {
            "dbname": "ademe",
            "user": "alexisperrier",
            "password": pg_password,
            "host": "ademe-mlops-db.postgres.database.azure.com",
            "port": "5432",
            "sslmode": "require",
        }

        self.connection = psycopg2.connect(**db_params)
        self.engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )

    def insert(self, insert_query):
        """Insert rows into the database"""
        cursor = self.connection.cursor()

        cursor.execute(insert_query)
        self.connection.commit()
        cursor.close()

    def execute(self, query_):
        """Execute command"""
        cursor = self.connection.cursor()

        cursor.execute(query_)
        self.connection.commit()
        cursor.close()

    def close(self):
        """Close connexion"""
        self.connection.close()
        self.engine.dispose()

class FeatureSets:
    """Feature set class"""
    input_columns = [
        # -- id
        "n_dpe",
        # -- targets
        "etiquette_dpe",
        "etiquette_ges",
        # -- date
        # "date_visite_diagnostiqueur",
        # -- categorical
        "version_dpe",
        "periode_construction",
        "secteur_activite",
        "type_energie_principale_chauffage",
        "type_energie_n_1",
        "type_usage_energie_n_1",
        # "methode_du_dpe",
        # "categorie_erp",
        # "type_energie_n_2",
        # "type_energie_n_3",
        # "type_usage_energie_n_2",
        # "type_usage_energie_n_3",
        # -- float
        "surface_utile",
        "conso_kwhep_m2_an",
        "conso_e_finale_energie_n_1",
        # "emission_ges_kgco2_m2_an",
        # "surface_shon"
        # "conso_e_primaire_energie_n_1",
        # "frais_annuel_energie_n_1",
        # "conso_e_finale_energie_n_2",
        # "conso_e_primaire_energie_n_2",
        # "frais_annuel_energie_n_2",
        # "conso_e_finale_energie_n_3",
        # "conso_e_primaire_energie_n_3",
        # "frais_annuel_energie_n_3",
        # -- int
        # "annee_construction",
        # "nombre_occupant",
        # "annee_releve_conso_energie_n_1",
        # "annee_releve_conso_energie_n_2",
        # "annee_releve_conso_energie_n_3",
    ]

    columns_categorical = [
        # "version_dpe",
        "periode_construction",
        "secteur_activite",
        "type_energie_principale_chauffage",
        "type_energie_n_1",
        "type_usage_energie_n_1",
    ]

    columns_num = [
        "surface_utile",
        "conso_kwhep_m2_an",
        "conso_e_finale_energie_n_1",
        # "date_visite_diagnostiqueur",
        "version_dpe",
    ]

    # categorical mappings
    map_target = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7}

    map_type_energie = {
        "non renseigné": "non renseigné",
        "Électricité": "Électricité",
        "Électricité d'origine renouvelable utilisée dans le bâtiment": "Électricité",
        "Gaz naturel": "Gaz naturel",
        "Butane": "GPL",
        "Propane": "GPL",
        "GPL": "GPL",
        "Fioul domestique": "Fioul domestique",
        "Réseau de Chauffage urbain": "Réseau de Chauffage urbain",
        "Charbon": "Combustible fossile",
        "autre combustible fossile": "Combustible fossile",
        "Bois – Bûches": "Bois",
        "Bois – Plaquettes forestières": "Bois",
        "Bois – Granulés (pellets) ou briquettes": "Bois",
        "Bois – Plaquettes d’industrie": "Bois",
    }
    map_periode_construction = {
        "avant 1948": 0,
        "1948-1974": 1,
        "1975-1977": 2,
        "1978-1982": 3,
        "1983-1988": 4,
        "1989-2000": 5,
        "2001-2005": 6,
        "2006-2012": 7,
        "2013-2021": 8,
        "après 2021": 9,
    }

    map_secteur_activite = {
        "autres tertiaires non ERP": 1,
        "M : Magasins de vente, centres commerciaux": 2,
        "W : Administrations, banques, bureaux": 3,
        "locaux d'entreprise (bureaux)": 4,
        "J : Structures d’accueil pour personnes âgées ou personnes handicapées": 5,
        "N : Restaurants et débits de boisson": 6,
        "U : Établissements de soins": 7,
        "GHW : Bureaux": 8,
        "R : Établissements d’éveil, d’enseignement, de formation, centres de vacances, centres de loisirs sans hébergement": 9,
        "O : Hôtels et pensions de famille": 10,
        "GHZ : Usage mixte": 11,
        "X : Établissements sportifs couverts": 12,
        "L : Salles d'auditions, de conférences, de réunions, de spectacles ou à usage multiple": 13,
        "T : Salles d'exposition à vocation commerciale": 14,
        "P : Salles de danse et salles de jeux": 15,
        "GHR : Enseignement": 16,
        "V : Établissements de divers cultes": 17,
        "S : Bibliothèques, centres de documentation": 18,
        "OA : Hôtels-restaurants d'Altitude": 19,
        "GHU : Usage sanitaire": 20,
        "PA : Établissements de Plein Air": 21,
        "GHA : Habitation": 22,
        "GHO : Hôtel": 23,
        "Y : Musées": 24,
        "PS : Parcs de Stationnement couverts": 25,
        "GHTC : tour de contrôle": 26,
        "REF : REFuges de montagne": 27,
        "GA : Gares Accessibles au public (chemins de fer, téléphériques, remonte-pentes...)": 28,
        "CTS : Chapiteaux, Tentes et Structures toile": 29,
        "GHS : Dépôt d'archives": 30,
    }

    map_type_energie = {
        "non renseigné": -1,
        "Électricité": 1,
        "Électricité d'origine renouvelable utilisée dans le bâtiment": 1,
        "Gaz naturel": 2,
        "Butane": 3,
        "Propane": 3,
        "GPL": 3,
        "Fioul domestique": 4,
        "Réseau de Chauffage urbain": 5,
        "Charbon": 6,
        "autre combustible fossile": 6,
        "Bois – Bûches": 7,
        "Bois – Plaquettes forestières": 7,
        "Bois – Granulés (pellets) ou briquettes": 7,
        "Bois – Plaquettes d’industrie": 7,
    }

    map_usage_energie = {
        "non renseigné": -1,
        "périmètre de l'usage inconnu": -1,
        "Chauffage": 1,
        "Eau Chaude sanitaire": 2,
        "Eclairage": 3,
        "Refroidissement": 4,
        "Ascenseur(s)": 5,
        "auxiliaires et ventilation": 6,
        "Autres usages": 7,
        "Bureautique": 8,
        "Abonnements": 9,
        "Production d'électricité à demeure": 10,
    }
    payload_columns = [
        "etiquette_dpe",
        "etiquette_ges",
        "version_dpe",
        "periode_construction",
        "secteur_activite",
        "type_energie_principale_chauffage",
        "type_energie_n_1",
        "type_usage_energie_n_1",
        "surface_utile",
        "conso_kwhep_m2_an",
        "conso_e_finale_energie_n_1",
    ]

    train_columns = [
        "version_dpe",
        "periode_construction",
        "secteur_activite",
        "type_energie_principale_chauffage",
        "type_energie_n_1",
        "type_usage_energie_n_1",
        "surface_utile",
        "conso_kwhep_m2_an",
        "conso_e_finale_energie_n_1",
    ]


class FeatureProcessor:
    """ """

    def encode_categorical_wth_map(self, column, mapping, default_unknown=""):
        """Encode categorical with map"""
        valid_values = list(mapping.keys())
        # id unknown values
        self.data.loc[~self.data[column].isin(valid_values), column] = default_unknown
        # always cast missing values as -1
        mapping[default_unknown] = -1
        # encode
        self.data[column] = self.data[column].apply(lambda d: mapping[d])

    def __init__(self, data, target="etiquette_dpe"):
        """Init Context"""
        self.data = data
        self.target = target

    def missing_values(self):
        """treat missing values"""
        for col in FeatureSets.columns_categorical:
            self.data[col].fillna("", inplace=True)

        for col in FeatureSets.columns_num:
            self.data[col].fillna(-1, inplace=True)
            self.data.loc[self.data[col] == "", col] = -1.0

    def encode_categoricals(self):
        """encode categorical"""
        # version_dpe as float
        self.data["version_dpe"] = self.data["version_dpe"].astype(float)
        # map_periode_construction
        self.encode_categorical_wth_map(
            "periode_construction", FeatureSets.map_periode_construction
        )

        # secteur_activite
        self.encode_categorical_wth_map("secteur_activite", FeatureSets.map_secteur_activite)

        # type energie
        self.encode_categorical_wth_map(
            "type_energie_principale_chauffage", FeatureSets.map_type_energie
        )
        self.encode_categorical_wth_map("type_energie_n_1", FeatureSets.map_type_energie)
        # type_usage_energie_n_1
        self.encode_categorical_wth_map("type_usage_energie_n_1", FeatureSets.map_usage_energie)

        # encode targets
        for target in ["etiquette_dpe", "etiquette_ges"]:
            try:
                if target in self.data.columns():
                    self.encode_categorical_wth_map(target, FeatureSets.map_target, default_unknown=-1)
            except:
                pass
    def encode_floats(self):
        """Encode floating"""
        self.data[FeatureSets.columns_num] = (
            self.data[FeatureSets.columns_num].astype(float).astype(int)
        )

    def process(self):
        """Process"""
        self.missing_values()
        self.encode_categoricals()
        self.encode_floats()
        return self.data

logger = logging.getLogger(__name__)

class NotEnoughSamples(ValueError):
    """NES"""
    pass


# --------------------------------------------------
# TrainDPE class
# --------------------------------------------------


class TrainDPE:
    """TrainDPE Class"""
    param_grid = {
        "n_estimators": sorted([random.randint(1, 20) * 10 for _ in range(2)]),
        "max_depth": [random.randint(3, 10)],
        "min_samples_leaf": [random.randint(2, 5)],
    }
    n_splits = 3
    test_size = 0.3
    minimum_training_samples = 500

    def __init__(self, data, target="etiquette_dpe"):
        """Construct"""
        # drop samples with no target
        data = data[data[target] >= 0].copy()
        data.reset_index(inplace=True, drop=True)
        if data.shape[0] < TrainDPE.minimum_training_samples:
            raise NotEnoughSamples(
                "data has {data.shape[0]} samples, which is not enough to train a model. min required {TrainDPE.minimum_training_samples}"
            )

        self.data = data
        print(f"training on {data.shape[0]} samples")

        self.model = RandomForestClassifier()
        self.target = target
        self.params = {}
        self.train_score = 0.0

        self.precision_score = 0.0
        self.recall_score = 0.0
        self.probabilities = [0.0, 0.0]

    def main(self):
        """Train a model"""
        # shuffle

        X = self.data[FeatureSets.train_columns].copy()  # Features
        y = self.data[self.target].copy()  # Target variable

        # Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=TrainDPE.test_size, random_state=808
        )

        # Setup GridSearchCV with k-fold cross-validation
        cv = KFold(n_splits=TrainDPE.n_splits, random_state=42, shuffle=True)

        grid_search = GridSearchCV(
            estimator=self.model, param_grid=TrainDPE.param_grid, cv=cv, scoring="accuracy"
        )

        # Fit the model
        grid_search.fit(X_train, y_train)

        self.model = grid_search.best_estimator_
        self.params = grid_search.best_params_
        self.train_score = grid_search.best_score_

        yhat = grid_search.predict(X_test)
        self.precision_score = precision_score(y_test, yhat, average="weighted")
        self.recall_score = recall_score(y_test, yhat, average="weighted")
        self.probabilities = np.max(grid_search.predict_proba(X_test), axis=1)

    def report(self):
        """Report the model"""
        # Best parameters and best score
        print("--" * 20, "Best model")
        print(f"\tparameters: {self.params}")
        print(f"\tcross-validation score: {self.train_score}")
        print(f"\tmodel: {self.model}")
        print("--" * 20, "performance")
        print(f"\tprecision_score: {np.round(self.precision_score, 2)}")
        print(f"\trecall_score: {np.round(self.recall_score, 2)}")
        print(f"\tmedian(probabilities): {np.round(np.median(self.probabilities), 2)}")
        print(f"\tstd(probabilities): {np.round(np.std(self.probabilities), 2)}")


# --------------------------------------------------
# set up MLflow
# --------------------------------------------------
from mlflow import MlflowClient

experiment_name = "dpe_tertiaire"

# mlflow.set_tracking_uri("http://host.docker.internal:5001")
# mlflow.set_tracking_uri("http://localhost:9090")

mlflow.set_tracking_uri("http://mlflow:5000")


print("--" * 40)
print("mlflow set experiment")
print("--" * 40)
mlflow.set_experiment(experiment_name)

mlflow.sklearn.autolog()

# --------------------------------------------------
# load data
# --------------------------------------------------


def load_data_for_inference(n_samples):
    """Load data for inference"""
    db = Database()
    query = f"select * from dpe_training order by created_at desc limit {n_samples}"
    df = pd.read_sql(query, con=db.engine)
    db.close()
    # dump payload into new dataframe
    df["payload"] = df["payload"].apply(lambda d: json.loads(d))
    data = pd.DataFrame(list(df.payload.values))

    data.drop(columns="n_dpe", inplace=True)
    data = data.astype(int)
    data = data[data.etiquette_dpe > 0].copy()
    data.reset_index(inplace=True, drop=True)
    print(data.shape)
    y = data["etiquette_dpe"]
    X = data[FeatureSets.train_columns]

    return X, y


def load_data_for_training(n_samples):
    """Load data for training"""
    # simply load payload not all columns
    db = Database()
    query = f"select * from dpe_training order by random() limit {n_samples}"
    df = pd.read_sql(query, con=db.engine)
    db.close()
    # dump payload into new dataframe
    df["payload"] = df["payload"].apply(lambda d: json.loads(d))
    data = pd.DataFrame(list(df.payload.values))
    data.drop(columns="n_dpe", inplace=True)
    data = data.astype(int)
    data = data[data.etiquette_dpe > 0].copy()
    data.reset_index(inplace=True, drop=True)
    return data


# ---------------------------------------------
#  tasks
# ---------------------------------------------
challenger_model_name = "dpe_challenger"
champion_model_name = "dpe_champion"
client = MlflowClient()


def train_model():
    """Train"""
    data = load_data_for_training(n_samples=2000)
    with mlflow.start_run() as run:
        train = TrainDPE(data)
        train.main()
        train.report()

        try:
            model = client.get_registered_model(challenger_model_name)
        except:
            print("model does not exist")
            print("registering new model", challenger_model_name)
            client.create_registered_model(
                challenger_model_name, description="sklearn random forest for dpe_tertiaire"
            )

        # set version and stage
        run_id = run.info.run_id
        model_uri = f"runs:/{run_id}/model"
        model_version = client.create_model_version(
            name=challenger_model_name, source=model_uri, run_id=run_id
        )

        client.transition_model_version_stage(
            name=challenger_model_name, version=model_version.version, stage="Staging"
        )


def create_champion():
    """
    if there is not champion yet, creates a champion from current challenger
    """
    results = client.search_registered_models(filter_string=f"name='{champion_model_name}'")
    # if not exists: promote current model
    if len(results) == 0:
        print("champion model not found, promoting challenger to champion")

        champion_model = client.copy_model_version(
            src_model_uri=f"models:/{challenger_model_name}/Staging",
            dst_name=champion_model_name,
        )
        client.transition_model_version_stage(
            name=champion_model_name, version=champion_model.version, stage="Staging"
        )

        # reload champion and print info
        results = client.search_registered_models(filter_string=f"name='{champion_model_name}'")
        print(results[0].latest_versions)


def promote_model():
    """Promote model"""
    X, y = load_data_for_inference(1000)
    # inference challenger and champion
    # load model & inference
    chl = mlflow.sklearn.load_model(f"models:/{challenger_model_name}/Staging")
    yhat = chl.best_estimator_.predict(X)
    challenger_precision = precision_score(y, yhat, average="weighted")
    challenger_recall = recall_score(y, yhat, average="weighted")
    print(f"\t challenger_precision: {np.round(challenger_precision, 2)}")
    print(f"\t challenger_recall: {np.round(challenger_recall, 2)}")

    # inference on production model
    champ = mlflow.sklearn.load_model(f"models:/{champion_model_name}/Staging")
    yhat = champ.best_estimator_.predict(X)
    champion_precision = precision_score(y, yhat, average="weighted")
    champion_recall = recall_score(y, yhat, average="weighted")
    print(f"\t champion_precision: {np.round(champion_precision, 2)}")
    print(f"\t champion_recall: {np.round(champion_recall, 2)}")

    # if performance 5% above current champion: promote
    if challenger_precision > champion_precision:
        print(f"{challenger_precision} > {champion_precision}")
        print("Promoting new model to champion ")
        champion_model = client.copy_model_version(
            src_model_uri=f"models:/{challenger_model_name}/Staging",
            dst_name=champion_model_name,
        )

        client.transition_model_version_stage(
            name=champion_model_name, version=champion_model.version, stage="Staging"
        )
    else:
        print(f"{challenger_precision} < {champion_precision}")
        print("champion remains undefeated ")


# ---------------------------------------------
#  DAG
# ---------------------------------------------
with DAG(
    "ademe_models",
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=10),
    },
    description="Model training and promotion",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ademe"],
) as dag:
    train_model_task = PythonOperator(task_id="train_model_task", python_callable=train_model)

    create_champion_task = PythonOperator(
        task_id="create_champion_task", python_callable=create_champion
    )

    promote_model_task = PythonOperator(task_id="promote_model_task", python_callable=promote_model)

    train_model_task >> create_champion_task >> promote_model_task