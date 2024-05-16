"""Feature module"""
import os
import logging
import psycopg2
from airflow.models import Variable

from sqlalchemy import create_engine
class Database:
    '''Database class'''
    def __init__(self):
        try:
            pg_password = Variable.get("AZURE_PG_PASSWORD")
        except Exception: # pylint: disable=bare-except, broad-exception-caught
            pg_password = os.environ.get("AZURE_PG_PASSWORD")

        db_params = {
            "dbname": "ademe",
            "user": "alexisperrier",
            "password": pg_password,
            "host": "ademe-mlops-db.postgres.database.azure.com",
            "port": "5432",
            "sslmode": "require",
        }

        db_user = db_params["user"]
        db_password = db_params["password"]
        db_host = db_params["host"]
        db_port = db_params["port"]
        db_name = db_params["dbname"]

        self.connection = psycopg2.connect(**db_params)
        self.engine = create_engine(
            f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )

    def insert(self, insert_query):
        '''Insert method'''
        cursor = self.connection.cursor()

        cursor.execute(insert_query)
        self.connection.commit()
        cursor.close()

    def execute(self, query_):
        '''Execute method'''
        cursor = self.connection.cursor()

        cursor.execute(query_)
        self.connection.commit()
        cursor.close()

    def close(self):
        '''Close method'''
        self.connection.close()
        self.engine.dispose()

class FeatureSets:
    """FeatureSets class"""
    @staticmethod
    def get_input_columns():
        """Get input columns"""
        return FeatureSets.input_columns

    @staticmethod
    def get_categorical_mappings():
        """Get categorical mappings"""
        return {
            "target": FeatureSets.map_target,
            "type_energie": FeatureSets.map_type_energie,
            "periode_construction": FeatureSets.map_periode_construction,
            "secteur_activite": FeatureSets.map_secteur_activite,
            "type_usage_energie": FeatureSets.map_usage_energie
        }

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
        "R : Établissements d’éveil, d’enseignement, de formation,"
            + " centres de vacances, centres de loisirs sans hébergement": 9,
        "O : Hôtels et pensions de famille": 10,
        "GHZ : Usage mixte": 11,
        "X : Établissements sportifs couverts": 12,
        "L : Salles d'auditions, de conférences, de réunions," 
            + " de spectacles ou à usage multiple": 13,
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
    """ FeatureProcessor class"""

    def encode_categorical_wth_map(self, column, mapping, default_unknown=""):
        """Encode categorical with mapping"""
        valid_values = list(mapping.keys())
        # id unknown values
        self.data.loc[~self.data[column].isin(valid_values), column] = default_unknown
        # always cast missing values as -1
        mapping[default_unknown] = -1
        # encode
        self.data[column] = self.data[column].apply(lambda d: mapping[d])

    def __init__(self, data, target="etiquette_dpe"):
        self.data = data
        self.target = target

    def missing_values(self):
        """Missing values"""
        for col in FeatureSets.columns_categorical:
            self.data[col].fillna("", inplace=True)

        for col in FeatureSets.columns_num:
            self.data[col].fillna(-1, inplace=True)
            self.data.loc[self.data[col] == "", col] = -1.0

    def encode_categoricals(self):
        """Encode categoricals"""
        # version_dpe as float
        self.data["version_dpe"] = self.data["version_dpe"].astype(float)
        # map_periode_construction
        self.encode_categorical_wth_map(
            "periode_construction", FeatureSets.map_periode_construction
        )

        # secteur_activite
        self.encode_categorical_wth_map("secteur_activite", FeatureSets.map_secteur_activite)


        energie_map = FeatureSets.map_type_energie
        # type energie
        self.encode_categorical_wth_map(
            "type_energie_principale_chauffage", energie_map
        )
        self.encode_categorical_wth_map("type_energie_n_1", energie_map)
        # type_usage_energie_n_1
        self.encode_categorical_wth_map("type_usage_energie_n_1", energie_map)

        map_target = FeatureSets.map_target
        # encode targets
        for target in ["etiquette_dpe", "etiquette_ges"]:
            try:
                if target in self.data.columns():
                    self.encode_categorical_wth_map(target, map_target, default_unknown=-1)
            except Exception: # pylint: disable=bare-except, broad-exception-caught
                pass
    def encode_floats(self):
        """Encode floats"""
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
