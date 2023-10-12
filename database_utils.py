import yaml
import pandas as pd
from sqlalchemy import create_engine


class DatabaseConnector:
    # A class for connecting and uploading data to a database.

    def __init__(self, cred_dict_path):
        # Initialising attributes - assignment of 'self.cred_dict' 
        # and 'self.engine' is performed through
        # calling subsequent methods.
        self.cred_dict_path = cred_dict_path
        self.cred_dict = None
        self.engine = None

    def read_db_creds(self):
        # Returns the file ""db_creds.yaml"" as a python dictionary.
        with open(f'{self.cred_dict_path}', 'r') as cred_file:
            cred_dict = yaml.safe_load(cred_file)
        self.cred_dict = cred_dict
    
    def init_db_engine(self):
        # Reads a python dictionary and initialises and returns
        # an sqlalchemy database engine.
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.cred_dict["HOST"]
        USER = self.cred_dict["USER"]
        PASSWORD = self.cred_dict["PASSWORD"]
        DATABASE = self.cred_dict["DATABASE"]
        PORT = self.cred_dict["PORT"]
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}" + \
                               f":{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        self.engine = engine
        return engine
    
    def upload_to_db(self, df, table_name):
        df.to_sql(f"{table_name}", self.engine)