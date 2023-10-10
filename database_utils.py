import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect


class DatabaseConnector:
    # A class for connecting and uploading data to a database.
    
    def read_db_creds(self):
        # Returns the file ""db_creds.yaml"" as a python dictionary.
        with open('db_creds.yaml', 'r') as cred_file:
            cred_dict = yaml.safe_load(cred_file)
        return cred_dict
    
    def init_db_engine(self, cred_dict:dict):
        # Reads a python dictionary and initialises and returns
        # an sqlalchemy database engine.
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = cred_dict["RDS_HOST"]
        USER = cred_dict["RDS_USER"]
        PASSWORD = cred_dict["RDS_PASSWORD"]
        DATABASE = cred_dict["RDS_DATABASE"]
        PORT = cred_dict["RDS_PORT"]
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}" + \
                               f":{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    
Connector1 = DatabaseConnector()
db_creds = Connector1.read_db_creds()
engine = Connector1.init_db_engine(db_creds)


