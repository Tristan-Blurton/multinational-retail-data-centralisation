from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import yaml
import pandas as pd


Connector1 = DatabaseConnector()

cred_dict = Connector1.read_db_creds()
engine = Connector1.init_db_engine(cred_dict)

Extractor1 = DataExtractor(engine)

legacy_user_data = Extractor1.read_rds_table("legacy_users")

legacy_user_data.to_csv("legacy_user_data.csv", index=False)
