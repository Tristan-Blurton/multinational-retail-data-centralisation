from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import yaml
import datetime as dt
pd.set_option("display.max_rows",100)
pd.set_option("display.max_colwidth",100)
Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
engine = Connector_RDS.init_db_engine()
Extractor = DataExtractor(engine)

Cleaner = DataCleaning()

Extractor.list_db_tables()
user_data = Extractor.read_rds_table("legacy_users")
print(user_data[user_data.user_uuid == "15e92a28-e885-4844-9ead-17cb1a56fa6c"])
user_data = Cleaner.clean_user_data(user_data)
print(user_data[user_data.user_uuid == "15e92a28-e885-4844-9ead-17cb1a56fa6c"])
print(user_data.info())