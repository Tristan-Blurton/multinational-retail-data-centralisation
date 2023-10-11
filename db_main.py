from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from sqlalchemy import text
import yaml
import pandas as pd


Connector1 = DatabaseConnector()
Cleaner1 = DataCleaning()

Connector1.read_db_creds()
engine = Connector1.init_db_engine()

Extractor1 = DataExtractor(engine)

Extractor1.list_db_tables()

#legacy_user_data = Extractor1.read_rds_table("legacy_users")
table_name = "legacy_users"

legacy_user_data = Extractor1.read_rds_table("legacy_users")

legacy_user_data = Cleaner1.clean_user_data(legacy_user_data)

print(legacy_user_data)

Connector1.upload_to_db(legacy_user_data,"dim_users")