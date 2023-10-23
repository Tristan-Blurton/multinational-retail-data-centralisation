from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd

Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
Connector_PG4 = DatabaseConnector("parameters/db_creds_pg4.yaml")
engine_rds = Connector_RDS.init_db_engine()
engine_pg4 = Connector_PG4.init_db_engine()
Extractor_RDS = DataExtractor(engine_rds)
Cleaner = DataCleaning()

user_data = Extractor_RDS.read_rds_table("legacy_users")
user_data = Cleaner.clean_user_data(user_data)
Connector_PG4.upload_to_db(user_data, "dim_users")