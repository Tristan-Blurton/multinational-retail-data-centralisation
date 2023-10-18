from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd

Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
Connector_PG4 = DatabaseConnector("parameters/db_creds_pg4.yaml")
Connector_RDS.read_db_creds()
Connector_PG4.read_db_creds()
engine_rds = Connector_RDS.init_db_engine()
engine_pg4 = Connector_PG4.init_db_engine()
Extractor_RDS = DataExtractor(engine_rds)
Cleaner = DataCleaning()

s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
path = "archive_data/events_data.json"
events_data = Extractor_RDS.extract_json_from_s3(s3_address, path)
events_data = Cleaner.clean_events_data(events_data)
Connector_PG4.upload_to_db(events_data, "dim_datetimes")