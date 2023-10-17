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

Extractor_RDS.list_db_tables()
order_data = Extractor_RDS.read_rds_table("orders_table")
order_data = Cleaner.clean_order_data(order_data)
Connector_PG4.upload_to_db(order_data, "orders_table")