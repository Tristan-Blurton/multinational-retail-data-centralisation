from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd

Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
Connector_PG4 = DatabaseConnector("parameters/db_creds_pg4.yaml")
Extractor_RDS = DataExtractor(Connector_RDS.engine)
Cleaner = DataCleaning()