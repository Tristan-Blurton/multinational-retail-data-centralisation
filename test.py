from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import yaml

Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
engine = Connector_RDS.init_db_engine()
Extractor = DataExtractor(engine)

pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_data = Extractor.retrieve_pdf_data(pdf_path)

Cleaner = DataCleaning()

card_data = Cleaner.clean_card_data(card_data)
print(card_data.head(100))