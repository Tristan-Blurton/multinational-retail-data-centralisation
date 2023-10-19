from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import yaml
import datetime as dt
pd.set_option("display.max_rows",100)
pd.set_option("display.max_colwidth",100)
#Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
#engine = Connector_RDS.init_db_engine()
#Extractor = DataExtractor(engine)

Cleaner = DataCleaning()

card_data = pd.read_csv("archive_data/card_data.csv")
print(card_data[card_data.card_number == "60485138139"])
card_data = Cleaner.clean_card_data(card_data)
print(card_data[card_data.card_number == 60485138139])
print(card_data.info())