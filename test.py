from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import yaml

pd.set_option("display.max_rows",100)
pd.set_option("display.max_colwidth",100)
#Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
#engine = Connector_RDS.init_db_engine()
#Extractor = DataExtractor(engine)

Cleaner = DataCleaning()

file_path = "archive_data/orders_data.csv"
orders_data = pd.read_csv(file_path, index_col="index")


orders_data.drop(["Unnamed: 0", "level_0", "first_name", "last_name", "1"],
                 axis=1, inplace=True)
orders_data = orders_data.convert_dtypes()




print(orders_data.info())
print(orders_data.head(100))