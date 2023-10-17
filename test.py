from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import yaml

pd.set_option("display.max_rows",1853)
pd.set_option("display.max_colwidth",100)
#Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
#engine = Connector_RDS.init_db_engine()
#Extractor = DataExtractor(engine)

Cleaner = DataCleaning()

#product_data = Extractor.extract_from_s3("s3://data-handling-public/products.csv", "product_data.csv")

file_path = "product_data.csv"
product_data = pd.read_csv(file_path, index_col=[0])
product_data = Cleaner.convert_product_weights(product_data)



# Drop rows where weight is zero - all contain only corrupt or missing data:
product_data = product_data[product_data.weight != 0]
# Drop rows with duplicate names:
product_data.drop_duplicates(subset="product_name", inplace=True)
# Convert object dtypes to string:
product_data = product_data.convert_dtypes()
# Convert date_added to datetime64 format:
product_data.date_added = pd.to_datetime(product_data.date_added, format="mixed")