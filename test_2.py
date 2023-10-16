import pandas as pd
import numpy as np
import yaml
import requests
import json
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
engine = Connector_RDS.init_db_engine()
Extractor = DataExtractor(engine)

def retrieve_stores_data(self, store_number):
        """Retrieve tabular data from a specified store as a dataframe."""
        header_dict, url_dict = self.__open_api_info()
        for num in store_number:
            store_data = requests.get(f"{url_dict['retrieve-store']}{num}",
                                  headers=header_dict)
            store_data = store_data.append(json.loads(store_data.text))
            return(store_data)

stores_data = Extractor.retrieve_stores_data(541)
print(stores_data)