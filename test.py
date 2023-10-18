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

file_path = "archive_data/events_data.json"
events_data = pd.read_json(file_path)

events_data = events_data.convert_dtypes()
digits = map(str, list(range(0,10)))
events_data = events_data.mask(events_data.isin(digits), "0" + events_data)
events_data = events_data[events_data.timestamp.str.match("^\d\d:\d\d:\d\d$")]
events_data["datetime"] = events_data.year\
                        + events_data.month\
                        + events_data.day\
                        + " " + events_data.timestamp                  

events_data.datetime = pd.to_datetime(events_data.datetime)
                                       
events_data = events_data.iloc[:, [6,4,5]]

print(events_data.time_period.value_counts())