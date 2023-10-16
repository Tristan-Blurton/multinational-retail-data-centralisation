from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import yaml

pd.set_option("display.max_rows",451)
Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
engine = Connector_RDS.init_db_engine()
Extractor = DataExtractor(engine)

Cleaner = DataCleaning()

store_data = pd.read_csv("store_data.csv", index_col="index")

# Drop "lat" column as it contains mostly null values and can be replaced
# by "latitude".  
store_data.drop("lat", axis=1, inplace=True)
store_data.continent.replace(["eeEurope", "eeAmerica"],
                             ["Europe", "America"],
                             inplace=True)

store_data.continent = store_data.continent.astype("str")
store_data = store_data[store_data.continent.str.fullmatch("^Europe$|^America$")]
store_data = store_data.iloc[:, [0,1,7,2,3,4,5,6,8,9]]
store_data.staff_numbers = store_data.staff_numbers.astype("str")
store_data.staff_numbers = store_data.staff_numbers.str.replace("(\D)", "", 
                                                                regex=True)

store_data.address = store_data.address.astype("str")
store_data.locality = store_data.locality.astype("str")
store_data.store_code = store_data.store_code.astype("str")


store_data.latitude = store_data.latitude.astype("Float64")
store_data.longitude = store_data.longitude.astype("Float64")
store_data.longitude = store_data.longitude.round(decimals=4)
store_data.latitude = store_data.latitude.round(decimals=4)
store_data.latitude.where((store_data.latitude < 90.0) 
                            &(store_data.latitude > -90.0),
                            None,
                            inplace=True)
store_data.longitude.where((store_data.longitude < 90.0) 
                            &(store_data.longitude > -90.0),
                            None,
                            inplace=True)

store_data.opening_date = pd.to_datetime(store_data.opening_date,
                                         format="mixed")


print(store_data.staff_numbers)
