from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np

#Connector_RDS = DatabaseConnector("db_creds_rds.yaml")
#engine = Connector_RDS.init_db_engine()
#Extractor = DataExtractor(engine)

#pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
#pdf_data = Extractor.retrieve_pdf_data(pdf_path)

card_data = pd.read_csv("card_data.csv", index_col=0)

# Remove rows where the card number has any non-digit character:
card_data = card_data[card_data.card_number.str.match("\D") == False] 

# Change column data to appropriate types:
card_data.card_number = card_data.card_number.astype("int64", errors="raise")
card_data.expiry_date = pd.to_datetime(card_data.expiry_date,
                                       format="%m/%y",
                                       errors="raise",)
card_data.card_provider = card_data.card_provider.astype("string")
card_data.date_payment_confirmed = pd.to_datetime(card_data.date_payment_confirmed,
                                                  format="mixed",
                                                  errors="raise")

#print(card_data[card_data.expiry_date < card_data.date_payment_confirmed])
#print(card_data.info())

card_data.card_number = card_data.card_number.astype("string")

test_data = card_data[card_data.card_provider.str.contains("16 digit")]

bad_numbers = test_data.card_number[test_data.card_number.str.match("\d{16}") == False]
print(bad_numbers)


