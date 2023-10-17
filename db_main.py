from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


# Initialise connector classes for the old (RDS) and new
# (PGAdmin4) databases.
Connector_RDS = DatabaseConnector("parameters/db_creds_rds.yaml")
Connector_PG4 = DatabaseConnector("parameters/db_creds_pg4.yaml")

# Read database credentials and initialise sqlalchemy engines
# for both connector classes.
Connector_RDS.read_db_creds()
Connector_PG4.read_db_creds()
engine_rds = Connector_RDS.init_db_engine()
engine_pg4 = Connector_PG4.init_db_engine()

# Initialise RDS data extractor class with previously-created
# RDS engine. List RDS data tables. Read selected RDS table
# into variable.
Extractor_RDS = DataExtractor(engine_rds)
##Extractor_RDS.list_db_tables()
## legacy_user_data = Extractor_RDS.read_rds_table("legacy_users")

# Initialise data cleaner class and call a method to clean user data.
Cleaner = DataCleaning()

## store_data = Extractor_RDS.retrieve_stores_data(451)
## Connector_PG4.upload_to_db(store_data, "dim_store_details")

s3_address = "s3://data-handling-public/products.csv"
file_path = "archive_data/product_data.csv"
product_data = Extractor_RDS.extract_from_s3(s3_address, file_path)
product_data = Cleaner.clean_product_data(product_data)

Connector_PG4.upload_to_db(product_data, "dim_products")