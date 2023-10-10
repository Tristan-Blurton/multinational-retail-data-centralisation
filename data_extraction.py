import pandas as pd
from sqlalchemy import inspect, text

class DataExtractor:
    # A utility class for extracting data from different sources.
    
    def __init__(self,engine):
        self.engine = engine
        self.insp = inspect(self.engine)

    def list_db_tables(self):
        # Lists names of all tables in the database connected to the
        # initialised engine. 
        tables = self.insp.get_table_names()
        print(tables)
    
    def read_rds_table(self, table_name):
        with self.engine.connect() as connection:
            table_data = connection.execute(text(f"SELECT * FROM {table_name}"))
        table_df = pd.DataFrame(table_data, index=False)
        return table_df



