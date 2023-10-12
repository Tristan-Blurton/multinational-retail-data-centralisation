import pandas as pd
from sqlalchemy import inspect, text
from tabula import read_pdf


class DataExtractor:
    """Extract data from a database."""
    
    def __init__(self,engine):
        """Initialise the DataExtractor Instance."""
        self.engine = engine
        self.insp = inspect(self.engine)

    def list_db_tables(self):
        """Print the table names of all tables in the associated database."""
        tables = self.insp.get_table_names()
        print(tables)
    
    def read_rds_table(self, table_name):
        """Return a specificed table as a pandas DataFrame."""
        with self.engine.connect() as connection:
            table_data = connection.execute(text(f"SELECT * FROM {table_name}"))
        table_df = pd.DataFrame(table_data)
        return table_df
    
    def retrieve_pdf_data(self, pdf_path):
        """Return a pdf as a pandas DataFrame."""
        pdf_data = read_pdf(pdf_path,
                            pages="all")
        data = pd.concat(pdf_data)
        return data



