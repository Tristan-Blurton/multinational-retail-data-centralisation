from sqlalchemy import inspect, text
from tabula import read_pdf
import boto3
import requests
import yaml
import json
import re
import pandas as pd


class DataExtractor:
    """Contains methods for extract data from databases.
    
    A seperate instance with seperate engine is required for 
    each different database that data must be extracted from.
    Methods that do not use the engine attribute are general
    and can be called from any instance of the class.

    Public Methods:
     - list_db_tables(engine)
     - read_rds_table()
     - retrieve_pdf_data()
     - list_number_of_stores()
     - retrieve_stores_data(stores_number)
     - extract_csv_from_s3(s3_address, file_path)
     - extract_json_from_s3(web_address, file_path)
     
    Instance Variables:
     - engine (sqlalchemy engine object): Initialised using the 
       DatabaseConnector class.

    Attributes:
     - engine  (sqlalchemy engine object): as above.
     - insp (sqlalchemy inspector object): Created during class 
       initialisation. Used to extract information about the 
       associated database.
     """
    
    def __init__(self,engine):
        """Initialise the DataExtractor Instance.
        
        Arguments:
         - engine (sqlalchemy engine object): This object is initialised
           using the DatabaseConnector class and associates the 
           DataExtractor class with a database.
        """
        self.engine = engine
        self.insp = inspect(self.engine)

    def list_db_tables(self):
        """Print the table names of all tables in the associated database."""
        tables = self.insp.get_table_names()
        print(tables)
    
    def read_rds_table(self, table_name):
        """Return a specificed table from an RDS database as a pandas DataFrame.
        
        Convenient to use in conjuction with list_db_tables.

        Arguments:
         - table_name(str): The name of a table in the associated database.

        Keyword Arguments:
         - None

        Returns:
         - data (DataFrame): A pandas DataFrame of data from 
           the table specified.
        """
        with self.engine.connect() as connection:
            table_data = connection.execute(text(f"SELECT * FROM {table_name}"))
        data = pd.DataFrame(table_data)
        return data
    
    def retrieve_pdf_data(self, pdf_address):
        """Return a pdf as a pandas DataFrame.
        
        Handles multiple pages of pdf data.
        
        Arguments:
        - pdf_address (str): The web address of the pdf data to be extracted.

        Keyword Arguments:
        - None

        Returns:
        - data (DataFrame): A pandas DataFrame of the pdf data.
        """
        pdf_data = read_pdf(pdf_address, pages="all")
        data = pd.concat(pdf_data)
        return(data) 

    def __open_api_info(self):
        """Open header and url dictionaries."""

        header_dict_path = "parameters/headers_dict.yaml"
        url_dict_path = "parameters/url_dict.yaml"
        with open(header_dict_path, "r") as file1,\
             open(url_dict_path, "r") as file2:
            header_dict = yaml.safe_load(file1)
            url_dict = yaml.safe_load(file2)
        return(header_dict, url_dict)

    def list_number_of_stores(self):
        """List the number of stores in the business.
        
        Arguments:
        - None
        
        Keyword Arguments:
        - None
        
        Returns:
        - number_stores (int): The number of stores stored at 
        the location provided.
        """
        header_dict, url_dict = self.__open_api_info()
        number_stores = requests.get(url_dict["number-stores"],
                                     headers=header_dict)
        number_stores = eval(number_stores.text)
        print(number_stores["number_stores"])
    
    def retrieve_stores_data(self, store_number):
        """Retrieve dataframe of information on stores.
        
        Variable execution time depeneding on number of stores retrieved.

        Arguments:
        - store_number (int): The number of stores to retrieve data on.
        The maximum value of this integer can be found by calling 
        "list_number_of_stores".
        
        Keyword Arguments:
        - None
        
        Returns:
        - store_data (DataFrame): a pandas dataframe of the collated 
        store data."""

        store_data = []
        header_dict, url_dict = self.__open_api_info()
        for num in range(store_number):
            loop_data = requests.get(f"{url_dict['retrieve-store']}{num}",
                                  headers=header_dict)
            loop_data = json.loads(loop_data.text)
            store_data.append(loop_data)
        
        store_data = pd.DataFrame(store_data)
        store_data.set_index("index", inplace=True)
        return(store_data)
    
    def extract_csv_from_s3(self, s3_address, file_path):
        """Retrieve csv data from s3 bucket and return as DataFrame.
        
        Must be called with an s3 address that contains csv data only.

        Arguments: 
        - s3_address (str): The s3 address of the bucket for information
        to be retrieved from.
        - file_path (str): a directory for the downloaded file to be saved
        to. 
        Keyword Arguments:
        - None
        
        Returns: 
        - data (DataFrame): A pandas dataframe of csv data. Also saves
         a csv to file. 
        """
        bucket_info = re.match("^s3:\/\/(.*)\/(.*)$", s3_address)
        name = bucket_info.group(1)
        key = bucket_info.group(2)
        s3 = boto3.client("s3")
        s3.download_file(name, key, file_path)
        data = pd.read_csv(file_path, index_col=[0])
        return(data)
    
    def extract_json_from_s3(self, web_address, file_path):
        """Retrieve json data from s3 bucket and return as DataFrame.
        
        Must be called with a web address that contains csv data only.

        Arguments: 
        - web_address (str): The web address of the s3 bucket with information
        to be retrieved from.
        - file_path (str): a directory for the downloaded file to be saved
        to. 
        Keyword Arguments:
        - None
        
        Returns: 
        - data (DataFrame): A pandas dataframe of the json data. Also saves
         a json to file. 
        """
        bucket_info = re.match("^https:\/\/([^\.]+).*\/(.*)$", web_address)
        name = bucket_info.group(1)
        key = bucket_info.group(2)
        s3 = boto3.client("s3")
        s3.download_file(name, key, file_path)
        data = pd.read_json(file_path)
        return(data)