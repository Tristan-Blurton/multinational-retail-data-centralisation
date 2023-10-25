from sqlalchemy import create_engine
import yaml

class DatabaseConnector:
    """Contains utility methods for connecting to databases.
    
    Public methods:
     - upload_to_db()

     Instance variables:
     - 'cred_dict_path' (str): the relative or absolute path 
       to a dictionary of credentials to a database. Dictionary must
       be in the format laid out in the file parameters/db_creds_xxx.yaml 
       and of type yaml.
    """

    def __init__(self, cred_dict_path):
        """Class constructor.
        
        Uses private method '__read_db_creds()' to define attribute 
        'cred_dict'. Subsequently uses this attribute to initialise an 
        sqlalchemy engine linking it to the database specified in the 
        credentials file.
        
        Attributes:
         - self.cred_dict_path (str): should be passed at initialisation. 
           See class docstring.
         - self.cred_dict (dict): python dictionary of database credentials.
         - self.engine (engine): sqlalchemy engine assigned when method 
           'init_db_engine' is called.
        """
        self.cred_dict_path = cred_dict_path
        self.cred_dict = self.__read_db_creds()
        self.engine = self.__init_db_engine()

    def __read_db_creds(self):
        """Returns the file ""db_creds.yaml"" as a python dictionary."""
        with open(f'{self.cred_dict_path}', 'r') as cred_file:
            cred_dict = yaml.safe_load(cred_file)
        return cred_dict
        
    def __init_db_engine(self):
        """Initialise and return an sqlalchemy 'engine' object.
        
        Arguments: 
         - None.

        Keyword Arguments:
         - None.
        
        Returns:
         - sqlalchemy 'engine' object. 'engine' object is associated with the 
           database whose credentials were used to initialise the class and 
           becomes an attribute of the class.
        """
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.cred_dict["HOST"]
        USER = self.cred_dict["USER"]
        PASSWORD = self.cred_dict["PASSWORD"]
        DATABASE = self.cred_dict["DATABASE"]
        PORT = self.cred_dict["PORT"]
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}" + \
                               f":{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        self.engine = engine
        return engine
    
    def upload_to_db(self, df, table_name):
        """Upload a DataFrame to the class-associated database.
        
        Arguments:
         - df (DataFrame): The dataframe to be uploaded.
         - table_name (str): The name of the table as it 
           should appear in the new database.
        
        Keyword Arguments:
         - None.

        Returns:
         - None.
         """
        df.to_sql(f"{table_name}", self.engine)