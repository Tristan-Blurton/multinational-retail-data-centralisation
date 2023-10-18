# Multinational Retail Data Centralisation
## Contents
-  **Overview**
    - Function
    -  Purpose
    - Takeaways
-  **Instructions**
    - Installation
    - Usage
- **Structure**
- **License**
## Overview
### Function
This project extracts, cleans, and collates data from multiple sources into a single PGAdmin4 relational database.
### Purpose
The project is a practice exercise in skills applicable to data handling in a commercial enviroment. Upon completion I will have a firm grip of the fundamentals of data manipulation and database management.
### Takeaways
So far I have become vastly more proficient with pandas and its myriad methods, attributes, and functions. The importance of abstraction and encapsulation in producing logical, easy-to-understand systems has become evident, and writing  code all day has made me a more efficient text editor at a basic level.
## Instructions
### Installation
Download all files into whichever directory you desire and enjoy. The directory archive_data is required for some file path definitions, but its contents are not - they are pulled from various sources as methods are called.
### Usage
The function of the project is to collate cleaned data into a single database. To get started, the following steps are essential:
1. Initialise the DatabaseConnector class once for each database to be worked with. The credentials should be in the same format as the 'db_creds_XXX' yaml files in the 'parameters' directory.

2. Initialise an engine for each DatabaseConnector instance using the '.init_db_engine()' method. There is no need to pass anything to the function. Returns an engine object.

3. Initialise an instance of the DataExtractor Class for each database to be extracted from using the corresponding engine object.

4. Initialise an instance of DataCleaning. This class contains methods that exclusively act on dataframes so only one instance is required.

5. You're set - now use the various methods as outlined below to download, extract, clean, and reupload to your heart's content.
## Structure
Currently there are three directories:
 - **"project_MRDC"** is the root directory. The three main modules ("data_cleaning", "data_extraction", and "database_utils") along with "db_main"
and a couple of development test files are stored here.

- **"archive_data"** contains database files that are downloaded as extraction methods are called.

- **"parameters"** contains various dictionaries of information used throughout, like database credentials, urls, and other miscellany.

## License
No License - it's only for practice!







The Project has three main classes, from which all methods (so far) are called: 
 - DatabaseConnector: Contains all methods related to, surprisingly, connecting to databases:
     -  __read_db_creds: private method intended only for reading a set of database credentials when the class is initialised.
     - init_db_engine: uses the credentials read on initialisation to create an sqlalchemy engine for that database.
     - upload_to_db 
 - DataExtractor: Contains methods related to retrieving data from databases. It is initialised using the engine created with the DatabaseConnector class:
     - list_db_tables: lists the names of all tables in the associated database. only tested with AWS RDS databases so far.
     - read_rds_table: 
 - DataCleaning