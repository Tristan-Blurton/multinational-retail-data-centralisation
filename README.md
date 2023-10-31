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
This project extracts, cleans, and collates data from multiple sources into a single Postgres relational database.
### Purpose
The project is a practice exercise in skills applicable to data handling in a commercial enviroment. Upon completion I will have a firm grip of the fundamentals of data manipulation and database management.
### Takeaways
So far I have become vastly more proficient with pandas and its myriad methods, attributes, and functions. The importance of abstraction and encapsulation in producing logical, easy-to-understand systems has become evident, and writing  code all day has made me a more efficient text editor at a basic level. I have also become proficient in writing SQL queries to determine business metrics from the data.
## Instructions
### Installation
Download all files into whichever directory you desire and enjoy. The directory archive_data is required for some file path definitions, but its contents are not - they are pulled from various sources as methods are called.

#### environment packages required:
- json
- re
- yaml
- requests
- boto3
- pandas
- numpy
- tabula
- sqlalchemy
### Usage
The function of the project is to collate cleaned data into a single database. To get started, the following steps are essential:
1. Initialise the DatabaseConnector class once for each database to be worked with. The credentials should be in the same format as the 'db_creds_XXX' yaml files in the 'parameters' directory.

2. Initialise an instance of the DataExtractor Class for each database to be extracted from using the engine attribute of the appropriate DatabaseConnector instance.

3. Initialise an instance of DataCleaning. This class contains methods that exclusively act on dataframes. Only one instance is required.

4. You're set - now use the various methods (all can be called with the help function) to download, extract, clean, and reupload to your heart's content. 
An initialisation order in db_main has been suggested. 

## Structure
Currently there are four directories:
 - **"project_MRDC"** is the root directory. The three main modules ("data_cleaning", "data_extraction", and "database_utils") along with "db_main"
and a couple of development test files are stored here.

- **"archive_data"** contains database files that are downloaded as extraction methods are called.

- **"parameters"** contains various dictionaries of information used throughout, like database credentials, urls, and other miscellany.

- **"SQL"** contains text files for the various SQL operations performed on the database once uploaded.
## Notes
There are some instances in which functions were created to remove nonsensical data which turned out to be correct, such as removing card numbers which did not have the correct number of digits for their type. Sadly these tended to be the more interesting functions to create so I have left them commented in.
## License
No License - it's only for practice!
