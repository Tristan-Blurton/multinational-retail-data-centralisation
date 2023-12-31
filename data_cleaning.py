import pandas as pd
import numpy as np
import yaml


class DataCleaning:
    """Contains methods for cleaning Pandas DataFrame data.

    The functions herein are best called with arguments (IE dataframes)
    directly extracted from their sources by the DataExtractor class 
    methods *in the same script*; cleaning the data from pd.read_csv or
    pd.read_json can cause indexing issues. 

    Private methods called in the public methods are organised above 
    the method which calls them.

    Public Methods:
     - clean_user_data(user_data)
     - clean_card_data(card_data)
     - clean_store_data(store_data)
     - clean_product_data(product_data)
     - clean_order_data(order_data)
     - clean_events_data(events_data)
     
    Attributes:
     - None
    """

    def clean_user_data(self, user_data):
        """Clean user data dataframe.
        
        Changes formatting to appropriate datatypes. Drops null rows
        and those with invalid or nonsensical data.
        
        Arguments:
         - user_data (DataFrame): Dataframe of user data to be cleaned.
         
        Keyword Arguments: 
         - None
        
        Returns:
         - user_data (DataFrame): Cleaned dataframe of user data.
        """
        user_data.set_index("index", inplace=True)
        # Converting all column types based on pandas defaults:
        user_data = user_data.convert_dtypes()
        # Converting 'date_of_birth' and 'join_date' to datetime format:
        user_data.date_of_birth = pd.to_datetime(user_data.date_of_birth,
                                               format="mixed",
                                               errors="coerce")
        user_data.join_date = pd.to_datetime(user_data.join_date,
                                           format="mixed",
                                           errors="coerce")
        # Dropping rows with null values both previously existing 
        # and those generated by to_datetime() where no date format was found:
        user_data.dropna(inplace=True)
        # Replacing mistyped country code from 'GGB' to 'GB':
        user_data["country_code"].replace({"GGB":"GB"}, inplace=True)
        # Filtering out rows with user age outside of reasonable range 
        # (there aren't any removed in this case). 
        # 'min_user_age' and 'max_user_age' can be adjusted as desired:
        now = pd.Timestamp.now()
        min_user_age = np.timedelta64(16, "Y")
        max_user_age = np.timedelta64(120, "Y")
        user_data = user_data[(user_data.date_of_birth + min_user_age < now) 
                              | (user_data.date_of_birth + max_user_age > now)]
        # Filtering out rows where 'join_date' is in the future or 'join_date' 
        # is earlier than 'date_of_birth'. Not currently in use. 
        #user_data = user_data[(user_data.join_date < now)
        #                      & (user_data.join_date > user_data.date_of_birth)]
        return user_data
    
    def __card_number_length_check(self, card_data):
        """Drop rows with incorrect card number lengths."""
        # Load dictionary relating card lengths to providers:
        with open("parameters/number_lengths.yaml", "r") as file:
            number_lengths = dict(yaml.safe_load(file))
        # Initialise list:
        valid_numbers = []
        # Create a list of series of card numbers of invalid length:
        # Create two boolean masks per iteration:
        #  - 'provider_mask' is true when the card provider matches the 
        #     loop iteration's current card number length. 
        #  - 'correct_length_mask' is true when the length of the card number
        #     matches the iteration's current card number length.  
        # Create a new list of card numbers where both these masks are true.
        # Append this list to the list 'valid_numbers' each iteration.  
        for card_length in number_lengths.keys():
            provider_mask = card_data.card_provider.isin(number_lengths[card_length])
            correct_length_mask = card_data.card_number.str.match(f"^\d{card_length}$")
            valid_numbers.extend(card_data.card_number[provider_mask & correct_length_mask])

        # Drop rows where card numbers are invalid:
        card_data = card_data[card_data.card_number.isin(valid_numbers)]
        return(card_data)

    def __card_data_reformat(self, card_data):
        """Change card data dataframe columns to appropriate data types."""
        card_data.card_number = card_data.card_number.astype("int64", errors="raise")
        card_data.expiry_date = pd.to_datetime(card_data.expiry_date,
                                       format="%m/%y",
                                       errors="raise",)
        card_data.card_provider = card_data.card_provider.astype("string")
        card_data.date_payment_confirmed = pd.to_datetime(card_data.date_payment_confirmed,
                                                  format="mixed",
                                                  errors="raise")
        return(card_data)
    
    def clean_card_data(self, card_data):
        """Clean card data dataframe.
        
        Changes formatting to appropriate datatypes. Drops null rows
        and those with invalid or nonsensical data.
        
        Arguments:
         - card_data (DataFrame): Dataframe of card data to be cleaned.
         
        Keyword Arguments: 
         - None
        
        Returns:
         - card_data (DataFrame): Cleaned dataframe of card data.
        """
        # Drop rows with null values:
        card_data = card_data.dropna()
        # Change datatype to string for easier manipulation:
        card_data.loc[:, "card_number"] = card_data.card_number.astype("string")
        # Strip invalid characters from beginning and end:
        card_data.loc[:, "card_number"] = card_data.card_number.str.strip("? ")
        # Drop rows where card number have other invalid characters:
        card_data = card_data[card_data.card_number.str.match("^\d+$")] 
        # Drop rows with invalid card number lengths (Currently not in
        # use due to the realities of the practical exercise):
        ## card_data = self.__card_number_length_check(card_data)
        # Change columns to appropriate data types:
        card_data = self.__card_data_reformat(card_data)
        # Correct index:
        card_data.reset_index(inplace=True, drop=True)
        card_data.index.rename("Index", inplace=True)
        return (card_data)
    
    def __clean_lat_long(self, store_data):
        """Fix latitude and longitude data in stores dataset."""
        # Give latitude and longitude a consistent number of decimal places
        # (four is enough for any addressing requirements):
        store_data.longitude = store_data.longitude.round(decimals=4)
        store_data.latitude = store_data.latitude.round(decimals=4)
        # Remove lat and long data where invalid:
        store_data.latitude.where((store_data.latitude < 90.0) 
                                    &(store_data.latitude > -90.0),
                                    None,
                                    inplace=True)
        store_data.longitude.where((store_data.longitude < 90.0) 
                                    &(store_data.longitude > -90.0),
                                    None,
                                    inplace=True)
        return(store_data)
    
    def __store_data_reformat(self, store_data):
        """Change store data dataframe columns to appropriate data types."""
        store_data = store_data.convert_dtypes()
        store_data.opening_date = pd.to_datetime(store_data.opening_date,
                                                 format="mixed")
        store_data.latitude = pd.to_numeric(store_data.latitude,
                                            errors="coerce")
        store_data.longitude = pd.to_numeric(store_data.longitude,
                                            errors="coerce")
        store_data.staff_numbers = store_data.staff_numbers.astype("Int16")
        return (store_data)
    
    def clean_store_data(self, store_data):
        """Clean store data dataframe.
        
        Changes formatting to appropriate datatypes. Drops null rows
        and those with invalid or nonsensical data. Some null values 
        remain as most appropriate datatype, particularly in the web
        store row.
        
        Arguments:
         - store_data (DataFrame): Dataframe of store data to be cleaned.
         
        Keyword Arguments: 
         - None
        
        Returns:
         - store_data (DataFrame): Cleaned dataframe of store data.
        """
        #Drop lat column completely as it contains no useful data:
        store_data.drop("lat", axis=1, inplace=True)
        # Change continent datatype:
        store_data.continent = store_data.continent.astype("string")
        # Replace typos in continent column:
        store_data.continent.replace(["eeEurope", "eeAmerica"],
                                    ["Europe", "America"],
                                    inplace=True)
        # Remove corrupted rows in dataframe based on continents column:
        store_data = store_data[store_data.continent.str.fullmatch("^Europe$|^America$")]        
        # Fix staff numbers column to remove any non-digit characters. 
        # Using .loc instead of the series attribute to assign because 
        # the 'copy of a view' error is thrown otherwise:
        store_data.loc[:,"staff_numbers"] = store_data.staff_numbers\
                                            .astype("string")
        store_data.loc[:,"staff_numbers"] = store_data.staff_numbers\
                                            .str.replace("(\D)", "", 
                                            regex=True)
        # Format datatypes:
        store_data = self.__store_data_reformat(store_data)
        # Clean lat and long data:
        store_data = self.__clean_lat_long(store_data)
        # Reorder columns to put latitude and longitude next to each other:
        store_data = store_data.iloc[:, [0,1,7,2,3,4,5,6,8,9]]
        # Replace address data newline characters with commas:
        store_data.address = store_data.address.str.replace("(\n)", ", ", regex=True)
        return(store_data)

    def __multipack_to_kg(self, product_data):
        """Add column "multipack" with mulitpack items in kg as type float."""
        product_data[["multipack_1", "multipack_2"]] = product_data.weight\
                                                       .str.extract("^(\d+) x (\d+)")
        product_data.multipack_1 = product_data.multipack_1.astype("float32")
        product_data.multipack_2 = product_data.multipack_2.astype("float32")
        product_data["multipack"] = product_data.multipack_1 * product_data.multipack_2
        product_data.drop(["multipack_1", "multipack_2"], axis=1, inplace=True)
        product_data.multipack = product_data.multipack\
                                 .divide(1000)\
                                 .fillna(value=0)
        return(product_data)

    def __oz_to_kg(self, product_data):
        """Add column "weight_oz" with items in ounces to kg as type float."""
        product_data["weight_oz"] = product_data.weight.str.extract("^(\d+\.?\d+)oz+$")
        product_data.weight_oz = product_data.weight_oz\
                                 .astype("float32")\
                                 .divide(35.27)\
                                 .fillna(value=0)
        return(product_data)

    def __g_ml_to_kg(self, product_data):
        """Add Column "weight_g_ml" with items in g and ml to kg as type float."""
        product_data["weight_g_ml"] = product_data.weight\
                                      .str.extract("^(\d+\.?\d*)[^k]?[g|ml].*$")\
                                      .astype("float32")\
                                      .divide(1000)\
                                      .fillna(value=0)
        return(product_data)

    def __format_kg(self, product_data):
        """Reformat weight column to be compatible with column addition."""
        product_data.weight = product_data.weight[product_data.weight.str.contains("kg")]\
                              .str.strip("kg")\
                              .astype("float32")
        product_data.weight.fillna(value=0, inplace=True)
        return(product_data)
    
    def __correct_homeware_situation(self, product_data):
        """
        Correct Toaster and Kettle weights.
        
        Could also change the unit before processing in 'convert_product_weights'
        but this method accounts for all too-light errors with those products
        rather than just incorrect unit errors.
        """
        product_data.product_name = product_data.product_name.astype("string")
        product_data.weight.mask((product_data.product_name.str.contains("Toaster|Kettle"))
                                 & (product_data.weight < 0.050),
                                 product_data.weight * 1000,
                                 inplace=True)
        return(product_data)
    
    def __convert_product_weights(self, product_data):
        """Convert product data weight column to kg as float values."""
        # Change weight column dtype to string:
        product_data.weight = product_data.weight.astype("string")
        # Deal with multipack values:
        product_data = self.__multipack_to_kg(product_data)
        # Deal with values in ounces:
        product_data = self.__oz_to_kg(product_data)
        # Deal with values in grams or millilitres:
        product_data = self.__g_ml_to_kg(product_data)
        # Reformat weight column:
        product_data = self.__format_kg(product_data)
        # Recombine columns:
        product_data.weight = product_data.multipack\
                              + product_data.weight_oz\
                              + product_data.weight_g_ml\
                              + product_data.weight
        # Drop obselete columns:
        product_data.drop(["multipack", "weight_oz", "weight_g_ml"],
                           axis=1,
                           inplace=True)
        # Correct kettle and toaster weight values where unreasonably small:
        product_data = self.__correct_homeware_situation(product_data)
        return(product_data)

    def clean_product_data(self, product_data):
        """Clean product data dataframe.
        
        Changes formatting to appropriate datatypes. Drops null rows
        and those with invalid or nonsensical data. Converts all weights
        to kg and type float. Corrects some incorrectly-entered weights.
        
        Arguments:
         - product_data (DataFrame): Dataframe of product data to be cleaned.
         
        Keyword Arguments: 
         - None
        
        Returns:
         - product_data (DataFrame): Cleaned dataframe of product data.
        """
        # Return weights column in kg as float values:
        product_data = self.__convert_product_weights(product_data)
        # Drop rows where weight is zero - all contain only corrupt or missing data:
        product_data = product_data[product_data.weight != 0]
        # Convert object dtypes to string:
        product_data = product_data.convert_dtypes()
        # Convert date_added to datetime64 format:
        product_data.date_added = pd.to_datetime(product_data.date_added, format="mixed")
        return(product_data)

    def clean_order_data(self, order_data):
        """Clean order data dataframe.
        
        Changes formatting to appropriate datatypes. Drops null rows
        and columns with invalid or nonsensical data.
        
        Arguments:
         - order_data (DataFrame): Dataframe of order data to be cleaned.
         
        Keyword Arguments: 
         - None
        
        Returns:
         - order_data (DataFrame): Cleaned dataframe of order data.
        """
        # Drop bad rows:
        order_data.drop(["level_0", "first_name", "last_name", "1"],
                 axis=1, inplace=True)
        # Set index column as pandas index:
        order_data.set_index("index", drop=True, inplace=True)
        # Convert object to string datatypes:
        order_data = order_data.convert_dtypes()
        return(order_data)
    
    def clean_events_data(self, events_data):
        """Clean events data dataframe.
        
        Changes formatting to appropriate datatypes. Drops null rows
        and those with invalid or nonsensical data. Combines the seperate 
        datetime data columns into a single column with type datetime64(ns).
        
        Arguments:
         - events_data (DataFrame): Dataframe of events data to be cleaned.
         
        Keyword Arguments: 
         - None
        
        Returns:
         - events_data (DataFrame): Cleaned dataframe of events data.
        """
        # Convert all datatypes to string:
        events_data = events_data.convert_dtypes()
        # Create new column "datetime":
        # - Create list "digits" of single digits in string format.
        # - When any dataframe item matches an item in the list
        #   then the value is replaced with itself and a leading zero.
        # - Combine the four date/time data columns into one string per row 
        # that "to_datetime()" can parse. 
        digits = map(str, list(range(0,10)))
        events_data = events_data.mask(events_data.isin(digits), "0" + events_data)
        events_data = events_data[events_data.timestamp.str.match("^\d\d:\d\d:\d\d$")]
        events_data["datetime"] = events_data.year\
                                + events_data.month\
                                + events_data.day\
                                + " " + events_data.timestamp                  
        # Format datetime column as pandas datetime(ns).
        events_data.datetime = pd.to_datetime(events_data.datetime)
        # Drop obselete rows:                                    
        events_data = events_data.iloc[:, [6,4,5]]
        return(events_data)