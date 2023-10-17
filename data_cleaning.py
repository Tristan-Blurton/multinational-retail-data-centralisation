import pandas as pd
import numpy as np
import yaml


class DataCleaning:
    # A class used to clean data from different sources.

    def clean_user_data(self, user_df):
        # A method to clean the user data from the RDS database provided.
        # Remove double index:
        user_df.set_index("index", inplace=True)
        # Converting all column types based on pandas defaults:
        user_df = user_df.convert_dtypes()
        # Converting 'date_of_birth' and 'join_date' to datetime format:
        user_df.date_of_birth = pd.to_datetime(user_df.date_of_birth,
                                               format="mixed",
                                               errors="coerce")
        user_df.join_date = pd.to_datetime(user_df.join_date,
                                           format="mixed",
                                           errors="coerce")
        # Dropping rows with null values both previously existing 
        # and those generated by to_datetime() where no date format was found:
        user_df.dropna(inplace=True)
        # Replacing mistyped country code from 'GGB' to 'GB':
        user_df["country_code"].replace({"GGB":"GB"}, inplace=True)
        # Filtering out rows with user age outside of reasonable range 
        # (there aren't any removed in this case). 
        # 'min_user_age' and 'max_user_age' can be adjusted as desired:
        now = pd.Timestamp.now()
        min_user_age = np.timedelta64(16, "Y")
        max_user_age = np.timedelta64(120, "Y")
        user_df = user_df[(user_df.date_of_birth + min_user_age < now) 
                              | (user_df.date_of_birth + max_user_age > now)]
        # Filtering out rows where 'join_date' is in the future or 'join_date' 
        # is earlier than 'date_of_birth'.
        user_df = user_df[(user_df.join_date < now)
                              & (user_df.join_date > user_df.date_of_birth)]
        # Filtering out rows where 'user_uuid' is in an incorrect format
        # (there are none in this case).
        user_df = user_df[user_df.user_uuid.str.match(
                          "^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$")]
        return user_df
    
    def __card_number_length_check(self, card_data):
        """Drop rows with incorrect card number lengths."""
        # Load dictionary relating card lengths to providers:
        with open("parameters/number_lengths.yaml", "r") as file:
            number_lengths = dict(yaml.safe_load(file))
        # Initialise list:
        invalid_numbers = []
        # Create a list of series of card numbers of invalid length:
        # Create two boolean masks per iteration:
        #  - 'provider_mask' is true when the card provider matches the 
        # loop iteration's current card number length. 
        #  - 'incorrect_length_mask' is true when the length of the card number
        #  does not match the iteration's current card number length.  
        # Create a new series of card numbers where both these masks are true.
        # Append this series to the list of series 'invalid_numbers' each iteration.  
        for card_length in number_lengths.keys():
            provider_mask = card_data.card_provider.isin(number_lengths[card_length])
            incorrect_length_mask = ~card_data.card_number.str.match(f"^\d{card_length}$")                     
            invalid_numbers.append(card_data.card_number[provider_mask & incorrect_length_mask])

        # Concatenate the list of series into one series:
        invalid_numbers = pd.concat(invalid_numbers)
        # Convert the series to a python list:
        invalid_numbers = invalid_numbers.tolist()
        # Drop rows where card numbers are invalid:
        card_data = card_data[~card_data.card_number.isin(invalid_numbers)]
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
        """Clean card data."""
        # Drop rows with null values:
        card_data.dropna(inplace=True)
        # Remove rows where the card number has any non-digit character:
        card_data.card_number = card_data.card_number.astype("string")
        card_data = card_data[card_data.card_number.str.match("^\d+$")] 
        # Drop rows with invalid card number lengths:
        card_data = self.__card_number_length_check(card_data)
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
        store_data.address = store_data.address.astype("string")
        store_data.latitude = store_data.latitude.astype("Float64")
        store_data.longitude = store_data.longitude.astype("Float64")
        store_data.locality = store_data.locality.astype("string")
        store_data.store_code = store_data.store_code.astype("string")
        store_data.opening_date = pd.to_datetime(store_data.opening_date, format="mixed")
        store_data.staff_numbers = store_data.staff_numbers.astype("int16")
        store_data.store_type = store_data.store_type.astype("string")
        store_data.country_code = store_data.country_code.astype("string")
    
    def clean_store_data(self, store_data):
        """Clean store data. Some null values remain as most appropriate type."""
        # Drop lat column as it contains mostly null values and can be replaced
        # by latitude:
        store_data.drop("lat", axis=1, inplace=True)
        # Change continent datatype:
        store_data.continent = store_data.continent.astype("string")
        # Replace typos in continent column:
        store_data.continent.replace(["eeEurope", "eeAmerica"],
                                    ["Europe", "America"],
                                    inplace=True)
        # Remove corrupted rows in dataframe based on continents column:
        store_data = store_data[store_data.continent.str.fullmatch("^Europe$|^America$")]        
        # Fix staff numbers column to remove any non-digit characters:
        store_data.staff_numbers = store_data.staff_numbers.astype("str")
        store_data.staff_numbers = store_data.staff_numbers.str.replace("(\D)", "", 
                                                                        regex=True)
        # Format datatypes:
        store_data = self.__store_data_reformat(store_data)
        # Clean lat and long data:
        store_data = self.__clean_lat_long(store_data)
        # Reorder columns to put latitude and longitude next to each other:
        store_data = store_data.iloc[:, [0,1,7,2,3,4,5,6,8,9]]
        # Replace address data newline characters with commas:
        store_data.address = store_data.address.str.replace("(\n)", ", ", regex=True)
        print(store_data.head(20))

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
        # Drop obviated columns:
        product_data.drop(["multipack", "weight_oz", "weight_g_ml"],
                           axis=1,
                           inplace=True)
        # Correct kettle and toaster weight values where unreasonably small:
        product_data = self.__correct_homeware_situation(product_data)
        return(product_data)

    def clean_product_data(self, product_data):
        """Clean "Product_Data" DataFrame."""
        # Return weights column in kg as float values:
        product_data = self.__convert_product_weights(product_data)
        # Drop rows where weight is zero - all contain only corrupt or missing data:
        product_data = product_data[product_data.weight != 0]
        # Drop rows with duplicate names:
        product_data = product_data.drop_duplicates(subset="product_name")
        # Convert object dtypes to string:
        product_data = product_data.convert_dtypes()
        # Convert date_added to datetime64 format:
        product_data.date_added = pd.to_datetime(product_data.date_added, format="mixed")
        return(product_data)
