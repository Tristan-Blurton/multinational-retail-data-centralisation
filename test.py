from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd
import numpy as np

legacy_user_data = pd.read_csv("legacy_user_data.csv")

# Converting all datatype based on pandas defaults:
legacy_user_data.set_index("index", inplace=True)
legacy_user_data = legacy_user_data.convert_dtypes()
# Converting date of birth and join date to datetime format:
legacy_user_data.date_of_birth = pd.to_datetime(legacy_user_data.date_of_birth,
                                                format="mixed",
                                                errors="coerce")
legacy_user_data.join_date = pd.to_datetime(legacy_user_data.join_date,
                                            format="mixed",
                                            errors="coerce")
# Dropping rows with null values - both previously existing and those generated
# by to_datetime where no date format was found:
legacy_user_data.dropna(inplace=True)
# Replacing mistyped country code from GGB to GB:
legacy_user_data["country_code"].replace({"GGB":"GB"}, inplace=True)
# Filtering out rows with user age outside of reasonable range (there aren't any removed
# in this case). min_user_age and max_user_age can be adjusted as desired:
now = pd.Timestamp.now()
min_user_age = np.timedelta64(16, "Y")
max_user_age = np.timedelta64(120, "Y")
legacy_user_data = legacy_user_data[(legacy_user_data.date_of_birth + min_user_age < now) 
                                        | (legacy_user_data.date_of_birth + max_user_age > now)]
# Filtering out rows where join date is in the future or join date is earlier than date of birth.
legacy_user_data = legacy_user_data[(legacy_user_data.join_date < now)
                                        & (legacy_user_data.join_date > legacy_user_data.date_of_birth)]
# Filtering out rows where user_uuid is in an invalid format (there are none in this case).
legacy_user_data = legacy_user_data[legacy_user_data.user_uuid.str.match(
                                    "^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$")]

print(legacy_user_data)

