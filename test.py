from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd


legacy_user_data = pd.read_csv("legacy_user_data.csv")
legacy_user_data = legacy_user_data.convert_dtypes()
legacy_user_data.date_of_birth = pd.to_datetime(legacy_user_data.date_of_birth,
                                                format="mixed",
                                                errors="coerce")
legacy_user_data.join_date = pd.to_datetime(legacy_user_data.join_date,
                                            format="mixed",
                                            errors="coerce")
legacy_user_data.dropna(inplace=True)

legacy_user_data["country_code"].replace({"GGB":"GB"}, inplace=True)

legacy_user_da
