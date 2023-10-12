import pandas as pd
import numpy as np

card_data = pd.read_csv("card_data.csv", index_col=0)
card_data.card_number = card_data.card_number.astype("string")
card_data.card_provider = card_data.card_provider.astype("string")
card_data = card_data[card_data.card_number.str.match("\D") == False]
pd.set_option("display.max_rows", 500)

number_lengths = {"{13}": ["VISA 13 digit"],
                  "{15}": ["JCB 15 digit", "American Express"],
                  "{16}": ["Visa 16 digit", "JCB 16 digit", "Diners Club / Carte Blanche", "Maestro", "Discover", "Mastercard",],
                  "{19}": ["VISA 19 digit"]
                  }

invalid_numbers = []




for card_length in number_lengths.keys():
    mask_1 = card_data.card_provider.isin(number_lengths[card_length])
    mask_2 = ~card_data.card_number.str.match(f"^\d{card_length}$")                     
    invalid_numbers.append(card_data.card_number[mask_1 & mask_2])

invalid_numbers = pd.concat(invalid_numbers)
invalid_numbers = invalid_numbers.tolist()


print(invalid_numbers)
print(card_data[card_data.card_number.isin(invalid_numbers)])





