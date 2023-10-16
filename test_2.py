import pandas as pd
import numpy as np
import yaml

card_data = pd.read_csv("card_data.csv")

card_data.dropna(inplace=True)
        # Remove rows where the card number has any non-digit character:
card_data = card_data[card_data.card_number.str.match("\d+$")] 

print(card_data)