import pandas as pd

df = pd.read_csv('hotel_bookings.csv')

df.to_json('jsonFile.json', orient='records', lines=True)