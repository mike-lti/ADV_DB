from pymongo import MongoClient
import json


client = MongoClient()

client = MongoClient('localhost', 27017)

f = open('Tabela1.json')
data1 = json.load(f)
f = open('Tabela2.json')
data2 = json.load(f)
f = open('Tabela3.json')
data3 = json.load(f)

db=client.DataBaseProj

collist = db.list_collection_names()

if "Main_Info" in collist:
  print("TThe collection Main_Info already exists.")
else: 
  db.create_collection('Main_Info')


if "Reservas_Status" in collist:
  print("The collection Reservas_Status already exists.")
else: 
  db.create_collection('Reservas_Status')


if "Stays_Info" in collist:
  print(f"The collection Stays_Info already exists.")
else: 
  db.create_collection('Stays_Info')


main_info = db.Main_Info
reserva_status = db.Reservas_Status
stays_info = db.Stays_Info


new_result1 = main_info.insert_many(data1)
new_result2 = reserva_status.insert_many(data2)
new_result3 = stays_info.insert_many(data3)


print(f"IDs inseridos: {new_result1.inserted_ids}")
print(f"IDs inseridos: {new_result2.inserted_ids}")
print(f"IDs inseridos : {new_result3.inserted_ids}")
client.close()