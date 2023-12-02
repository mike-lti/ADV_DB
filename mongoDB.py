from pymongo import MongoClient
import json


client = MongoClient('localhost', 27017)

f = open('Tabela1.json')
data1 = json.load(f)
f = open('Tabela2.json')
data2 = json.load(f)
f = open('Tabela3.json')
data3 = json.load(f)

db=client.DataBaseProj

collist = db.list_collection_names()
print(collist)
if "main_info" in collist:
  print("The collection Main_Info already exists.")
else: 
  db.create_collection('main_info')
  main_info = db.main_info
  new_result1 = main_info.insert_many(data1)


if "reservas_status" in collist:
  print("The collection Reservas_Status already exists.")
else: 
  
  db.create_collection('reservas_status')
  reserva_status = db.reservas_status
  new_result2 = reserva_status.insert_many(data2)


if "stays_info" in collist:
  print(f"The collection Stays_Info already exists.")
else: 
  db.create_collection('stays_info')
  stays_info = db.stays_info
  new_result3 = stays_info.insert_many(data3)


"""
#Quais são os país que tiveram reservas alteradas superior a 100 e, em que ano isto ocorreu? 
pipeline = [
    {
        "$match": {
            "booking_changes": 0,
            "country": {"$ne": None}
        }
    },
    {
        "$group": {
            "_id": {
                "arrival_date_year": "$arrival_date_year",
                "country": "$country"
            },
            "total_reservas_alteradas": {
                "$sum": 1
            }
        }
    },
    {
        "$match": {
            "total_reservas_alteradas": {
                "$gt": 100
            }
        }
    },
    {
        "$sort": {
            "_id.arrival_date_year": 1
        }
    }
]
"""

queryComplex = [
    {
        '$lookup': {
            'from': 'reservas_status',
            'localField': 'id_reservation',
            'foreignField': 'id_reservation',
            'as': 'results'
        }
    },
    {
        "$unwind": "$results"
    },
    {
        '$match': {
            '$and': [
                {
                    '$or': [
                        {'results.children': {'$gt': 0}},
                        {'results.babies': {'$gt': 0}}
                    ]
                },
                {
                  'results.is_canceled': {'$eq': 1}
                }
            ]
        }
    }
    
]

result = db.stays_info.aggregate(queryComplex)

# Print the result
for doc in result:
    print(doc)

queryComplexTwo = [
    {
        '$match': {
            '$and': [
                {
                    '$or': [
                        {'babies': {'$gt': 0}},
                        {'children': {'$gt': 0}}
                    ]
                },
                {
                    '$or': [
                        {'required_car_parking_spaces': 1},
                        {'meal': 'BB'}
                    ]
                }
            ]
        }
    }
]

# Execute the aggregation pipeline
resultTwo = list(db.stays_info.aggregate(queryComplexTwo))

# Print the result
for doc in resultTwo:
    print(doc)

# Execute the aggregation pipeline
#result = db.stays_info.aggregate(queryComplex)

# Print the result
#for doc in result:
    #print(doc)







#print(f"IDs inseridos: {new_result1.inserted_ids}")
#print(f"IDs inseridos: {new_result2.inserted_ids}")
#print(f"IDs inseridos : {new_result3.inserted_ids}")
client.close()
