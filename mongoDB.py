from pymongo import MongoClient
import json
import time
import pandas as pd


client = MongoClient('localhost', 27017)
#apagar a base de dados se existir
if 'DataBaseProj' in client.list_database_names():
   client.drop_database('DataBaseProj')
   print("Previous database deleted with success\n")

#load dos ficheiro para cada documento
f = open('Tabela1.json')
data1 = json.load(f)
f = open('Tabela2.json')
data2 = json.load(f)
f = open('Tabela3.json')
data3 = json.load(f)

#criação da base de dados
db=client.DataBaseProj
print("Database created!\n")
collist = db.list_collection_names()


#criar
if "main_info" in collist:
  print("The collection Main_Info already exists.")
else: 
  db.create_collection('main_info')
  main_info = db.main_info
  new_result1 = main_info.insert_many(data1)
  print("Document main_info was created with success\n")

if "reservas_status" in collist:
  print("The collection Reservas_Status already exists.")
else: 
  
  db.create_collection('reservas_status')
  reserva_status = db.reservas_status
  new_result2 = reserva_status.insert_many(data2)
  print("Document reservas_status was created with success\n")

if "stays_info" in collist:
  print(f"The collection Stays_Info already exists.")
else: 
  db.create_collection('stays_info')
  stays_info = db.stays_info
  new_result3 = stays_info.insert_many(data3)
  print("Document stays_info was created with success\n")

print("Quais são os país que tiveram reservas alteradas superior a 90 e, em que ano isto ocorreu?\n")
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
                "$gt": 90
            }
        }
    },
    {
        "$sort": {
            "_id.arrival_date_year": 1
        }
    },

    
]
start = time.time()
result = db.main_info.aggregate(pipeline)
end = time.time()

for doc in result:
    print(doc)

print("Time taken:", end - start, "\n")


print("Quais são os país que tiveram um numero de reservas alteradas superior a 500 e, em que ano isto ocorreu?\n")
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
                "$gt": 500
            }
        }
    },
    {
        "$sort": {
            "_id.arrival_date_year": 1
        }
    }
]
start = time.time()
result = db.main_info.aggregate(pipeline)
end = time.time()
for doc in result:
    print(doc)

print("Time taken:", end - start)

print("how many families with kids cancelled their reservations\n")
queryComplex = [
    {
        '$lookup': {
            'from': 'reservas_status', 
            'localField': 'id_reservation', 
            'foreignField': 'id_reservation', 
            'as': 'reservation_status'
        }
    }, {
        '$unwind': '$reservation_status'
    }, {
        '$match': {
            '$and': [
                {
                    '$or': [
                        {
                            'children': {
                                '$gt': 0
                            }
                        }, {
                            'babies': {
                                '$gt': 0
                            }
                        }
                    ]
                }, {
                    'reservation_status.is_canceled': 1
                },
                
            ]
            
        },
    

        
    }, 
    
    
]


start = time.time()
result = db.stays_info.aggregate(queryComplex)

end = time.time()
print("Time taken without indexes:", end - start)


print("casais sem filhos ou com filhos tem parqueamento ou refeições\n")
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
                },
                
            ]
        }
    },
    
]
start = time.time()
resultTwo = list(db.stays_info.aggregate(queryComplexTwo))
end = time.time()


print("Time taken without indexes:", end - start)


print("Atualizar um campo 'hotel' para City Seven onde 'hotel' é igual a Resort Hotel\n")
filter_condition = {"hotel": "Resort Hotel"}
update_data = {"$set": {"hotel": "City Seven"}}

result = db.main_info.update_one(filter_condition, update_data)

print(f"Total de documentos correspondentes: {result.matched_count}")
print(f"Total de documentos modificados: {result.modified_count}")


print("criacao de uma nova reserva\n")
main_info_insert={
    "id_reservation": 119391,
    "hotel": "BDA Hotel",
    "lead_time": 30,
    "arrival_date_year": 2023,
    "arrival_date_month": "December",
    "arrival_date_week_number": 48,
    "arrival_date_day_of_month": 1,
    "country": "PRT",
    "market_segment": "Direct",
    "distribution_channel": "Direct",
    "is_repeated_guest": 0,
    "booking_changes": 2,
    "days_in_waiting_list": 0,
    "company": "BDA",
    "customer_type": "Transient",
    "reservation_status_date": "2023-12-01"}


stay_info_insert={
  "id_reservation": 119391,
  "stays_in_week_nights": 0,
  "stays_in_weekend_nights": 0,
  "adults": 2,
  "children": 0,
  "babies": 0,
  "required_car_parking_spaces": 0,
  "meal": "BB",
  "reserved_room_type": "C",
  "total_of_special_requests": 0
}

reservation_status_insert={
    "id_reservation": 119391,
    "is_canceled": 0,
    "arrival_date_year": 2023,
    "arrival_date_month": "December",
    "agent": 48,
    "previous_cancellations": 1,
    "previous_bookings_not_canceled": 0,
    "country": "PRT",
    "deposit_type": "No Deposit",
    "reservation_status": "Check-Out"
    }


# Inserir o novo documento na coleção
try:
    result = db.main_info.insert_one(main_info_insert)
    
    # Verificar se a inserção foi bem-sucedida
    if result.inserted_id:
        print(f"Novo documento inserido com sucesso. ID: {result.inserted_id}")



except Exception as e:
    print(f"Falha ao inserir o documento. Erro: {e}")

try:
    result = db.stays_info.insert_one(stay_info_insert)
    
    # Verificar se a inserção foi bem-sucedida
    if result.inserted_id:
        print(f"Novo documento inserido com sucesso. ID: {result.inserted_id}")



except Exception as e:
    print(f"Falha ao inserir o documento. Erro: {e}")

try:
    result = db.reservas_status.insert_one(reservation_status_insert)
    
    # Verificar se a inserção foi bem-sucedida
    if result.inserted_id:
        print(f"Novo documento inserido com sucesso. ID: {result.inserted_id}")



except Exception as e:
    print(f"Falha ao inserir o documento. Erro: {e}")   



print("Creating indexes\n")
if len(db.stays_info.index_information()) == 1:
    db.stays_info.create_index("id_reservation")
if len(db.reservas_status.index_information()) == 1:
    db.reservas_status.create_index("id_reservation")
if len(db.main_info.index_information()) == 1:
    db.main_info.create_index("id_reservation")

print("how many families with kids cancelled their reservations\n")
queryComplex = [
    {
        '$lookup': {
            'from': 'reservas_status', 
            'localField': 'id_reservation', 
            'foreignField': 'id_reservation', 
            'as': 'reservation_status'
        }
    }, {
        '$unwind': '$reservation_status'
    }, {
        '$match': {
            '$and': [
                {
                    '$or': [
                        {
                            'children': {
                                '$gt': 0
                            }
                        }, {
                            'babies': {
                                '$gt': 0
                            }
                        }
                    ]
                }, {
                    'reservation_status.is_canceled': 1
                },
                
            ]
            
        },
    

        
    }, 
    
    
]


start = time.time()
result = db.stays_info.aggregate(queryComplex)
end = time.time()
docs = list(result)

df = pd.DataFrame(docs)





# Print specific columns
print(df[['_id', 'children', 'babies']].head(10))



print("Number of results:", len(docs))
print("Time taken with indexes:", end - start)


print("casais sem filhos ou com filhos tem parqueamento ou refeições\n")
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
                },
                
            ]
        }
    },
    
]
start = time.time()
resultTwo = list(db.stays_info.aggregate(queryComplexTwo))
end = time.time()
docs = list(resultTwo)
df = pd.DataFrame(docs)





# Print specific columns
print(df[['_id', 'children', 'babies', 'required_car_parking_spaces', 'meal']].head(10))

print("Number of results:", len(docs))

print("Time taken with indexes:", end - start)


client.close()
