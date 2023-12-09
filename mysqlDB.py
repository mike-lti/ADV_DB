import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, String

# Creates the database
mydb = mysql.connector.connect(
  host="localhost",
  user="root", 
  password = '1234'
  
)

mycursor = mydb.cursor()

mycursor.execute('DROP DATABASE IF EXISTS DataBaseProj')
mycursor.execute("CREATE DATABASE DataBaseProj")
mydb.commit()
# Close the connection to the database
mydb.close()



mydb = mysql.connector.connect(
  host="localhost",
  user="root", 
  password = '1234',
  database = 'DataBaseProj'
  
)

mycursor = mydb.cursor()
mycursor.execute('DROP TABLE IF EXISTS Main_Info')
mycursor.execute("""
CREATE TABLE main_info (
    id_reservation INT AUTO_INCREMENT PRIMARY KEY,
    hotel VARCHAR(255),
    lead_time INT,
    arrival_date_year INT,
    arrival_date_month VARCHAR(255),
    arrival_date_week_number INT,
    arrival_date_day_of_month INT,
    country VARCHAR(255),
    market_segment VARCHAR(255),
    distribution_channel VARCHAR(255),
    is_repeated_guest INT,
    booking_changes INT,
    days_in_waiting_list INT,
    company VARCHAR(255),
    customer_type VARCHAR(255),
    reservation_status_date DATE
);

""")

mycursor.execute('DROP TABLE IF EXISTS Reservas_Status')
mycursor.execute("""
CREATE TABLE reservas_status (
    id_reservation INT,
    is_canceled INT,
    arrival_date_year INT,
    arrival_date_month VARCHAR(255),
    agent VARCHAR(255),
    previous_cancellations INT,
    previous_bookings_not_canceled INT,
    country VARCHAR(255),
    deposit_type VARCHAR(255),
    reservation_status VARCHAR(255),
    PRIMARY KEY (id_reservation),
    FOREIGN KEY (id_reservation) REFERENCES Main_Info(id_reservation)
);

""")

mycursor.execute('DROP TABLE IF EXISTS Stays_Info')
mycursor.execute("""
CREATE TABLE stays_info (
    id_reservation INT,
    stays_in_week_nights INT,
    stays_in_weekend_nights INT,
    adults INT,
    children INT,
    babies INT,
    required_car_parking_spaces INT,
    meal VARCHAR(255),
    reserved_room_type VARCHAR(255),
    total_of_special_requests INT,
    PRIMARY KEY (id_reservation),
    FOREIGN KEY (id_reservation) REFERENCES Main_Info(id_reservation)
);

""")

mydb.commit()
print("Database 'Table Main_Info, Reservas_Status, Stays_Info ' created successfully!")


df_main_info = pd.read_csv("Tabela1.csv")
df_main_info_list = df_main_info.values.tolist()


df_reservas_status = pd.read_csv("Tabela2.csv")
df_reservas_status_list = df_reservas_status.values.tolist()


df_stays_info = pd.read_csv("Tabela3.csv")
df_stays_info_list = df_stays_info.values.tolist()

 

# mycursor.executemany(main_info_insert_query, df_main_info_list)
# mycursor.executemany(reservas_status_insert_query, df_reservas_status_list)
# mycursor.executemany(stays_info_insert_query, df_stays_info_list)
# mydb.commit()

#Query simples 1 "Quais são os país que tiveram resevas superior a 1000 e, em que ano e mes que isto aconteceu e o hotel que mais requições teve?"
start_time = time.time()

sql_consulta = """
    SELECT hotel,country,arrival_date_year AS arrival_year,arrival_date_month AS arrival_month,COUNT(*) AS total_reservas
    FROM main_info GROUP BY hotel, country, arrival_date_year, arrival_date_month HAVING COUNT(*) > 500 ORDER BY total_reservas DESC
"""
explain_query = f"{sql_consulta}"
mycursor.execute(explain_query)
explain_result = mycursor.fetchall()
# Crie uma lista de dicionários a partir do resultado
columns = ["country", "hotel", "arrival_year", "arrival_month", "babies", "total_reservas"]
explain_rows = [dict(zip(columns, row)) for row in explain_result]
# Converta a lista de dicionários em um DataFrame
df_explain = pd.DataFrame(explain_rows)
# Exiba o DataFrame
print(df_explain)
# Calcule e exiba o tempo de execução
end_time = time.time()
execution_time = end_time - start_time
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")

#Query simple 2 "Quais são os país que tiveram resevas alteradas suerior a 100 e, em que ano isto ocorreu?"
#R:
start_time = time.time()
sqlConsult = """
    SELECT country, arrival_date_year, COUNT(*) AS total_reservas_alteradas FROM main_info
   WHERE booking_changes = 0 GROUP BY arrival_date_year, country HAVING COUNT(*) > 90 ORDER BY arrival_date_year;
"""
#A cláusula HAVING é usada em combinação com GROUP BY para filtrar os resultados de uma agregação com base a contagem das reservas.
#Neste caso, estamos filtrando os resultados para incluir apenas as linhas onde o total de reservas é maior que 100.
explain_query = f"{sqlConsult}"
mycursor.execute(explain_query)
explain_result = mycursor.fetchall()
# Cria uma lista de dicionários a partir do resultado
columns = ["country", "arrival_year", "total_reservas_alteradas"]
explain_rows = [dict(zip(columns, row)) for row in explain_result]
# Converta a lista de dicionários em um DataFrame
df_explain = pd.DataFrame(explain_rows)
# Exiba o DataFrame
print(df_explain)
# Calcule e exiba o tempo de execução
end_time = time.time()
execution_time = end_time - start_time
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")


#query complexa 1 casais sem filhos ou com filhos quem cancela mais reservas

complex_query1 = """
    SELECT main_info.country, main_info.hotel, stays_info.adults, stays_info.children,stays_info.babies, reservas_status.reservation_status
    FROM main_info JOIN stays_info ON main_info.id_reservation = stays_info.id_reservation
    JOIN reservas_status ON main_info.id_reservation = reservas_status.id_reservation
    WHERE stays_info.adults > 0 AND (stays_info.children > 0 OR stays_info.babies > 0) AND reservas_status.reservation_status NOT LIKE 'Canceled';
"""
# Execute a consulta e armazene o resultado em um DataFrame
explain_query = f"{complex_query1}"
mycursor.execute(explain_query)
explain_result = mycursor.fetchall()
# Crie uma lista de dicionários a partir do resultado
columns = ["country", "hotel", "adults", "children", "babies", "reservation_status"]
explain_rows = [dict(zip(columns, row)) for row in explain_result]
# Converta a lista de dicionários em um DataFrame
df_explain = pd.DataFrame(explain_rows)
# Exiba o DataFrame
print(df_explain)
# Calcule e exiba o tempo de execução
end_time = time.time()
execution_time = end_time - start_time
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")

#query complexa 2 casais sem filhos ou com filhos tem parqueamento e/ou refeições
complex_query2 = """
    SELECT main_info.country, main_info.hotel,stays_info.adults,stays_info.children,stays_info.babies,
    stays_info.required_car_parking_spaces,stays_info.meal
    FROM main_info JOIN stays_info ON main_info.id_reservation = stays_info.id_reservation
    WHERE stays_info.adults > 0 AND (stays_info.children > 0 OR stays_info.babies > 0) AND (stays_info.required_car_parking_spaces > 0 
      OR stays_info.meal NOT LIKE 'Undefined' OR stays_info.meal NOT LIKE 'SC');
"""
# Execute a consulta e armazene o resultado em um DataFrame
explain_query1 = f"{complex_query2}"
mycursor.execute(explain_query1)
explain_result1 = mycursor.fetchall()
# Crie uma lista de dicionários a partir do resultado
columns = ["country", "hotel", "adults", "children", "babies", "car_parking", "meal"]
explain_rows1 = [dict(zip(columns, row)) for row in explain_result1]
# Converta a lista de dicionários em um DataFrame
df_explain1 = pd.DataFrame(explain_rows1)
# Exiba o DataFrame
print(df_explain1)
# Calcule e exiba o tempo de execução
end_time = time.time()
execution_time = end_time - start_time
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")


#Actualização de dados na tabela Main_info
# Novo valor para a coluna hotel
novo_hotel = "City Hotel"
start_time = time.time()
# Consulta de atualização
sql_update = text(f"""
    UPDATE main_info
    SET hotel = :novo_hotel
    WHERE hotel = "7 Hotel"
""")

# Execute a consulta de atualização
with engine.begin() as connection:
    connection.execute(sql_update, {"novo_hotel": novo_hotel})
end_time = time.time()
execution_time = end_time - start_time
print("Atualização concluída com sucesso.")
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")

#Inserção de dados na tabela Main_info
try:
    start_time = time.time()

    #Inserção de novos dados na tabela Main_Info
    data = {
        'hotel': ['BDA Hotel'],
        'lead_time': [30],
        'arrival_date_year': [2023],
        'arrival_date_month': ['December'],
        'arrival_date_week_number': [48],
        'arrival_date_day_of_month': ['1'],
        'country': ['PRT'],
        'market_segment': ['Direct'],
        'distribution_channel': ['Direct'],
        'is_repeated_guest': [0],
        'booking_changes': [2],
        'days_in_waiting_list': [0],
        'company': ['BDA'],
        'customer_type': ['Transient'],
        'reservation_status_date': ['2023-12-01']
    }

    df = pd.DataFrame(data)

    # Inserção de dados na tabela 'Main_Info' usando SQLAlchemy
    df.to_sql('main_info', con=engine, if_exists='append', index=False)

    # Recupere o registro recém-inserido
    query = "SELECT id_reservation,hotel,company,reservation_status_date FROM main_info WHERE hotel = 'BDA Hotel' LIMIT 1"
    inserted_record = pd.read_sql(query, con=engine)

    end_time = time.time()
    execution_time = end_time - start_time

    print("Inserção concluída com sucesso.")
    print("Registro inserido:")
    print(inserted_record)
   data_hora_execucao = datetime.now()
   print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")

except Exception as e:
    print(f"Erro durante a inserção: {str(e)}")

#Inserção de novos dados nas tabela que contem chaves estrangeiras(reservas_status e stays_info)

start_time = time.time()

#Novos dados a serem inseridos
data1 = {
    'id_reservation': [119397],
    'is_canceled': [0],
    'arrival_date_year': [2023],
    'arrival_date_month': ['December'],
    'agent': [48],
    'previous_cancellations': [1],
    'previous_bookings_not_canceled': [0],
    'country': ['PRT'],
    'deposit_type': ['No Deposit'],
    'reservation_status': ['Check-Out']
}

data2 = {
    'id_reservation': [119397],
    'stays_in_week_nights': [3],
    'stays_in_weekend_nights': [2],
    'adults': [2],
    'children': [1],
    'babies': [1],
    'required_car_parking_spaces': [1],
    'meal': ['BB'],
    'reserved_room_type': ['B'],
    'total_of_special_requests': [2]
}

# Inserção de dados nas tabelas 'reservas_status' e 'stays_info' usando SQLAlchemy
df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)

try:
    df1.to_sql('reservas_status', con=engine, if_exists='append', index=False)
    df2.to_sql('stays_info', con=engine, if_exists='append', index=False)
    print("Novos dados inseridos com sucesso.")
except exc.IntegrityError as e:
    print(f"Erro:  O registro já existe na tabela.")
 
end_time = time.time()
execution_time = end_time - start_time
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")

#Criação indices
mycursor = mydb.cursor()

# Índice para a tabela Main_Info
mycursor.execute("CREATE INDEX idx_id_reservation_main ON main_info(id_reservation)")

# Índice para a tabela Reservas_Status
mycursor.execute("CREATE INDEX idx_id_reservation_reservas ON reservas_status(id_reservation)")

# Índice para a tabela Stays_Info
mycursor.execute("CREATE INDEX idx_id_reservation_stays ON stays_info(id_reservation)")

# Confirmar as alterações no banco de dados
mydb.commit()
print("Índices criados com sucesso!")
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")

# Close the connection to the database
mydb.close()


