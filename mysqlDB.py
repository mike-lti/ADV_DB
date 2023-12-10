import mysql.connector
import pandas as pd
from datetime import datetime
import time

# Estabelece a conexão com o MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password='1234'
)

mycursor = mydb.cursor()

# Cria o banco de dados
mycursor.execute('DROP DATABASE IF EXISTS DataBaseProj')
mycursor.execute("CREATE DATABASE DataBaseProj")

# Fecha a conexão com o MySQL
mydb.close()
print('Criação de tabelas na base de dados\n')
# Reabre a conexão com o banco de dados recém-criado
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password='1234',
    database='DataBaseProj'
)
mycursor = mydb.cursor()

# Cria as tabelas
tables_creation_queries = [
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """
]

for query in tables_creation_queries:
    mycursor.execute(query)

# Confirma as alterações no banco de dados
mydb.commit()
print('Inserção de dados na base de dados usando datasets csv\n')
# Leitura dos dados dos arquivos CSV
df_main_info = pd.read_csv("Tabela1.csv")
df_reservas_status = pd.read_csv("Tabela2.csv")
df_stays_info = pd.read_csv("Tabela3.csv")


# Converte DataFrames para listas
df_main_info_list = df_main_info.values.tolist()
df_reservas_status_list = df_reservas_status.values.tolist()
df_stays_info_list = df_stays_info.values.tolist()

# Executa consultas de inserção
main_info_insert_query = """
INSERT INTO Main_Info (
    hotel, lead_time, arrival_date_year, arrival_date_month,
    arrival_date_week_number, arrival_date_day_of_month,
    country, market_segment, distribution_channel,
    is_repeated_guest, booking_changes, days_in_waiting_list,
    company, customer_type, reservation_status_date
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

reservas_status_insert_query = """
INSERT INTO Reservas_Status (
    id_reservation, is_canceled, arrival_date_year,
    arrival_date_month, agent, previous_cancellations,
    previous_bookings_not_canceled, country, deposit_type,
    reservation_status
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

stays_info_insert_query = """
INSERT INTO Stays_Info (
    id_reservation, stays_in_week_nights,
    stays_in_weekend_nights, adults, children, babies,
    required_car_parking_spaces, meal, reserved_room_type,
    total_of_special_requests
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
insert_queries = [
    main_info_insert_query,
    reservas_status_insert_query,
    stays_info_insert_query
]

lists = [df_main_info_list, df_reservas_status_list, df_stays_info_list]

for query, data_list in zip(insert_queries, lists):
    mycursor.executemany(query, data_list)

# Confirma as alterações no banco de dados
mydb.commit()
print('\n')
print('Quais são os país que tiveram resevas superior a 1000 e, em que ano e mes que isto aconteceu e o hotel que mais requições teve?\n')
sql_consulta = """
    SELECT hotel,country,arrival_date_year AS arrival_year,arrival_date_month AS arrival_month,COUNT(*) AS total_reservas
    FROM main_info GROUP BY hotel, country, arrival_date_year, arrival_date_month HAVING COUNT(*) > 500 ORDER BY total_reservas DESC
"""

# Executa a consulta
explain_query1 = f"{sql_consulta}"
mycursor.execute(explain_query1)
# Obtém os resultados
result = mycursor.fetchall()

# Exibe o resultado
columns = ["hotel", "country", "arrival_year", "arrival_month", "total_reservas"]
df_result = pd.DataFrame(result, columns=columns)
print(df_result)
start_time = time.time()
# Calcula e exibe o tempo de execução
end_time = time.time()
execution_time = end_time - start_time
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")

print('\n')
print('Quais são os país que tiveram resevas alteradas superior a 90 e, em que ano isto ocorreu?\n')
sqlConsulta = """
    SELECT country, arrival_date_year, COUNT(*) AS total_reservas_alteradas
    FROM main_info
    WHERE booking_changes = 0
    GROUP BY arrival_date_year, country
    HAVING COUNT(*) > 90
    ORDER BY arrival_date_year;
"""
# Executa a consulta
explain_query1 = f"{sqlConsulta}"
mycursor.execute(explain_query1)
resultConsulta = mycursor.fetchall()

# Exibe o resultado
columnsConsulta = ["country", "arrival_year", "total_reservas_alteradas"]
df_resultConsulta = pd.DataFrame(resultConsulta, columns=columnsConsulta)
print(df_resultConsulta)

# Calcula e exibe o tempo de execução
end_time = time.time()
execution_time = end_time - start_time
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")
print('how many families with kids cancelled their reservations\n')
# Consulta complexa 1
complex_query1 = """
    SELECT main_info.country, main_info.hotel, stays_info.adults, stays_info.children, stays_info.babies, reservas_status.reservation_status
    FROM main_info
    JOIN stays_info ON main_info.id_reservation = stays_info.id_reservation
    JOIN reservas_status ON main_info.id_reservation = reservas_status.id_reservation
    WHERE stays_info.adults > 0 AND (stays_info.children > 0 OR stays_info.babies > 0) AND reservas_status.reservation_status NOT LIKE 'Canceled';
"""
# Executa a consulta
explain_query1 = f"{complex_query1}"
mycursor.execute(explain_query1)
result_complex_query1 = mycursor.fetchall()
# Crie uma lista de dicionários a partir do resultado
columns_complex_query1 = ["country", "hotel", "adults", "children", "babies", "reservation_status"]
df_result_complex_query1 = pd.DataFrame(result_complex_query1, columns=columns_complex_query1)
print(df_result_complex_query1)
end_time = time.time()
execution_time = end_time - start_time
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")

print('How many families with kids have parking spaces or meals at the hotel\n')
start_time = time.time()
sql_rn_canceladas = """
    SELECT main_info.country, main_info.hotel,stays_info.adults,stays_info.children,stays_info.babies,
    stays_info.required_car_parking_spaces,stays_info.meal
    FROM main_info JOIN stays_info ON main_info.id_reservation = stays_info.id_reservation
    WHERE stays_info.adults > 0 AND (stays_info.children > 0 OR stays_info.babies > 0) AND (stays_info.required_car_parking_spaces > 0 
      OR stays_info.meal NOT LIKE 'Undefined' OR stays_info.meal NOT LIKE 'SC');
"""
explain_query = f"{sql_rn_canceladas}"
mycursor.execute(explain_query)
explain_result = mycursor.fetchall()
# Crie uma lista de dicionários a partir do resultado
columns = ["country", "hotel", "adults", "children", "babies", "car_parking", "meal"]
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
print('\n')
print('Atualização de dados na tabela Main_info\n')
# Atualização de dados na tabela Main_info
novo_hotel = "City Hotel"
start_time = time.time()
sql_update = "UPDATE main_info SET hotel = 'City Hotel' WHERE hotel = '7 Hotel'"
mycursor.execute(sql_update)
mydb.commit()
end_time = time.time()
execution_time = end_time - start_time
print("Atualização concluída com sucesso.")
data_hora_execucao = datetime.now()
print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")
print('\n')
print('Inserção de dados na tabela Main_info\n')
# Inserção de dados na tabela Main_info
try:
    start_time = time.time()
 

    data_nova_reserva = {
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

    df_nova_reserva = pd.DataFrame(data_nova_reserva)
    # Criar cursor para operações SQL
    mycursor = mydb.cursor()
    # Iterar sobre as linhas do DataFrame e executar INSERT para cada linha
    for index, row in df_nova_reserva.iterrows():
        insert_query = """
        INSERT INTO main_info (
            hotel, lead_time, arrival_date_year, arrival_date_month, arrival_date_week_number,
            arrival_date_day_of_month, country, market_segment, distribution_channel,
            is_repeated_guest, booking_changes, days_in_waiting_list, company, customer_type,
            reservation_status_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = tuple(row)
        mycursor.execute(insert_query, values)

    mydb.commit()

    end_time = time.time()
    execution_time = end_time - start_time
    print("Os dados na tabela Main_info foram inseridos com sucesso.")
    data_hora_execucao = datetime.now()
    print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")

except Exception as e:
    print(f"Erro durante a inserção: {str(e)}")

finally:
    # Fechar a conexão com o MySQL
    if mydb.is_connected():
        mycursor.close()
        mydb.close()

print('\n')
print('Inserção de novos dados nas tabelas com chaves estrangeiras (reservas_status e stays_info\n')
# Inserção de novos dados nas tabelas com chaves estrangeiras (reservas_status e stays_info)
mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="DataBaseProj"
    )
try:
    start_time = time.time()

    data_reservas_status = {
        'id_reservation': [119391],
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

    data_stays_info = {
        'id_reservation': [119391],
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

    df_reservas_status_novas = pd.DataFrame(data_reservas_status)
    df_stays_info_novas = pd.DataFrame(data_stays_info)

    # Criar cursor para operações SQL
    mycursor = mydb.cursor()

    # Iterar sobre as linhas do DataFrame e executar INSERT para cada linha (reservas_status)
    for index, row in df_reservas_status_novas.iterrows():
        insert_query_reservas = """
        INSERT INTO reservas_status (
            id_reservation, is_canceled, arrival_date_year, arrival_date_month,
            agent, previous_cancellations, previous_bookings_not_canceled,
            country, deposit_type, reservation_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values_reservas = tuple(row)
        mycursor.execute(insert_query_reservas, values_reservas)

    # Iterar sobre as linhas do DataFrame e executar INSERT para cada linha (stays_info)
    for index, row in df_stays_info_novas.iterrows():
        insert_query_stays = """
        INSERT INTO stays_info (
            id_reservation, stays_in_week_nights, stays_in_weekend_nights,
            adults, children, babies, required_car_parking_spaces,
            meal, reserved_room_type, total_of_special_requests
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values_stays = tuple(row)
        mycursor.execute(insert_query_stays, values_stays)

    mydb.commit()

    end_time = time.time()
    execution_time = end_time - start_time
    print("Novos dados inseridos com sucesso na tabela reservas_status e stays_info.")
    data_hora_execucao = datetime.now()
    print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execução: {data_hora_execucao}")

except Exception as e:
    print(f"Erro durante a inserção: {str(e)}")

finally:
    # Fechar a conexão com o MySQL
    if mydb.is_connected():
        mycursor.close()
        mydb.close()
print('\n')
print('Criação de índices\n')
# Criação de índices
mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="DataBaseProj"
    )
try:
    start_time = time.time()

    mycursor = mydb.cursor()

    # Índice para a tabela Main_Info
    mycursor.execute("CREATE INDEX idx_id_reservation_main ON main_info(id_reservation)")

    # Índice para a tabela Reservas_Status
    mycursor.execute("CREATE INDEX idx_id_reservation_reservas ON reservas_status(id_reservation)")

    # Índice para a tabela Stays_Info
    mycursor.execute("CREATE INDEX idx_id_reservation_stays ON stays_info(id_reservation)")

    # Confirma as alterações no banco de dados
    mydb.commit()
    print("Índices criados com sucesso!")

    end_time = time.time()
    execution_time = end_time - start_time
    data_hora_execucao = datetime.now()
    print(f"Tempo de execução: {execution_time:.2f} segundos.\nData e Hora da execucao: {data_hora_execucao}")

except Exception as e:
    print(f"Erro durante a criação de índices: {str(e)}")
