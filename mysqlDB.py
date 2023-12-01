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
    id_reservation, is_cancelled, arrival_date_year,
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

mycursor.executemany(main_info_insert_query, df_main_info_list)
mycursor.executemany(reservas_status_insert_query, df_reservas_status_list)
mycursor.executemany(stays_info_insert_query, df_stays_info_list)
mydb.commit()


# Close the connection to the database
mydb.close()


