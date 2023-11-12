DROP TABLE IF EXISTS Tabela1
DROP TABLE IF EXISTS Tabela2
DROP TABLE IF EXISTS Tabela3


CREATE TABLE Tabela1(
    id_reservation int NOT NULL,
    hotel VARCHAR(255) NOT NULL,
    lead_time int NOT NULL,
    arrival_date_year int NOT NULL, 
    arrival_date_month int NOT NULL,
    arrival_date_week_number int NOT NULL,
    arrival_date_day_of_month int NOT NULL,
    country VARCHAR(255) NOT NULL,
    market_segment VARCHAR(255) NOT NULL
    distribution_channel VARCHAR NOT NULL,
    is_repeated_guest bit NOT NULL,
    booking_changes int NOT NULL,
    days_in_waiting_list int NOT NULL,
    company int,
    customer_type VARCHAR(255) NOT NULL,
    reservation_status_date DATE NOT NULL


    PRIMARY KEY (id_reservation),

);

CREATE TABLE Tabela2(
   id_reservation INT NOT NULL,
   is_canceled INT NOT NULL,
   arrival_date_year INT NOT NULL,
   arrival_date_month VARCHAR(255) NOT NULL,
   agent INT,
   previous_cancellations INT NOT NULL,
   previous_bookings_not_canceled INT NOT NULL,
   country VARCHAR(255) NOT NULL,
   deposit_type VARCHAR(255) NOT NULL,
   reservation_status VARCHAR(255) NOT NULL,

   PRIMARY KEY(id_reservation),
   FOREIGN KEY(id_reservation) REFERENCES Tabela1(id_reservation),
);

CREATE TABLE Tabela3 (
    id_reservation INT NOT NULL,
    stays_in_week_nights INT NOT NULL,
    stays_in_weekend_nights INT NOT NULL,
    adults INT NOT NULL,
    children FLOAT NOT NULL,
    babies INT NOT NULL,
    required_car_parking_spaces INT NOT NULL,
    meal VARCHAR(20) NOT NULL,
    reserved_room_type INT NOT NULL,
    total_of_special_requests INT NOT NULL
    PRIMARY KEY(id_reservation),
    FOREIGN KEY (id_reservation) REFERENCES Tabela1(id_reservation)
);




