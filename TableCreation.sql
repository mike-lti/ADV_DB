DROP TABLE IF EXISTS Tabela1
DROP TABLE IF EXISTS Tabela2
DROP TABLE IF EXISTS Tabela3


CREATE TABLE main_info(
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

    PRIMARY KEY (id_reservation),

);

CREATE TABLE reservas_status(
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

   PRIMARY KEY(id_reservation),
   FOREIGN KEY(id_reservation) REFERENCES Tabela1(id_reservation),
);

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

    PRIMARY KEY(id_reservation),
    FOREIGN KEY (id_reservation) REFERENCES Tabela1(id_reservation)
);




