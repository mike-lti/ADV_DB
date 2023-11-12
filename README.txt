Dataset: https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand/data
Lines: 119390 Lines

Table1:{
    name: Main_Info
    colunas: hotel, lead_time, arrival_date_year, arrival_date_month, arrival_date_week_number,
    arrival_date_day_of_month, country, market_segment, distribution_channel, is_repeated_guest, booking_changes, days_in_waiting_list, company, customer_type, reservation_status_date

}


Ver os meses e anos das reserva mais canceladas, comparar também pessoas de agencia cancelam ou nao as reservas, checking wich country cancells aswell as the deposit type
Table2:{
    name: Reservas Status
    columns: is_cancelled, arrival_date_year, arrival_date_month, angent, previous_cancellations, previous_bookings_not_cancelled, country, deposit_type, reservation_status
}


Comparar o tempo da estadia de familias com filhos e sem filhos e ver se requisitam parque de estacionamento para carro
Table3:{
    name:
    columns: stays_in_week_nights, stays_in_weekends_nights, adult, children, babies, required_car_parking_spaces, meal, reserved_room_type, total_of_special_requests
}