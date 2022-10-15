db_in_params = {
        "db_user": "etl_tech_user",
        "db_password": "etl_tech_user_password",
        "db_host": "de-edu-db.chronosavant.ru",
        "db_port": "5432",
        "db_name": "taxi",
        "schema": "main"
    }

db_out_params = {
    "db_name": "dwh",
    "db_user": "dwh_barnaul",
    "db_password": "dwh_barnaul_YHQdvxan",
    "db_host": "de-edu-db.chronosavant.ru",
    "db_port": "5432"
}

create_fact_rides_table = """
CREATE TABLE IF NOT EXISTS fact_rides(
                            ride_id INTEGER PRIMARY KEY,
                            point_from_txt VARCHAR (200),
                            point_to_txt VARCHAR (200),
                            distance_val NUMERIC (5, 2),
                            price_amt NUMERIC (7, 2),
                            client_phone_num CHAR (18),
                            driver_pers_num CHAR (12),
                            car_plate_num CHAR (9),
                            ride_arrival_dt TIMESTAMP (0),
                            ride_start_dt TIMESTAMP (0),
                            ride_end_dt TIMESTAMP (0));
"""