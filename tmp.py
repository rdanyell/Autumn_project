# from psycopg2 import OperationalError
import psycopg2

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except psycopg2.OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except psycopg2.OperationalError as e:
        print(f"The error '{e}' occurred")

connection = create_connection("dwh", "dwh_barnaul", "dwh_barnaul_YHQdvxan", "de-edu-db.chronosavant.ru", "5432"
)

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

create_dim_drivers_table = """
CREATE TABLE IF NOT EXISTS dim_drivers(
                            personnel_num CHAR (12) PRIMARY KEY,
                            start_dt TIMESTAMP(0),
                            last_name VARCHAR (20),
                            first_name VARCHAR (20),
                            middle_name VARCHAR (20),
                            birth_date DATE,
                            card_num CHAR (19),
                            driver_license_dt CHAR (12),
                            deleted_flag CHAR (1),
                            end_dt DATE, 
                            FOREIGN KEY (personnel_num)
                            REFERENCES fact_rides (driver_pers_num));
"""

# execute_query(connection, create_fact_rides_table)
execute_query(connection, create_dim_drivers_table)