import sqlalchemy
import pandas as pd
import pandas.io.sql as psql

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

ftp_params = {
    "host_name": "de-edu-db.chronosavant.ru",
    "user_name": "etl_tech_user",
    "password": "etl_tech_user_password"
}

create_tables = """
CREATE TABLE IF NOT EXISTS dim_drivers(
                            personnel_num SERIAL,
                            start_dt TIMESTAMP(0),
                            last_name VARCHAR (20),
                            first_name VARCHAR (20),
                            middle_name VARCHAR (20),
                            birth_dt DATE,
                            card_num CHAR (19),
                            driver_license_num CHAR(12),
                            driver_license_dt DATE,
                            deleted_flag CHAR (1),
                            end_dt TIMESTAMP(0));

CREATE TABLE IF NOT EXISTS fact_rides(
                            ride_id INTEGER,
                            point_from_txt VARCHAR (200),
                            point_to_txt VARCHAR (200),
                            distance_val NUMERIC (5, 2),
                            price_amt NUMERIC (7, 2),
                            client_phone_num CHAR (18),
                            driver_pers_num INT,
                            car_plate_num CHAR (9),
                            ride_arrival_dt TIMESTAMP (0),
                            ride_start_dt TIMESTAMP (0),
                            ride_end_dt TIMESTAMP (0));

CREATE TABLE IF NOT EXISTS fact_waybills(
                            waybill_num VARCHAR (10),
                            driver_pers_num INT,
                            car_plate_num CHAR (9),
                            work_start_dt TIMESTAMP(0),
                            work_end_dt TIMESTAMP(0),
                            issue_dt TIMESTAMP(0));

CREATE TABLE IF NOT EXISTS dim_cars(
                            plate_num CHAR (9),
                            start_dt TIMESTAMP(0),
                            model_name VARCHAR(30),
                            revision_dt DATE,
                            deleted_flag CHAR (1),
                            end_dt TIMESTAMP(0));

CREATE TABLE IF NOT EXISTS dim_clients (
                            phone_num CHAR (18),
                            start_dt TIMESTAMP(0),
                            card_num CHAR(19),
                            deleted_flag CHAR (1),
                            end_dt TIMESTAMP(0));

CREATE TABLE IF NOT EXISTS fact_payments (
                            transaction_id INT,
                            card_num VARCHAR,
                            transaction_amt FLOAT,
                            transaction_dt TIMESTAMP(0));
"""
