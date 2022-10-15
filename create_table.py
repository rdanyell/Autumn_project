from sqlite3 import Timestamp

CREATE TABLE IF NOT EXISTS dim_drivers(
                            personnel_num SERIAL,
                            start_dt TIMESTAMP,
                            last_name VARCHAR (20),
                            first_name VARCHAR (20),
                            middle_name VARCHAR (20),
                            birth_dt DATE,
                            card_num CHAR (19),
                            driver_license_num CHAR(12),
                            driver_license_dt TIMESTAMP,
                            deleted_flag CHAR (1),
                            end_dt TIMESTAMP,
                            PRIMARY KEY (personnel_num, start_dt),
                            UNIQUE (personnel_num)
                            );

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
                            ride_end_dt TIMESTAMP (0),
                            PRIMARY KEY (ride_id));

CREATE TABLE IF NOT EXISTS fact_waybills(
                            waybill_num VARCHAR (10) PRIMARY KEY,
                            driver_pers_num INT,
                            car_plate_num CHAR (9),
                            work_start_dt TIMESTAMP,
                            work_end_dt TIMESTAMP,
                            issue_dt TIMESTAMP);

CREATE TABLE IF NOT EXISTS dim_cars(
                            plate_num CHAR (9),
                            start_dt TIMESTAMP,
                            model_name VARCHAR(30),
                            revision_dt DATE,
                            deleted_flag CHAR (1),
                            end_dt TIMESTAMP,
                            UNIQUE (plate_num),
                            PRIMARY KEY (plate_num, start_dt));

CREATE TABLE IF NOT EXISTS dim_clients (
                            phone_num CHAR (18),
                            start_dt TIMESTAMP,
                            card_num CHAR(19),
                            deleted_flag CHAR (1),
                            end_dt TIMESTAMP,
                            UNIQUE (phone_num),
                            PRIMARY KEY (phone_num, start_dt));

CREATE TABLE IF NOT EXISTS fact_payments (
                            transaction_id SERIAL PRIMARY KEY,
                            card_num VARCHAR,
                            transaction_amt FLOAT,
                            transaction_dt TIMESTAMP);



ALTER TABLE fact_rides
    ADD CONSTRAINT fk_drivers
    FOREIGN KEY (driver_pers_num)
    REFERENCES  dim_drivers(personnel_num),

    ADD CONSTRAINT fk_cars
    FOREIGN KEY (car_plate_num)
    REFERENCES dim_cars(plate_num),

    ADD CONSTRAINT fk_clients
    FOREIGN KEY (client_phone_num)
    REFERENCES dim_clients(phone_num);

ALTER TABLE fact_waybills
    ADD CONSTRAINT fk_friver
    FOREIGN KEY (driver_pers_num)
    REFERENCES dim_drivers (personnel_num),

    ADD CONSTRAINT  fk_carsbill
    FOREIGN KEY (car_plate_num)
    REFERENCES dim_cars (plate_num);