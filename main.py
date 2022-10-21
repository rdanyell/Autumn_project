from Connectors import FtpsConnector as FTPs_C
import fact_riders_handle as frh
import sqlalchemy
import pandas as pd
import pandas.io.sql as psql
from info import ftp_params
from info import db_out_params, create_tables, db_in_params
from ftp_handle import Ftp_handler

# from Connectors import PosrtgresConnector as PG_C
# import pandas as pd

def main():

    frh.FactRiders.create_tables()
    csv_files = Ftp_handler()
    csv_files.csv_handler_first_call()
    csv_files.xml_handler_first_call()
    
     
    engine_in = sqlalchemy.create_engine('postgresql://etl_tech_user:etl_tech_user_password@de-edu-db.chronosavant.ru:5432/taxi')
    engine_out = sqlalchemy.create_engine('postgresql://dwh_barnaul:dwh_barnaul_YHQdvxan@de-edu-db.chronosavant.ru:5432/dwh')
    driver_source_upd = psql.read_sql('SELECT update_dt AS start_dt, last_name, first_name, middle_name, birth_dt, card_num, \
        driver_license AS driver_license_num, driver_valid_to AS driver_license_dt FROM taxi.main.drivers', engine_in)
    driver_tbl_name = "dim_drivers"
    driver_key = "driver_license_num"
    driver_pk = "personnel_num"
    driver_target = pd.read_sql('Select start_dt, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, \
        driver_license_dt FROM dwh.dwh_barnaul."dim_drivers"', engine_out)

    cars_source_upd = psql.read_sql('SELECT plate_num, register_dt as start_dt, model AS model_name, revision_dt, \
        finished_flg AS deleted_flag FROM taxi.main.car_pool', engine_in)
    cars_tbl_name = "dim_cars"
    cars_key = "plate_num"
    cars_pk = "plate_num"
    cars_target = pd.read_sql('Select plate_num, start_dt, model_name, revision_dt, deleted_flag FROM dwh.dwh_barnaul."dim_cars"', engine_out)


    frh.FactRiders.increment_load_new_data(engine_in, engine_out, source_upd_df = driver_source_upd, tbl_name=driver_tbl_name, key = driver_key, pk = driver_pk, target = driver_target)
    frh.FactRiders.increment_load_new_data(engine_in, engine_out, source_upd_df = cars_source_upd, tbl_name=cars_tbl_name, key = cars_key, pk = cars_pk, target = cars_target)
    frh.FactRiders.load_csv(engine_in, engine_out)
    
    
if __name__ == "__main__":
    main()