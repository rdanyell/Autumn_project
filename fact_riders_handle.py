import pandas as pd
# import SQLAlchemy
from Connectors import PosrtgresConnector as PG_C
from info import db_out_params, create_fact_rides_table, db_in_params

class FactRiders:
    def first_start():
        pstgrs_cnnctn = PG_C(db_out_params)
        pstgrs_cnnctn.connection.autocommit = True
        cursor = pstgrs_cnnctn.connection.cursor()
        try:
            cursor.execute(create_fact_rides_table)
            print("Query executed successfully")
        except Exception as err:
            print(err)
        pstgrs_cnnctn.connection.close()

    def first_fill_from_in():
        in_cnnctn = PG_C(db_in_params)
        in_cnnctn.connection.autocommit = True
        out_cnnctn = PG_C(db_out_params)
        out_cnnctn.connection.autocommit = True

        in_cursor = in_cnnctn.connection.cursor()
        out_cursor = out_cnnctn.connection.cursor()
        try:
            in_cursor.execute("""SELECT driver_license, first_name, last_name, 
            middle_name, driver_valid_to, card_num, update_dt, birth_dt FROM taxi.main.drivers""")
            rows = in_cursor.fetchall()
            for row in rows:
               out_cursor.execute("""INSERT INTO dwh.dwh_barnaul.dim_drivers (driver_license_num, 
               first_name, last_name, middle_name, driver_license_dt, card_num, start_dt, birth_dt) VALUES 
               ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % 
               (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
            print("Query executed successfully")
        except Exception as err:
            print(err)
        
        in_cnnctn.connection.close()
        out_cnnctn.connection.close()
        # df = pd.read_sql('select * from riders', con=pstgrs_cnnctn)
         