from Connectors import FtpsConnector as FTPs_C
import fact_riders_handle as frh
# from Connectors import PosrtgresConnector as PG_C
# import pandas as pd

def main():
    ftp_params = {
        "host_name": "de-edu-db.chronosavant.ru",
        "user_name": "etl_tech_user",
        "password": "etl_tech_user_password"
    }
    
    
    
    # try:
    # ftp_cnnctn = FTPs_C(ftp_params)                     # open ftp connection
    # r = ftp_cnnctn.ftps.nlst()
        
        
        
        # print(r[0])
        # with open(r[0])
        # ftp_cnnctn.ftps.cwd(r[1])
        # f = ftp_cnnctn.ftps.nlst()
        # # print(f[0])
        # local_filename = 'current_waybils.xml'
        # with open(local_filename, 'wb') as fin:
        #     ftp_cnnctn.ftps.retrbinary("RETR " + f[0], fin.write)
        
        # # ftp_cnnctn.ftps.cwd(r[1])
        # # print(r[1])
        
        # # with open(f[0], 'r') as fin:
        # #     pd.read_csv(fin)
        # # ftp_cnnctn.ftps.dir()
    # ftp_cnnctn.ftps.quit()                          # close connection and quit

    # pstgrs_cnnctn = PG_C(db_params)
    # pstgrs_cnnctn.connection.close()
    # except Exception as err:
    #     print(err)
    
    # fact_riders_db = frh.FactRiders.first_start()
    frh.FactRiders.first_fill_from_in()
    
    
if __name__ == "__main__":
    main()