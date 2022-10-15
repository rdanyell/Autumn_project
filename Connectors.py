from ftplib import FTP_TLS
import psycopg2

class FtpsConnector:
    ftps = FTP_TLS()

    def __init__(self, params={}) -> None:
        print("FTPs connection status:", self.ftps.connect(
            host=params.get("host_name"), port=21, timeout=10))                           # Connect to host.
        print("FTPs connection status:", self.ftps.auth())                                # Set up secure control connection by using TLS/SSL
        print("FTPs connection status:", self.ftps.prot_p())                              # Set up secure data connection.
        print("FTPs connection status:", self.ftps.login(
            user=params.get("user_name"),
            passwd=params.get("password")))
        if self.ftps.voidcmd("NOOP")[:3] == "200":
            print(f'FTPs connection status: {params.get("host_name")} are now connected')
        else:
            print("FTPs connection status: not connected")

class PosrtgresConnector:
    connection = None
    
    def __init__(self, params={}) -> None:
        self.connection = psycopg2.connect(
            database=params.get("db_name"),
            user=params.get("db_user"),
            password=params.get("db_password"),
            host=params.get("db_host"),
            port=params.get("db_port"),
            
        )
        if self.connection.status == 1:
            print(f'DB connection status: {self.connection.get_dsn_parameters()} are now connected')
        else:
            print('DB connection status: not connected')
