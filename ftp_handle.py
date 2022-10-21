import os
import xml.etree.ElementTree as ET
import pandas as pd
from info import ftp_params
from Connectors import FtpsConnector as FTPs_C

class Ftp_handler:
    ftp_cnnctn = None
    
    def csv_handler_first_call(self):
        if os.path.exists("total_payments.csv"):
            os.remove("total_payments.csv")
        self.ftp_cnnctn = FTPs_C(ftp_params)                 # open ftp connection                 
        dirs = self.ftp_cnnctn.ftps.nlst()
        self.ftp_cnnctn.ftps.cwd(dirs[0])
        folders = self.ftp_cnnctn.ftps.nlst()
        
        local_filename = 'current_payments.tsv'
        total_filename = 'total_payments.csv'
        with open(total_filename, 'a') as fin:
            for i in folders:
                with open(local_filename, 'wb') as fin_local:
                    self.ftp_cnnctn.ftps.retrbinary("RETR " + i, fin_local.write)
                with open(local_filename, 'r') as fin_local:
                    fin.write(fin_local.read())
        self.ftp_cnnctn.ftps.quit()                         # close ftp connection
            
    def xml_handler_first_call(self):
        if os.path.exists("total_waybill.csv"):
            os.remove("total_waybill.csv")
        self.ftp_cnnctn = FTPs_C(ftp_params)                 # open ftp connection
        dirs = self.ftp_cnnctn.ftps.nlst()
        self.ftp_cnnctn.ftps.cwd(dirs[1])
        folders = self.ftp_cnnctn.ftps.nlst()
        
        local_filename = 'current_waybill.xml'
        total_filename = 'total_waybill.csv'

        with open(total_filename, 'a') as fin:
            for i in folders:
                with open(local_filename, 'wb') as fin_local:
                    self.ftp_cnnctn.ftps.retrbinary("RETR " + i, fin_local.write)
                with open(local_filename, 'r') as fin_local:
                    tree = ET.parse(fin_local)
                    root = tree.getroot()
                    for child in root:
                        r=[child.attrib["number"], child.attrib["issuedt"], root[0][0].text, root[0][1].text]
                        for element in child:
                            for element in element:
                                new=element.text
                                r.append(new)
                str = ''
                for i in r:
                    str = str + i + ';'
                str = str + "\n"
                fin.write(str)
                print (r) 
        self.ftp_cnnctn.ftps.quit()                         # close ftp connection
       