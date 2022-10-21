from numpy import empty
import pandas as pd
import sqlalchemy
from Connectors import PosrtgresConnector as PG_C
from info import db_out_params, create_tables, db_in_params
import pandas.io.sql as psql
from datetime import datetime

class FactRiders:

    #создаем необходимые таблицы в целевом хранилище данных
    def create_tables(): 
        engine_out = sqlalchemy.create_engine("postgresql://dwh_barnaul:dwh_barnaul_YHQdvxan@de-edu-db.chronosavant.ru:5432/dwh")
        result = engine_out.execute(create_tables)


    #загрузка данных из источника, проверка на новые строки, изменения 
    # (добавляет измененные строки, но не заполняет поле end_dt у предыдущего значения, при этом отдельно такой запрос в базе изменения вносит), 
    # также на удаленные строки (строки находит, но не ставит флаги)
    def increment_load_new_data(engine_in, engine_out, source_upd_df, tbl_name, key, pk, target):
    
        
        target.apply(tuple, 1)
        source_upd_df.apply(tuple,1).isin(target.apply(tuple,1))
        changes = source_upd_df[~source_upd_df.apply(tuple,1).isin(target.apply(tuple,1))]
        inserts = changes[~changes[key].isin(target[key])]
        inserts.to_sql(tbl_name, engine_out, if_exists='append', index=False)
        modified = changes[changes[key].isin(target[key])]
        if modified is not empty:
            # print('This lines are modified')
            # print(modified.head())
            # for i in range(len(modified)):
            #     date = str(modified['start_dt'].loc[i])
            #     line = modified[key].loc[i]
            #     statement = "WITH subquery AS (SELECT * FROM " + tbl_name +" WHERE " + key + " = '" + line + "') UPDATE " + tbl_name + " SET end_dt = '" + date + "' WHERE " + pk  +" = (SELECT MAX(subquery."+ pk+") FROM subquery);"
            #     print(statement)
            #     engine_out.execute(statement)    
            #     print('statement executed')
            modified.to_sql(tbl_name, engine_out, if_exists='append', index=False)
        # deleted = target[~target.apply(tuple,1).isin(source_upd_df.apply(tuple,1))]
        # print('This lines are deleted from source')
        # print(deleted.head())   

    def load_csv(engine_in, engine_out):
        custom_date_parser = lambda x: datetime.strptime(x, "%d %m %Y %H:%M:%S")
        colnames_payments=['datetime', 'card_num', 'transaction_amt']
        
        payments_df = pd.read_csv('total_payments.csv', sep ='\t', names=colnames_payments, header=None,  
            date_parser=custom_date_parser)
        format1 ="%d.%m.%Y %H:%M:%S"
        format2 = "%Y-%m-%d %H:%M:%S"
        payments_df['datetime'] = payments_df['datetime'].apply(lambda x: datetime.strptime(x, format1).strftime(format2)) 
        payments_df['transaction_id'] = payments_df.index
        payments_df = payments_df[['transaction_id', 'card_num', 'transaction_amt', 'datetime']]
        payments_df = payments_df.rename(columns={'datetime': 'transaction_dt'})
     
        # print(payments_df.head(3))
        source_pay = payments_df 
        pay_tbl_name = "fact_payments"
        pay_key = "transaction_id"
        pay_pk = "transaction_id"
        pay_target = pd.read_sql('Select transaction_id, card_num, transaction_amt, transaction_dt \
            FROM dwh.dwh_barnaul."fact_payments"', engine_out)

        FactRiders.increment_load_new_data(engine_in, engine_out, source_upd_df = source_pay,  \
            tbl_name = pay_tbl_name, key = pay_key, pk = pay_pk, target = pay_target)