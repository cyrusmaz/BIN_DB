
import sqlite3
import os 
import datetime
from copy import deepcopy
import json
from db_helpers import *



class oi_db():
    
    def __init__(self, DB_DIRECTORY, DB_NAME, SYMBOL,INTERVAL, TYPE, EXCHANGE, **kwargs):

        self.symbol = SYMBOL
        self.type = TYPE
        self.exchange = EXCHANGE
        self.interval = INTERVAL
        
        if not os.path.exists(DB_DIRECTORY):
            print(f"CREATING DIRECTORY: {DB_DIRECTORY}")
            os.makedirs(DB_DIRECTORY)

        self.con = sqlite3.connect(DB_DIRECTORY+DB_NAME)
        self.cur = self.con.cursor()

        self.create_candle_table()
        self.create_info_table()
        self.create_view()


    def get_last(self):
        row = self.query(f"SELECT oi FROM OI_TABLE WHERE oi_time=(SELECT last_oi_time FROM LAST_INSERT_VIEW)") 
        if len(row)>1:
            print(f'OIDBClass.get_last - {self.symbol}: {len(row)} MATCHING ENTRIES - SOMETHING IS WRONG? SHOULD BE JUST ONE')

        if len(row)==0:
            # print('no matches')
            return None
        return json.loads(row[0][0])


    def get_first(self):
        # row = self.query(f"SELECT candle FROM CANDLE_TABLE WHERE open_time=(SELECT first_open FROM LAST_INSERT_VIEW)") 
        row = self.query(f"SELECT oi FROM OI_TABLE WHERE oi_time=(SELECT first_oi_time FROM LAST_INSERT_VIEW)") 
        if len(row)>1:
            print(f'OIDBClass.get_first - {self.symbol}: {len(row)} MATCHING ENTRIES - SOMETHING IS WRONG? SHOULD BE JUST ONE')

        if len(row)==0:
            # print('not enough entries ')
            return None

        return json.loads(row[0][0])    


    def delete_last(self):
        self.execute(f"DELETE FROM OI_TABLE WHERE oi_time=(SELECT last_oi_time FROM LAST_INSERT_VIEW)") 
       
    def create_candle_table(self):
        create_table = f'''CREATE TABLE IF NOT EXISTS OI_TABLE
                           (oi TEXT, oi_time INT, oi_time_string TEXT, oi_sum TEXT, oi_sum_value TEXT, symbol TEXT, insert_timestamp TEXT)'''
        self.cur.execute(create_table)
        self.con.commit()

    def create_info_table(self):
        info_table = self.query("SELECT name FROM sqlite_master WHERE type='table' AND name='INFO_TABLE'")

        if len(info_table)>0:
            q = self.query("""SELECT * FROM INFO_TABLE""")
            if q[0]!=(self.exchange,self.symbol,self.type,self.interval):
                raise Exception(f'oi_db - INFO_TABLE: {q[0]}, NEW PARAMETERS: {(self.exchange, self.symbol, self.type, self.interval)}')


        if len(info_table)==0:
            create_table = f'''CREATE TABLE IF NOT EXISTS INFO_TABLE
                            (exchange TEXT, symbol TEXT, type TEXT, interval TEXT)'''
            self.cur.execute(create_table)
            self.cur.executemany(f'INSERT INTO INFO_TABLE VALUES (?,?,?,?)', [[self.exchange, self.symbol, self.type, self.interval]])
            self.con.commit()

    def create_view(self):
        self.execute(f"""
            CREATE VIEW IF NOT EXISTS LAST_INSERT_VIEW AS SELECT 
            MAX(oi_time)  AS last_oi_time, 
            MAX(oi_time_string)  AS last_oi_time_string,
            MIN(oi_time)  AS first_oi_time, 
            MIN(oi_time_string)  AS first_oi_time_string,
            (COUNT(oi_time)  - COUNT(DISTINCT(oi_time))) AS num_duplicates,
            (COUNT (oi_time)) AS num_total_rows FROM OI_TABLE            
            """)
        
    def insert_multiple(self, insert_list): 
        if len(insert_list)==0:
            print(f"OI_TABLE ({self.symbol}) - insert list is EMPTY" )
            return

        insert_list=deepcopy(insert_list)
        insert_list = [[s] for s in insert_list]
        inserted_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        for i in range(len(insert_list)):
            oi_time = insert_list[i][0]['timestamp']
            oi_sum = insert_list[i][0]['sumOpenInterest']
            oi_sum_value = insert_list[i][0]['sumOpenInterest']

            insert_list[i].append(oi_time)
            insert_list[i].append(long_to_datetime_str(oi_time))
            insert_list[i].append(oi_sum)
            insert_list[i].append(oi_sum_value)

            insert_list[i].append(self.symbol)    

            insert_list[i].append(inserted_at)
            insert_list[i][0]=json.dumps(insert_list[i][0])

        # INSERT AND COMMIT
        self.cur.executemany(f'INSERT INTO OI_TABLE VALUES (?,?,?,?,?,?,?)', insert_list)
        self.con.commit()
        print(f"OI_TABLE ({self.symbol}) - {len(insert_list)} entries ({insert_list[0][2]} to {insert_list[-1][2]}) - inserted at {inserted_at}")

    def query(self, query):
        self.cur.execute(query)
        rows = self.cur.fetchall()
        return rows

    def execute(self, query):
        self.cur.execute(query)
        self.con.commit()




# DB_DIRECTORY='/home/cm/Documents/PY_DEV/BINANCE_CANDLEGRAB/db/'
# avax=oi_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME='oi.db', SYMBOL='AVAX', TYPE='USDT_FUTS')


# avax.insert_multiple(oi2['BTCUSDT'])


# avax.get_last()

# avax.delete_last()

