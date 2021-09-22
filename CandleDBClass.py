import sqlite3
import os 
import datetime
from copy import deepcopy
import json
from db_helpers import long_to_datetime_str

class candle_db():
    
    def __init__(self, DB_DIRECTORY, DB_NAME, SYMBOL, INTERVAL, TYPE, EXCHANGE, **kwargs):
        try: 
            self.symbol = SYMBOL
            self.interval = INTERVAL
            self.type = TYPE
            self.exchange = EXCHANGE

            # print(DB_DIRECTORY+DB_NAME)
            # print(self.symbol)
            # print(self.interval)
            # print(self.type)
            # print(self.exchange)             
            
            if not os.path.exists(DB_DIRECTORY):
                print(f"CREATING DIRECTORY: {DB_DIRECTORY}")
                os.makedirs(DB_DIRECTORY)

            self.con = sqlite3.connect(DB_DIRECTORY+DB_NAME, timeout=60)
            self.cur = self.con.cursor()
            
            # print('before info table')
            self.create_info_table()
            # print('before candle table')            
            self.create_candle_table()
            # print('before view')                        
            self.create_view()
            # print('after view') 
            # print('*************************************************************************** ')

        except Exception as e: 
            # input('hee')
            print(e)
            print(type(e))
            print(DB_DIRECTORY+DB_NAME)
            print(self.symbol)
            print(self.interval)
            print(self.type)
            print(self.exchange) 
            print('...............')
            raise e           


    def get_last(self):
        print(f'CandleDBClass.get_last() {self.symbol}, {self.type}, {self.interval}, {self.exchange}')
        row = self.query(f"SELECT candle FROM CANDLE_TABLE WHERE open_time=(SELECT last_open FROM LAST_INSERT_VIEW)") 
        if len(row)>1:
            print(f'CandleDBClass.get_last - {self.symbol}: {len(row)} MATCHING ENTRIES - SOMETHING IS WRONG? SHOULD BE JUST ONE')

        if len(row)==0:
            # print('not enough entries ')
            return None

        return json.loads(row[0][0])


    def get_first(self):
        row = self.query(f"SELECT candle FROM CANDLE_TABLE WHERE open_time=(SELECT first_open FROM LAST_INSERT_VIEW)") 
        if len(row)>1:
            print(f'CandleDBClass.get_first - {self.symbol}: {len(row)} MATCHING ENTRIES - SOMETHING IS WRONG? SHOULD BE JUST ONE')

        if len(row)==0:
            # print('not enough entries ')
            return None

        return json.loads(row[0][0])        

    def delete_last(self):
        self.execute(f"DELETE FROM CANDLE_TABLE WHERE open_time=(SELECT last_open FROM LAST_INSERT_VIEW)") 
       
    def create_candle_table(self):
        create_table = f'''CREATE TABLE IF NOT EXISTS CANDLE_TABLE
                           (candle TEXT, open_time_string TEXT, close_time_string TEXT, open_time INT, close_time INT, symbol TEXT, interval TEXT, type TEXT, insert_timestamp TEXT)'''
        self.cur.execute(create_table)
        self.con.commit()

    def create_info_table(self):
        info_table = self.query("SELECT name FROM sqlite_master WHERE type='table' AND name='INFO_TABLE'")
        if len(info_table)>0:
            q = self.query("""SELECT * FROM INFO_TABLE""")
            if q[0]!=(self.exchange,self.symbol,self.interval, self.type,):        
                raise Exception(f'INFO_TABLE: {q[0]}, NEW PARAMETERS: {(self.exchange,self.symbol,self.interval, self.type,)}')

        elif len(info_table)==0:
            create_table = f'''CREATE TABLE IF NOT EXISTS INFO_TABLE
                            (exchange TEXT, symbol TEXT, interval TEXT, type TEXT)'''
            self.cur.execute(create_table)
            self.cur.executemany(f'INSERT INTO INFO_TABLE VALUES (?,?,?,?)', [[self.exchange, self.symbol, self.interval, self.type]])
            self.con.commit()

    # def create_info_table(self):
    #     info_table = self.query("SELECT name FROM sqlite_master WHERE type='table' AND name='INFO_TABLE'")
    #     if len(info_table)==0:
    #         create_table = f'''CREATE TABLE IF NOT EXISTS INFO_TABLE
    #                         (exchange TEXT, symbol TEXT, type TEXT)'''
    #         self.cur.execute(create_table)
    #         self.cur.executemany(f'INSERT INTO INFO_TABLE VALUES (?,?)', [[self.exchange, self.symbol, self.type]])
    #         self.con.commit()            

    def create_view(self):
        self.execute(f"""
            CREATE VIEW IF NOT EXISTS LAST_INSERT_VIEW AS SELECT 
            MAX(open_time)  AS last_open , 
            MAX(close_time) AS last_close, 
            MAX(open_time_string)  AS last_open_string, 
            MAX(close_time_string) AS last_close_string,
            MIN(open_time)  AS first_open , 
            MIN(close_time) AS first_close, 
            MIN(open_time_string)  AS first_open_string, 
            MIN(close_time_string) AS first_close_string, 
            (COUNT(open_time)  - COUNT(DISTINCT(open_time))) AS num_duplicates,
            (COUNT (open_time)) AS num_total_rows FROM CANDLE_TABLE
            """)
        
    def insert_multiple(self, insert_list): 
        if len(insert_list)==0:
            return
        insert_list=deepcopy(insert_list)
        insert_list = [[s] for s in insert_list]
        inserted_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        for i in range(len(insert_list)):
            open_time = insert_list[i][0][0]
            close_time = insert_list[i][0][6]
            insert_list[i].append(long_to_datetime_str(open_time))
            insert_list[i].append(long_to_datetime_str(close_time))

            insert_list[i].append(open_time)
            insert_list[i].append(close_time)


            insert_list[i].append(self.symbol)    
            insert_list[i].append(self.interval)                    
            insert_list[i].append(self.type)
            insert_list[i].append(inserted_at)
            insert_list[i][0]=json.dumps(insert_list[i][0])

        # INSERT AND COMMIT
        self.cur.executemany(f'INSERT INTO CANDLE_TABLE VALUES (?,?,?,?,?,?,?,?,?)', insert_list)
        self.con.commit()
        print(f"CANDLE_TABLE ({self.symbol} {self.type}) -  {insert_list[0][1]}-{insert_list[-1][1]}  {len(insert_list)} entries inserted at {inserted_at}")

    def query(self, query):
        self.cur.execute(query)
        rows = self.cur.fetchall()
        return rows

    def execute(self, query):
        self.cur.execute(query)
        self.con.commit()

    def close_connection(self,):
        self.con.close()



# DB_DIRECTORY='/home/cm/Documents/PY_DEV/BINANCE_CANDLEGRAB/db/'

# DB_NAME ='bin_test4.db' 
# TABLE_NAME = 'test'


# db=candle_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME=DB_NAME, SYMBOL='test_symbol', INTERVAL='5m', TYPE='FUTS', EXCHANGE='binance')



# db.insert_multiple(insert_list=test['BTCUSDT'])

# db.get_last()
# db.delete_last()


# a=2
# s= db.query(f"SELECT * FROM CANDLE_TABLE ORDER BY open_time LIMIT {a}")

# s[0][1]
# s[-1][1]


# [json.loads(d[0]) for d in s]  
# s
# s[0][0]
# s
# # # db.create_table(table_name=TABLE_NAME)

# # # CREATE VIEW 
# # db.execute(f"CREATE VIEW IF NOT EXISTS LAST_sUPDATE_sVIEW AS SELECT MAX(open_time) AS last_open , MAX(close_time) AS last_close FROM {TABLE_NAME}")

# db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='CANDLE_TABLE'")

# s=db.query(f"SELECT candle FROM CANDLE_TABLE WHERE open_time=(SELECT last_open FROM LAST_INSERT_VIEW)") 
# json.loads(s[0][0])

# ### DELETE THE LAST INSERT: 
# db.execute(f"DELETE FROM {TABLE_NAME} WHERE open_time=(SELECT last_open FROM LAST_INSERT_VIEW)")




