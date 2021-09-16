
import sqlite3
import os 
import datetime
from copy import deepcopy
import json
from db_helpers import *

# def long_to_datetime_str(long, utc=True):
#     if utc is True:
#         return datetime.datetime.fromtimestamp(long/1000, tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
#     else:
#         return datetime.datetime.fromtimestamp(long/1000).strftime('%Y-%m-%d %H:%M:%S')


class funding_db():
    
    def __init__(self, DB_DIRECTORY, DB_NAME, SYMBOL, TYPE, EXCHANGE, **kwargs):

        self.symbol = SYMBOL
        self.type = TYPE
        self.exchange = EXCHANGE
        
        if not os.path.exists(DB_DIRECTORY):
            print(f"CREATING DIRECTORY: {DB_DIRECTORY}")
            os.makedirs(DB_DIRECTORY)

        self.con = sqlite3.connect(DB_DIRECTORY+DB_NAME)
        self.cur = self.con.cursor()

        self.create_candle_table()
        self.create_info_table()
        self.create_view()


    def get_last(self):
        row = self.query(f"SELECT funding FROM FUNDING_TABLE WHERE funding_time=(SELECT last_funding_time FROM LAST_INSERT_VIEW)") 
        if len(row)>1:
            print('TOO MANY MATCHING ENTRIES - SOMETHING IS WRONG? ')

        if len(row)==0:
            print('no matches')
            return {'fundingTime':1}
        return json.loads(row[0][0])

    def delete_last(self):
        self.execute(f"DELETE FROM FUNDING_TABLE WHERE funding_time=(SELECT last_funding_time FROM LAST_INSERT_VIEW)") 
       
    def create_candle_table(self):
        create_table = f'''CREATE TABLE IF NOT EXISTS FUNDING_TABLE
                           (funding TEXT, funding_time INT, funding_time_string TEXT, funding_rate TEXT, symbol TEXT, insert_timestamp TEXT)'''
        self.cur.execute(create_table)
        self.con.commit()

    def create_info_table(self):
        info_table = self.query("SELECT name FROM sqlite_master WHERE type='table' AND name='INFO_TABLE'")
        if len(info_table)>0:
            q = self.query("""SELECT * FROM INFO_TABLE""")
            if q[0]!=(self.exchange, self.symbol, self.type):
                raise Exception(f'funding_db - INFO_TABLE: {q[0]}, NEW PARAMETERS: {(self.exchange, self.symbol, self.type)}')

        if len(info_table)==0:
            create_table = f'''CREATE TABLE IF NOT EXISTS INFO_TABLE
                            (exchange TEXT, symbol TEXT, type TEXT)'''
            self.cur.execute(create_table)
            self.cur.executemany(f'INSERT INTO INFO_TABLE VALUES (?,?,?)', [[self.exchange, self.symbol, self.type]])
            self.con.commit()

    def create_view(self):
        self.execute(f"""
            CREATE VIEW IF NOT EXISTS LAST_INSERT_VIEW AS SELECT 
            MAX(funding_time)  AS last_funding_time, 
            MAX(funding_time_string)  AS last_funding_time_string,
            MIN(funding_time)  AS first_entry_time, 
            MIN(funding_time_string)  AS first_entry_time_string,    
            (COUNT(funding_time)  - COUNT(DISTINCT(funding_time))) AS num_duplicates,
            (COUNT (funding_time)) AS num_total_rows FROM FUNDING_TABLE          
            """)
        
    def insert_multiple(self, insert_list): 
        if len(insert_list)==0:
            print(f"FUNDING_TABLE ({self.symbol}) - insert list is EMPTY" )
            return

        insert_list=deepcopy(insert_list)
        insert_list = [[s] for s in insert_list]
        inserted_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        for i in range(len(insert_list)):
            funding_time = insert_list[i][0]['fundingTime']
            funding_rate = insert_list[i][0]['fundingRate']

            insert_list[i].append(funding_time)
            insert_list[i].append(long_to_datetime_str(funding_time))
            insert_list[i].append(funding_rate)

            insert_list[i].append(self.symbol)    

            insert_list[i].append(inserted_at)
            insert_list[i][0]=json.dumps(insert_list[i][0])

        # INSERT AND COMMIT
        self.cur.executemany(f'INSERT INTO FUNDING_TABLE VALUES (?,?,?,?,?,?)', insert_list)
        self.con.commit()
        print(f"FUNDING_TABLE ({self.symbol}) - {len(insert_list)} entries ({insert_list[0][2]} to {insert_list[-1][2]}) - inserted at {inserted_at}")

    def query(self, query):
        self.cur.execute(query)
        rows = self.cur.fetchall()
        return rows

    def execute(self, query):
        self.cur.execute(query)
        self.con.commit()







# db.insert_multiple(insert_list=btc_avax_funding['BTCUSDT'][-40:])



# db.get_last()
# db.delete_last()








