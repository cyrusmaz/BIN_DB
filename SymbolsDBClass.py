

import sqlite3
import os 
import datetime
from copy import deepcopy
import json
from db_helpers import *
# from datetime import datetime


class symbols_db():
    
    def __init__(self, DB_DIRECTORY, DB_NAME, EXCHANGE, LOGGER, READ_ONLY=False,):

        # self.symbol = SYMBOL
        # self.type = TYPE
        self.exchange = EXCHANGE
        self.logger = LOGGER
        
        if not os.path.exists(DB_DIRECTORY):
            print(f"CREATING DIRECTORY: {DB_DIRECTORY}")
            os.makedirs(DB_DIRECTORY)


        if READ_ONLY: 
            self.con = sqlite3.connect('file:'+DB_DIRECTORY+DB_NAME+'?mode=ro', uri=True)   
        else: 
            self.con = sqlite3.connect(DB_DIRECTORY+DB_NAME)

        self.cur = self.con.cursor()

        self.create_candle_table()
        self.create_info_table()
        self.create_view()

    def log_info(self, payload):
        if self.logger is not None: 
            self.logger.info(dict(origin='symbols_db', payload=payload))

    def get_last(self, raw_dump=False):
        row = self.query(f"SELECT * FROM SYMBOLS_TABLE WHERE insert_timestamp=(SELECT last_insert_time FROM LAST_INSERT_VIEW)") 
        if len(row)>1:
            print('TOO MANY MATCHING ENTRIES - SOMETHING IS WRONG? ')

        if len(row)==0:
            print('no matches')
            return dict(usd_futs=[], spot=[], coin_futs=[])

        if not raw_dump: return json.loads(row[0][0])
        else: return json.loads(row[0][6])

    # def delete_last(self):
    #     self.execute(f"DELETE FROM SYMBOLS_TABLE WHERE insert_time=(SELECT last_oi_time FROM LAST_INSERT_VIEW)") 
       
    def create_candle_table(self):
        create_table = f'''CREATE TABLE IF NOT EXISTS SYMBOLS_TABLE
                           (all_symbols TEXT, added TEXT, subtracted TEXT, spot_updated TEXT, usd_futs_updated TEXT, coin_futs_updated TEXT, raw_dump TEXT, insert_timestamp TEXT, insert_timestamp_string TEXT)'''
        self.cur.execute(create_table)
        self.con.commit()

    def create_info_table(self):
        info_table = self.query("SELECT name FROM sqlite_master WHERE type='table' AND name='INFO_TABLE'")
        if len(info_table)==0:
            create_table = f'''CREATE TABLE IF NOT EXISTS INFO_TABLE
                            (exchange TEXT)'''
            self.cur.execute(create_table)
            self.cur.executemany(f'INSERT INTO INFO_TABLE VALUES (?)', [[self.exchange]])
            self.con.commit()

    def create_view(self):
        self.execute(f"""
            CREATE VIEW IF NOT EXISTS LAST_INSERT_VIEW AS SELECT 
            MAX(insert_timestamp)  AS last_insert_time,
            MAX(insert_timestamp_string)  AS last_insert_time_string,
            MIN(insert_timestamp)  AS first_insert_time,
            MIN(insert_timestamp_string)  AS first_insert_time_string FROM SYMBOLS_TABLE            
            """)
        
    def insert_multiple(self, insert_list): 
        if len(insert_list)==0:
            print(f"SYMBOLS_TABLE (insert list is EMPTY" )
            return

        insert_list=deepcopy(insert_list)
        # insert_list = [[s] for s in insert_list]
        insert_list = [json.dumps(d) for d in insert_list]

        inserted_at_int = datetime.datetime.utcnow().timestamp()
        insert_list.append(inserted_at_int)
        inserted_at_string = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        insert_list.append(inserted_at_string)

        # INSERT AND COMMIT
        self.cur.executemany(f'INSERT INTO SYMBOLS_TABLE VALUES (?,?,?,?,?,?,?,?,?)', [insert_list])
        self.con.commit()
        # print(f"SYMBOLS_TABLE ({self.symbol}) - {len(insert_list)} entries ({insert_list[0][2]} to {insert_list[-1][2]}) - inserted at {inserted_at}")

    def query(self, query):
        self.cur.execute(query)
        rows = self.cur.fetchall()
        return rows

    def execute(self, query):
        self.cur.execute(query)
        self.con.commit()


    def update_symbols_db(self, new_symbols_dict, exchange_infos_dict):
        """ CHECK IF NEW SYMBOLS ADDED OR SUBTRACED, IF SO THEN UPDATE TABLE"""
        last_insert=self.get_last()

        added_usd_futs = list(set(new_symbols_dict['usd_futs']).difference(set(last_insert['usd_futs'])))
        subtracted_usd_futs = list(set(last_insert['usd_futs']).difference(set(new_symbols_dict['usd_futs'])))

        added_spot = list(set(new_symbols_dict['spot']).difference(set(last_insert['spot'])))
        subtracted_spot = list(set(last_insert['spot']).difference(set(new_symbols_dict['spot'])))

        added_coin_futs = list(set(new_symbols_dict['coin_futs']).difference(set(last_insert['coin_futs'])))
        subtracted_coin_futs = list(set(last_insert['coin_futs']).difference(set(new_symbols_dict['coin_futs'])))

        coin_futs_updated = True if len(added_coin_futs)+len(subtracted_coin_futs)>0 else False
        usd_futs_updated = True if len(added_usd_futs)+len(subtracted_usd_futs)>0 else False
        spot_updated = True if len(added_spot)+len(subtracted_spot)>0 else False

        if sum([coin_futs_updated, usd_futs_updated, spot_updated])>0:
            print(f"added_spot={added_spot}")
            print(f"subtracted_spot={subtracted_spot}")
            
            print(f"added_usd_futs={added_usd_futs}")
            print(f"subtracted_usd_futs={subtracted_usd_futs},")

            print(f"added_coin_futs={added_coin_futs}")
            print(f"subtracted_coin_futs={subtracted_coin_futs}")

            new_insert_object = [
                new_symbols_dict, 
                dict(spot=added_spot,usd_futs=added_usd_futs, coin_futs=added_coin_futs), 
                dict(spot=subtracted_spot,usd_futs=subtracted_usd_futs, coin_futs=subtracted_coin_futs),
                spot_updated,
                usd_futs_updated,
                coin_futs_updated,
                exchange_infos_dict]

            self.insert_multiple(new_insert_object)

            self.log_info(
                dict(origin=symbols_db,
                payload = dict(
                    added = dict(
                        spot=added_spot,
                        usd_futs=added_usd_futs, 
                        coin_futs=added_coin_futs), 
                    subtracted = dict(
                        spot=subtracted_spot,
                        usd_futs=subtracted_usd_futs, 
                        coin_futs=subtracted_coin_futs))
                        )
                )


        else: 
            print('NO SYMBOL UPDATES')        

        return dict(
            added = dict(spot=added_spot,usd_futs=added_usd_futs, coin_futs=added_coin_futs), 
            subtracted = dict(spot=subtracted_spot,usd_futs=subtracted_usd_futs, coin_futs=subtracted_coin_futs),)







# TESTS: 
# DB_DIRECTORY='/home/cm/Documents/PY_DEV/BINANCE_CANDLEGRAB/db/'
# db=symbols_db(DB_DIRECTORY=DB_DIRECTORY,DB_NAME='test_symbols.db',EXCHANGE='binance' )

# db.update_symbols_db(new_symbols_dict={'usd_futs':['test1', 'test2'], 'spot':['hello', 'i'], 'coin_futs':[]})

# db.update_symbols_db(new_symbols_dict={'usd_futs':['test2'], 'spot':['hello', 'i', 'test1', ], 'coin_futs':['yes']})

# db.update_symbols_db(new_symbols_dict={'usd_futs':['test2'], 'spot':['hello', 'i',  ], 'coin_futs':['yes']})

# interval