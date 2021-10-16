

import sqlite3
import os 
import datetime
from copy import deepcopy
import json
from db_helpers import *
# from datetime import datetime


class symbols_db():
    
    def __init__(self, DB_DIRECTORY, DB_NAME, EXCHANGE, LOGGER, READ_ONLY=False,):
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
            return dict(usdf=[], spot=[], coinf=[])

        if not raw_dump: return json.loads(row[0][0])
        else: return json.loads(row[0][7])

    def create_candle_table(self):
        create_table = f'''CREATE TABLE IF NOT EXISTS SYMBOLS_TABLE
                           (all_symbols TEXT, added TEXT, subtracted TEXT, spot_updated TEXT, usdf_updated TEXT, coinf_updated TEXT, coinf_details_updated TEXT, raw_dump TEXT, insert_timestamp TEXT, insert_timestamp_string TEXT)'''
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
        self.cur.executemany(f'INSERT INTO SYMBOLS_TABLE VALUES (?,?,?,?,?,?,?,?,?,?)', [insert_list])
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

        added_usdf = list(set(new_symbols_dict['usdf']).difference(set(last_insert['usdf'])))
        subtracted_usdf = list(set(last_insert['usdf']).difference(set(new_symbols_dict['usdf'])))

        added_spot = list(set(new_symbols_dict['spot']).difference(set(last_insert['spot'])))
        subtracted_spot = list(set(last_insert['spot']).difference(set(new_symbols_dict['spot'])))

        added_coinf = list(set(new_symbols_dict['coinf']).difference(set(last_insert['coinf'])))
        subtracted_coinf = list(set(last_insert['coinf']).difference(set(new_symbols_dict['coinf'])))

        # added_coinf_details = list(set(new_symbols_dict['coinf_details'].values()).difference(set(last_insert['coinf_details'].values())))
        # subtracted_coinf_details = list(set(last_insert['coinf_details'].values()).difference(set(new_symbols_dict['coinf_details'].values()))) 

        added_coinf_details = list(filter(lambda x: x not in list(last_insert['coinf_details'].values()), list(new_symbols_dict['coinf_details'].values()))) if 'coinf_details' in last_insert.keys() else list(new_symbols_dict['coinf_details'].values())
        subtracted_coinf_details = list(filter(lambda x: x not in list(new_symbols_dict['coinf_details'].values()), list(last_insert['coinf_details'].values()))) if 'coinf_details' in last_insert.keys() else []

        coinf_updated = True if len(added_coinf)+len(subtracted_coinf)>0 else False
        usdf_updated = True if len(added_usdf)+len(subtracted_usdf)>0 else False
        spot_updated = True if len(added_spot)+len(subtracted_spot)>0 else False
        coinf_details_updated = True if len(added_coinf_details)+len(subtracted_coinf_details)>0 else False

        if sum([coinf_updated, usdf_updated, spot_updated, coinf_details_updated])>0:
            print(f"added_spot={added_spot}")
            print(f"subtracted_spot={subtracted_spot}")
            
            print(f"added_usdf={added_usdf}")
            print(f"subtracted_usdf={subtracted_usdf},")

            print(f"added_coinf={added_coinf}")
            print(f"subtracted_coinf={subtracted_coinf}")

            print(f"added_coinf_details={added_coinf_details}")
            print(f"subtracted_coinf_details={subtracted_coinf_details}")            

            new_insert_object = [
                new_symbols_dict, 
                dict(spot=added_spot,usdf=added_usdf, coinf=added_coinf, coinf_details=added_coinf_details), 
                dict(spot=subtracted_spot,usdf=subtracted_usdf, coinf=subtracted_coinf, coinf_details=subtracted_coinf_details),
                spot_updated,
                usdf_updated,
                coinf_updated,
                coinf_details_updated,
                exchange_infos_dict]

            self.insert_multiple(new_insert_object)

            self.log_info(
                dict(origin=symbols_db,
                payload = dict(
                    added = dict(
                        spot=added_spot,
                        usdf=added_usdf, 
                        coinf=added_coinf), 
                    subtracted = dict(
                        spot=subtracted_spot,
                        usdf=subtracted_usdf, 
                        coinf=subtracted_coinf))
                        )
                )


        else: 
            print('NO SYMBOL UPDATES')        

        return dict(
            added = dict(spot=added_spot,usdf=added_usdf, coinf=added_coinf), 
            subtracted = dict(spot=subtracted_spot,usdf=subtracted_usdf, coinf=subtracted_coinf),)







# TESTS: 
# DB_DIRECTORY='/home/cm/Documents/PY_DEV/BINANCE_CANDLEGRAB/db/'
# db=symbols_db(DB_DIRECTORY=DB_DIRECTORY,DB_NAME='test_symbols.db',EXCHANGE='binance' )

# db.update_symbols_db(new_symbols_dict={'usdf':['test1', 'test2'], 'spot':['hello', 'i'], 'coinf':[]})

# db.update_symbols_db(new_symbols_dict={'usdf':['test2'], 'spot':['hello', 'i', 'test1', ], 'coinf':['yes']})

# db.update_symbols_db(new_symbols_dict={'usdf':['test2'], 'spot':['hello', 'i',  ], 'coinf':['yes']})

# interval