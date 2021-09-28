from candles_backfill import candles_backfill_fn
from candles_forwardfill import candles_forwardfill_fn

from oi_backfill import oi_backfill_fn
from oi_forwardfill import oi_forwardfill_fn

from funding_forwardfill import funding_forwardfill_fn

from CandleDBClass import candle_db
from OIDBClass import oi_db
from FundingDBClass import funding_db


from db_helpers import batch_symbols_fn
import aiohttp
import time
import datetime



def candle_fill_wrapper(
    symbols, 
    interval, 
    futs, 
    forward, 
    batch_size, 
    db_args_dict,
    dir_,
    startTimes_dict=None,
    logger=None, 
    backfill=None):
    j=0
    for batch_symbols in batch_symbols_fn(symbols=symbols, batch_size=batch_size):
        j+=1


        dbs={}
        # batch_symbols=list(filter(lambda x: x!='BNBBTC', batch_symbols))
        for s in batch_symbols:
            db_name=f'{s}_{interval}_candle.db'
            db_dir = f'{dir_}{interval}/'     
            dbs[s]=candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, INTERVAL=interval, SYMBOL=s, **db_args_dict)   




        try: 
            if forward is True: 
                candles_forwardfill_fn(symbols=batch_symbols,dbs=dbs, interval=interval, startTimes_dict=startTimes_dict, futs=futs, logger=logger)            
            elif forward is False:
                candles_backfill_fn(symbols=batch_symbols,dbs=dbs, interval=interval, backfill=backfill, futs=futs, logger=logger)
        except aiohttp.client_exceptions.ServerDisconnectedError: 
            exception_time = datetime.datetime.now()
            if logger is not None: 
                logger.critical(
                    dict(
                        origin='candle_fill_wrapper', 
                        payload=dict(
                            reason='aiohttp.client_exceptions.ServerDisconnectedError',                            
                            forward=forward,
                            batch_size=batch_size,
                            interval=interval,
                            futs=futs,
                            symbols=batch_symbols,)))

            print(f'candle_fill_wrapper {exception_time} - got an aiohttp.client_exceptions.ServerDisconnectedError, sleeping for 120 seconds before reattempting')
            print(f'candle_fill_wrapper {exception_time} - batch_size is {batch_size}, symbols remaining: {len(batch_symbols)}, forward: {forward}')
            time.sleep(120)
            if forward is True: 
                candles_forwardfill_fn(symbols=batch_symbols,dbs=dbs, interval=interval, backfill=backfill, futs=futs, logger=logger)            
            elif forward is False:
                candles_backfill_fn(symbols=batch_symbols,dbs=dbs, interval=interval, backfill=backfill, futs=futs, logger=logger)                    
        print(f'time.sleep(60) - candle_fill_wrapper() end of batch {j}, batch_size={batch_size}, symbols:{len(symbols)},futs:{futs}, interval:{interval}, forward:{forward}')
        time.sleep(60)

    print(f'candle_fill_wrapper() complete. futs:{futs}, interval:{interval}, forward:{forward}')


def funding_fill_wrapper(symbols, dbs, batch_size, logger=None):
    j=0
    for batch_symbols in batch_symbols_fn(symbols=symbols, batch_size=batch_size):
        j+=1
        try:
            funding_forwardfill_fn(symbols=batch_symbols,dbs=dbs, logger=logger)
        except aiohttp.client_exceptions.ServerDisconnectedError: 
            exception_time = datetime.datetime.now()
            if logger is not None: 
                logger.critical(
                    dict(
                        origin='funding_fill_wrapper', 
                        payload=dict(
                            reason='aiohttp.client_exceptions.ServerDisconnectedError',
                            batch_size=batch_size,
                            symbols=batch_symbols,)))
            print(f'funding_fill_wrapper {exception_time} - got an aiohttp.client_exceptions.ServerDisconnectedError, sleeping for 60 seconds before reattempting')
            print(f'funding_fill_wrapper {exception_time} - batch_size is {batch_size}, symbols remaining: {len(batch_symbols)}')
            time.sleep(60)
            funding_forwardfill_fn(symbols=batch_symbols,dbs=dbs, logger=logger)
        print(f'time.sleep(60) - funding_fill_wrapper() end of batch {j}, batch_size={batch_size}, symbols:{len(symbols)}.')
        time.sleep(60)
    print(f'funding_fill_wrapper() complete')



def oi_fill_wrapper(symbols, dbs, interval, forward, batch_size, logger=None):
    j=0
    for batch_symbols in batch_symbols_fn(symbols=symbols, batch_size=batch_size):
        j+=1
        try: 
            if forward is True: 
                oi_forwardfill_fn(symbols=batch_symbols,dbs=dbs, interval=interval, logger=logger)          
            elif forward is False:
                oi_backfill_fn(symbols=batch_symbols,dbs=dbs, interval=interval, backfill=None,logger=logger)
        except aiohttp.client_exceptions.ServerDisconnectedError: 
            exception_time = datetime.datetime.now()
            if logger is not None: 
                logger.critical(
                    dict(
                        origin='oi_fill_wrapper', 
                        payload=dict(
                            reason='aiohttp.client_exceptions.ServerDisconnectedError',
                            forward=forward,
                            batch_size=batch_size,
                            interval=interval,
                            # futs=futs,
                            symbols=batch_symbols,)))

            print(f'candle_fill_wrapper {exception_time} - got an aiohttp.client_exceptions.ServerDisconnectedError, sleeping for 60 seconds before reattempting')
            print(f'candle_fill_wrapper {exception_time} - batch_size is {batch_size}, symbols remaining: {len(batch_symbols)}, forward: {forward}')
            time.sleep(60)
            if forward is True: 
                oi_forwardfill_fn(symbols=batch_symbols,dbs=dbs, interval=interval, logger=logger)          
            elif forward is False:
                oi_backfill_fn(symbols=batch_symbols,dbs=dbs, interval=interval, backfill=None,logger=logger) 
                                 
        print(f'time.sleep(60) - oi_fill_wrapper() end of batch {j}, batch_size={batch_size}, symbols:{len(symbols)}, interval:{interval}, forward:{forward}')
        time.sleep(60)

    print(f'oi_fill_wrapper() complete')



def prepare_for_funding_fetch(dir_, symbols, db_args_dict):
    dbs_funding = {}
    for s in symbols:
        db_name=f'{s}_funding.db'
        # dbs_funding[s]=funding_db(SYMBOL=s, DB_DIRECTORY=dir_, DB_NAME=db_name, TYPE='funding', EXCHANGE=EXCHANGE)
        dbs_funding[s]=funding_db(SYMBOL=s, DB_DIRECTORY=dir_, DB_NAME=db_name, **db_args_dict)
    return symbols, dbs_funding 


def prepare_for_oi_fetch(dir_, symbols, oi_interval, db_args_dict, check_existence=True):
    symbols_exist = []
    symbols_dne = []
    dbs_exist = {}
    dbs_dne = {}
    # ESTABLISH WHICH SYMBOL HAS AN EXISTING DB WITH AT LEAST ONE RECORD, AND WHICH DOESN'T
    for s in symbols:

        db_name=f'{s}_{oi_interval}_oi.db'
        db_dir=f'{dir_}{oi_interval}/'
        # db=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=s, INTERVAL=oi_interval, TYPE='usd_futs', EXCHANGE=exchange)
        db=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=s, INTERVAL=oi_interval, **db_args_dict )
        if check_existence is True: 
            if db.get_last() is not None: 
                symbols_exist.append(s)
                dbs_exist[s]=db
            else:
                symbols_dne.append(s)
                dbs_dne[s]=db
        else: 
            symbols_exist.append(s)
            dbs_exist[s]=db                  

    return symbols_exist, dbs_exist, symbols_dne, dbs_dne

def prepare_for_candle_fetch(dir_, symbols, candle_interval, db_args_dict, check_existence=True, logger=None):
    """if check_existence is True then check if db exists and has at least one record"""
    symbols_exist = []
    symbols_dne = []
    # dbs_exist = {}
    # dbs_dne = {}
    j=0
    # ESTABLISH WHICH SYMBOL HAS AN EXISTING DB WITH AT LEAST ONE RECORD, AND WHICH DOESN'T
    startTimes_dict = dict()
    for s in symbols:
        j+=1
        print(j)
        db_name=f'{s}_{candle_interval}_candle.db'
        db_dir = f'{dir_}{candle_interval}/'     

        db=candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, INTERVAL=candle_interval, SYMBOL=s, **db_args_dict)    

        if check_existence is True: 
            last_candle=db.get_last()
            if last_candle is not None: 
                symbols_exist.append(s)
                startTimes_dict[s]=last_candle[0]
                # dbs_exist[s]=db
            else:
                symbols_dne.append(s)
                # dbs_dne[s]=db
        else: 
            symbols_exist.append(s)
            # dbs_exist[s]=db           

        # del db 
        db.close_connection()
        del db

    if len(startTimes_dict)==0:
        startTimes_dict=None

    return symbols_exist,startTimes_dict, symbols_dne 



























# def prepare_for_candle_fetch(dir_, symbols, candle_interval, db_args_dict, exists=True, logger=None):
#     """return only those that exists if exsits is True, else dne"""
#     """'exists' defined as database exists and contains atleast one non empty record"""

#     symbols_exist = []
#     symbols_dne = []
#     dbs_exist = {}
#     dbs_dne = {}
#     j=0
#     # ESTABLISH WHICH SYMBOL HAS AN EXISTING DB WITH AT LEAST ONE RECORD, AND WHICH DOESN'T
#     for s in symbols:
#         j+=1
#         print(j)
#         db_name=f'{s}_{candle_interval}_candle.db'
#         db_dir = f'{dir_}{candle_interval}/'     


#         db=candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, INTERVAL=candle_interval, SYMBOL=s, **db_args_dict)    

#         if exists: 
#             if db.get_last() is not None: 
#                 symbols_exist.append(s)
#                 dbs_exist[s]=db
#             else:
#                 del db
#         else: 
#             if db.get_last() is None: 
#                 symbols_exist.append(s)
#                 dbs_exist[s]=db
#             else:
#                 del db

#     return symbols_exist, dbs_exist, symbols_dne, dbs_dne












