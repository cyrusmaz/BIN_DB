# from candles_backfill import candles_backfill_fn
# from candles_forwardfill import candles_forwardfill_fn
# from oi_backfill import oi_backfill_fn
# from oi_forwardfill import oi_forwardfill_fn

from fill_candles_fns import candles_backfill_fn, candles_forwardfill_fn
from fill_oi_fns import oi_backfill_fn, oi_forwardfill_fn
from fill_funding_fns import funding_forwardfill_fn


from DB_class_candles import candle_db
from DB_class_oi import oi_db
from DB_class_funding import funding_db

from db_helpers import batch_symbols_fn
import aiohttp
import time
import datetime

def candle_fill_wrapper(
    symbols, 
    interval, 
    usdf, 
    coinf,
    mark,
    index,
    forward, 
    batch_size, 
    db_args_dict,
    dir_,
    limit,
    rate_limit,
    startTimes_dict=None,
    logger=None, 
    backfill=None,
    skip_sleep=False):
    """spot candles:     usdf=False, coinf=False, mark=False, index=False

       usdf candles: usdf=True, coinf=False, mark=False, index=False
       usdf mark:    usdf=True, coinf=False, mark=True,  index=False
       usdf index:   usdf=True, coinf=False, mark=False, index=True

       coinf candles: usdf=False, coinf=True, mark=False, index=False
       coinf mark:    usdf=False, coinf=True, mark=True,  index=False
       coinf index:   usdf=False, coinf=True, mark=False, index=True       
       """
    
    j=0
    for batch_symbols in batch_symbols_fn(symbols=symbols, batch_size=batch_size):
        j+=1

        dbs={}
        for s in batch_symbols:
            if not usdf and not coinf and not mark and not index:
                db_name=f'{s}_{interval}_spot_candles.db'

            elif usdf: 
                if not mark and not index:
                    db_name=f'{s}_{interval}_usdf_candles.db'
                if mark and not index:
                    db_name=f'{s}_{interval}_usdf_mark.db'
                elif not mark and index:
                    db_name=f'{s}_{interval}_usdf_index.db'

            elif coinf:     
                if not mark and not index:
                    db_name=f'{s}_{interval}_coinf_candles.db'               
                if mark and not index:
                    db_name=f'{s}_{interval}_coinf_mark.db'
                elif not mark and index:
                    db_name=f'{s}_{interval}_coinf_index.db'                 

            db_dir = f'{dir_}{interval}/'     
            dbs[s]=candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, INTERVAL=interval, SYMBOL=s, **db_args_dict)   

        try: 
            if forward is True: 
                candles_forwardfill_fn(
                    symbols=batch_symbols,dbs=dbs, 
                    interval=interval, startTimes_dict=startTimes_dict, 
                    coinf=coinf, usdf=usdf, 
                    mark=mark, index=index, 
                    limit=limit, rate_limit=rate_limit, 
                    logger=logger)      

            elif forward is False:
                candles_backfill_fn(
                    symbols=batch_symbols,dbs=dbs, 
                    interval=interval, backfill=backfill, 
                    usdf=usdf, coinf=coinf, 
                    mark=mark, index=index, 
                    limit=limit, rate_limit=rate_limit, 
                    logger=logger)
                    
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
                            usdf=usdf,
                            coinf=coinf,
                            mark=mark,
                            index=index,
                            symbols=batch_symbols,)))

            print(f'candle_fill_wrapper {exception_time} - got an aiohttp.client_exceptions.ServerDisconnectedError, sleeping for 120 seconds before reattempting')
            print(f'candle_fill_wrapper {exception_time} - batch_size is {batch_size}, symbols remaining: {len(batch_symbols)}, forward: {forward}')
            print(f'candle_fill_wrapper {exception_time} - usdf:{usdf},coinf:{coinf}, mark:{mark}, index:{index}')
            time.sleep(120)
            if forward is True: 
                candles_forwardfill_fn(
                    symbols=batch_symbols,dbs=dbs, 
                    interval=interval, backfill=backfill, 
                    usdf=usdf, coinf=coinf, 
                    mark=mark, index=index, 
                    limit=limit, rate_limit=rate_limit, 
                    logger=logger)            

            elif forward is False:
                candles_backfill_fn(
                    symbols=batch_symbols, dbs=dbs, 
                    interval=interval, backfill=backfill, 
                    usdf=usdf, coinf=coinf, 
                    mark=mark, index=index, 
                    limit=limit, rate_limit=rate_limit, 
                    logger=logger)                    

        print(f'candle_fill_wrapper() end of batch {j}, batch_size={batch_size}, symbols:{len(symbols)},usdf:{usdf},coinf:{coinf}, mark:{mark}, index:{index}, interval:{interval}, forward:{forward}')
        if not skip_sleep:
            print(f'{datetime.datetime.now()} time.sleep(60)')
            time.sleep(60)        
        

    print(f'candle_fill_wrapper() complete. usdf:{usdf},coinf:{coinf}, mark:{mark}, index:{index}, interval:{interval}, forward:{forward}')

def funding_fill_wrapper(symbols, dbs, batch_size, usdf, coinf, limit, rate_limit, logger=None,
    skip_sleep=False):
    j=0
    for batch_symbols in batch_symbols_fn(symbols=symbols, batch_size=batch_size):
        j+=1
        try:
            funding_forwardfill_fn(symbols=batch_symbols,dbs=dbs, usdf=usdf, coinf=coinf, limit=limit, rate_limit=rate_limit, logger=logger)
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
            print(f'usdf:{usdf},coinf:{coinf}')
            time.sleep(60)
            funding_forwardfill_fn(symbols=batch_symbols,dbs=dbs, usdf=usdf, coinf=coinf, limit=limit, rate_limit=rate_limit, logger=logger)
        print(f'funding_fill_wrapper() end of batch {j}, usdf:{usdf}, coinf:{coinf}, batch_size={batch_size}, symbols:{len(symbols)}.')
        # time.sleep(60)
        if not skip_sleep:
            print(f'{datetime.datetime.now()} time.sleep(60)')
            time.sleep(60)        
        
    print(f'funding_fill_wrapper() complete')

def oi_fill_wrapper(
    symbols, 
    # dbs, 
    interval, 
    forward, 
    batch_size, 
    usdf, 
    coinf, 
    limit,
    rate_limit,
    db_args_dict,
    startTimes_dict,
    dir_,
    logger=None,
    coinf_details=None,
    skip_sleep=False):
    j=0
    for batch_symbols in batch_symbols_fn(symbols=symbols, batch_size=batch_size):
        j+=1
        dbs={}
        for s in batch_symbols:
            if usdf:
                db_name=f'{s}_{interval}_usdf_oi.db'
                db_dir=f'{dir_}{interval}/'
            elif coinf: 
                db_name=f'{s}_{interval}_coinf_oi.db'
                db_dir=f'{dir_}{interval}/'            
            # db=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=s, INTERVAL=oi_interval, TYPE='usd_usdf', EXCHANGE=exchange)
            dbs[s]=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=s, INTERVAL=interval, **db_args_dict )
        try: 
            if forward is True: 
                oi_forwardfill_fn(
                    symbols=batch_symbols,dbs=dbs, 
                    interval=interval, usdf=usdf, 
                    coinf=coinf,limit=limit, 
                    rate_limit=rate_limit,  logger=logger,
                    startTimes_dict=startTimes_dict, 
                    coinf_details=coinf_details)          
            elif forward is False:
                oi_backfill_fn(
                    symbols=batch_symbols,dbs=dbs, 
                    interval=interval, usdf=usdf, 
                    coinf=coinf,limit=limit, 
                    rate_limit=rate_limit,  backfill=None,
                    logger=logger,coinf_details=coinf_details)

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
                            # usdf=usdf,
                            symbols=batch_symbols,)))

            print(f'oi_fill_wrapper {exception_time} - got an aiohttp.client_exceptions.ServerDisconnectedError, sleeping for 60 seconds before reattempting')
            print(f'oi_fill_wrapper {exception_time} - batch_size is {batch_size}, symbols remaining: {len(batch_symbols)}, forward: {forward}')
            print(f'usdf:{usdf},coinf:{coinf}')
            print(f'SLEEPING FOR 60 SECONDS, STARTING @ {datetime.datetime.now()}')
            time.sleep(60)
            if forward is True: 
                oi_forwardfill_fn(
                    symbols=batch_symbols,dbs=dbs, 
                    interval=interval, usdf=usdf, 
                    coinf=coinf,
                    limit=limit, rate_limit=rate_limit,  
                    logger=logger ,coinf_details=coinf_details)          
            elif forward is False:
                oi_backfill_fn(
                    symbols=batch_symbols,dbs=dbs, 
                    interval=interval, usdf=usdf, 
                    coinf=coinf,limit=limit, 
                    rate_limit=rate_limit,  backfill=None,
                    logger=logger, coinf_details=coinf_details) 
                                 
        print(f'oi_fill_wrapper() end of batch {j}, batch_size={batch_size}, symbols:{len(symbols)}, interval:{interval}, forward:{forward}')
        if not skip_sleep:
            print(f'{datetime.datetime.now()} time.sleep(60)')
            time.sleep(60)

    print(f'oi_fill_wrapper() complete')

def prepare_for_funding_fetch(
    dir_, 
    symbols, 
    db_args_dict, 
    usdf=False, 
    coinf=False):
    dbs_funding = {}
    for s in symbols:
        if usdf:
            db_name=f'{s}_usdf_funding.db'
        elif coinf: 
            db_name=f'{s}_coinf_funding.db'
        # dbs_funding[s]=funding_db(SYMBOL=s, DB_DIRECTORY=dir_, DB_NAME=db_name, TYPE='funding', EXCHANGE=EXCHANGE)
        dbs_funding[s]=funding_db(SYMBOL=s, DB_DIRECTORY=dir_, DB_NAME=db_name, **db_args_dict)
    return symbols, dbs_funding 

def prepare_for_oi_fetch(
    dir_, 
    symbols, 
    oi_interval, 
    db_args_dict, 
    usdf=False, 
    coinf=False, 
    check_existence=True):
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
        if usdf:
            db_name=f'{s}_{oi_interval}_usdf_oi.db'
            db_dir=f'{dir_}{oi_interval}/'
        elif coinf: 
            db_name=f'{s}_{oi_interval}_coinf_oi.db'
            db_dir=f'{dir_}{oi_interval}/'            
        # db=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=s, INTERVAL=oi_interval, TYPE='usd_usdf', EXCHANGE=exchange)
        db=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=s, INTERVAL=oi_interval, **db_args_dict )
        if check_existence is True: 
            last_insert = db.get_last()
            if last_insert is not None: 
                startTime = last_insert['timestamp']
                symbols_exist.append(s)
                startTimes_dict[s]=startTime
            else:
                symbols_dne.append(s)
        else: 
            symbols_exist.append(s)          
        db.close_connection()
        del db

    if len(startTimes_dict)==0:
        startTimes_dict=None

    return symbols_exist, startTimes_dict, symbols_dne

def prepare_for_candle_fetch(
    dir_, 
    symbols, 
    interval, 
    db_args_dict, 
    usdf=False, 
    coinf=False, 
    mark=False, 
    index=False, 
    check_existence=True, 
    logger=None):

    """if check_existence is True then check if db exists and has at least one record"""
    """spot candles:     usdf=False, mark=False, index=False
       usdf candles: usdf=True,  mark=False, index=False
       mark:             usdf=True,  mark=True,  index=False
       index:            usdf=True,  mark=False, index=True
       """

    symbols_exist = []
    symbols_dne = []
    j=0
    # ESTABLISH WHICH SYMBOL HAS AN EXISTING DB WITH AT LEAST ONE RECORD, AND WHICH DOESN'T
    startTimes_dict = dict()
    for s in symbols:
        j+=1
        print(j)

        if not usdf and not coinf and not mark and not index:
            db_name=f'{s}_{interval}_spot_candles.db'

        elif usdf: 
            if not mark and not index:
                db_name=f'{s}_{interval}_usdf_candles.db'
            if mark and not index:
                db_name=f'{s}_{interval}_usdf_mark.db'
            elif not mark and index:
                db_name=f'{s}_{interval}_usdf_index.db'

        elif coinf:     
            if not mark and not index:
                db_name=f'{s}_{interval}_coinf_candles.db'               
            if mark and not index:
                db_name=f'{s}_{interval}_coinf_mark.db'
            elif not mark and index:
                db_name=f'{s}_{interval}_coinf_index.db'                
        
        db_dir = f'{dir_}{interval}/'     

        db=candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, INTERVAL=interval, SYMBOL=s, **db_args_dict)    

        if check_existence is True: 
            last_candle=db.get_last()
            if last_candle is not None: 
                symbols_exist.append(s)
                startTimes_dict[s]=last_candle[0]
            else:
                symbols_dne.append(s)
        else: 
            symbols_exist.append(s)        

        db.close_connection()
        del db

    if len(startTimes_dict)==0:
        startTimes_dict=None

    return symbols_exist, startTimes_dict, symbols_dne 



























# def prepare_for_candle_fetch(dir_, symbols, interval, db_args_dict, exists=True, logger=None):
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
#         db_name=f'{s}_{interval}_candle.db'
#         db_dir = f'{dir_}{interval}/'     


#         db=candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, INTERVAL=interval, SYMBOL=s, **db_args_dict)    

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












