from DB_class_symbols import symbols_db
from DB_class_candles import candle_db
from DB_class_oi import oi_db
from DB_class_funding import funding_db
from db_helpers import get_directories_from_param_path

import json

# param_path='/home/cm/Documents/PY_DEV/DB_BIN/params.json'
# s=read_symbols_from_db(param_path, raw_dump=True)
def read_symbols_from_db(param_path, raw_dump=False):
    # symbols_dir, usdf_candles_dir, spot_candles_dir, usdf_oi_dir, usdf_funding_dir, exchange = get_directories_from_param_path(param_path)
    r = get_directories_from_param_path(param_path)
    exchange = r['exchange']
    symbols_dir =r['symbols_dir']

    db_symbols=symbols_db(DB_DIRECTORY=symbols_dir,DB_NAME='symbols.db', EXCHANGE=exchange, LOGGER=None, READ_ONLY=True)
    return db_symbols.get_last(raw_dump=raw_dump)

def read_oi_from_db(param_path, symbol, oi_interval, coinf,usdf, n, first_n=False, last_n=False):
    # symbols_dir, usdf_candles_dir, spot_candles_dir, usdf_oi_dir, usdf_funding_dir, exchange = get_directories_from_param_path(param_path)
    r = get_directories_from_param_path(param_path)
    exchange = r['exchange']
    usdf_oi_dir =r['usdf_oi_dir']
    coinf_oi_dir =r['coinf_oi_dir']

    if first_n == last_n: return
    if n<=0: return

    if usdf:
        db_name=f'{symbol}_{oi_interval}_usdf_oi.db'
        db_dir=f'{usdf_oi_dir}{oi_interval}/'
        db_args_dict=dict(TYPE='usdf_oi', EXCHANGE=exchange)        
    elif coinf: 
        db_name=f'{symbol}_{oi_interval}_coinf_oi.db'
        db_dir=f'{coinf_oi_dir}{oi_interval}/'  
        db_args_dict=dict(TYPE='coinf_oi', EXCHANGE=exchange)

    oi_db_=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=symbol, INTERVAL=oi_interval, READ_ONLY=True, **db_args_dict )

    if first_n: 
        results = oi_db_.query(f"SELECT oi, oi_time_string FROM OI_TABLE ORDER BY oi_time ASC LIMIT {n}")
        output = [json.loads(r[0]) for r in results] 
    elif last_n: 
        results = oi_db_.query(f"SELECT oi, oi_time_string FROM OI_TABLE ORDER BY oi_time DESC LIMIT {n}")
        results.reverse()
        
    output = [json.loads(r[0]) for r in results]
    try: 
        first_time = results[0][1]
        last_time = results[-1][1]  
        print(f'symbol:{symbol} oi, usdf:{usdf}, coinf:{coinf} - first timestamp: {first_time}, last timestamp: {last_time}')
    except Exception as e: 
        print(e)
        print(f'symbol:{symbol} oi, usdf:{usdf}, coinf:{coinf}')
        raise e

    del oi_db_
    return output

def read_funding_from_db(param_path, symbol, n, usdf, coinf, first_n=False, last_n=False):
    # symbols_dir, usdf_candles_dir, spot_candles_dir, usdf_oi_dir, usdf_funding_dir, exchange = get_directories_from_param_path(param_path)

    # symbols_dir, usdf_candles_dir, spot_candles_dir, usdf_oi_dir, usdf_funding_dir, exchange = get_directories_from_param_path(param_path)
    r = get_directories_from_param_path(param_path)
    exchange = r['exchange']
    usdf_funding_dir =r['usdf_funding_dir']
    coinf_funding_dir =r['coinf_funding_dir']

    if first_n == last_n: return
    if n<=0: return

    if usdf:
        db_name=f'{symbol}_usdf_funding.db'
        dir_=usdf_funding_dir
        db_args_dict=dict(TYPE='usdf_funding', EXCHANGE=exchange)
    elif coinf: 
        db_name=f'{symbol}_coinf_funding.db'
        dir_=coinf_funding_dir
        db_args_dict=dict(TYPE='coinf_funding', EXCHANGE=exchange)
        
    # db_args_dict=dict(TYPE='funding', EXCHANGE=exchange)
    # db_name=f'{symbol}_funding.db'
    funding_db_=funding_db(SYMBOL=symbol, DB_DIRECTORY=dir_, DB_NAME=db_name, READ_ONLY=True, **db_args_dict)

    if first_n: 
        results = funding_db_.query(f"SELECT funding, funding_time_string FROM FUNDING_TABLE ORDER BY funding_time ASC LIMIT {n}")
        output = [json.loads(r[0]) for r in results] 
    elif last_n: 
        results = funding_db_.query(f"SELECT funding, funding_time_string FROM FUNDING_TABLE ORDER BY funding_time DESC LIMIT {n}")
        results.reverse()
        
    output = [json.loads(r[0]) for r in results]
    try: 
        first_time = results[0][1]
        last_time = results[-1][1]  
        print(f'symbol:{symbol} funding, usdf:{usdf}, coinf:{coinf}, - first timestamp: {first_time}, last timestamp: {last_time}')
    except Exception as e: 
        print(e)
        print(f'symbol:{symbol} funding, usdf:{usdf}, coinf:{coinf}')
        raise e
    del funding_db_
    return output

def read_candle_from_db(param_path, symbol,candle_interval,usdf, coinf, mark, index, n, first_n=False, last_n=False):
    # symbols_dir, usdf_candles_dir, spot_candles_dir, usdf_oi_dir, usdf_funding_dir, exchange = get_directories_from_param_path(param_path)

    r = get_directories_from_param_path(param_path)
    exchange = r['exchange']
    usdf_candles_dir=r['usdf_candles_dir']
    usdf_index_candles_dir=r['usdf_index_candles_dir']
    usdf_mark_candles_dir=r['usdf_mark_candles_dir']
    coinf_candles_dir=r['coinf_candles_dir']
    coinf_index_candles_dir=r['coinf_index_candles_di']
    coinf_mark_candles_dir=r['coinf_mark_candles_dir']
    spot_candles_dir=r['spot_candles_dir']   

    if first_n == last_n: return
    if n<=0: return

    if not usdf and not coinf and not mark and not index:
        db_name=f'{symbol}_{candle_interval}_spot_candles.db'
        dir_=spot_candles_dir
        db_args_dict=dict(TYPE='spot_candles', EXCHANGE=exchange)
        
    elif usdf: 
        if not mark and not index:
            db_name=f'{symbol}_{candle_interval}_usdf_candles.db'
            dir_=usdf_candles_dir
            db_args_dict=dict(TYPE='usdf_candles', EXCHANGE=exchange)
        if mark and not index:
            db_name=f'{symbol}_{candle_interval}_usdf_mark.db'
            dir_=usdf_mark_candles_dir
            db_args_dict=dict(TYPE='usdf_mark', EXCHANGE=exchange)
        elif not mark and index:
            db_name=f'{symbol}_{candle_interval}_usdf_index.db'
            dir_=usdf_index_candles_dir
            db_args_dict=dict(TYPE='usdf_index', EXCHANGE=exchange)

    elif coinf:     
        if not mark and not index:
            db_name=f'{symbol}_{candle_interval}_coinf_candles.db'  
            dir_=coinf_candles_dir         
            db_args_dict=dict(TYPE='coinf_candles', EXCHANGE=exchange)
        if mark and not index:
            db_name=f'{symbol}_{candle_interval}_coinf_mark.db'
            dir_=coinf_mark_candles_dir
            db_args_dict=dict(TYPE='coinf_mark', EXCHANGE=exchange)
        elif not mark and index:
            db_name=f'{symbol}_{candle_interval}_coinf_index.db'    
            dir_=coinf_index_candles_dir
            db_args_dict=dict(TYPE='coinf_index', EXCHANGE=exchange)

    db_dir = f'{dir_}{candle_interval}/'     

    candle_db_=candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, INTERVAL=candle_interval, SYMBOL=symbol, READ_ONLY=True, **db_args_dict)    

    if first_n: 
        results = candle_db_.query(f"SELECT candle, open_time_string FROM CANDLE_TABLE ORDER BY open_time ASC LIMIT {n}")
        output = [json.loads(r[0]) for r in results] 
    elif last_n: 
        results = candle_db_.query(f"SELECT candle, open_time_string FROM CANDLE_TABLE ORDER BY open_time DESC LIMIT {n}")
        results.reverse()
        
    output = [json.loads(r[0]) for r in results]
    try: 
        first_time = results[0][1]
        last_time = results[-1][1]  
        print(f'symbol:{symbol} usdf:{usdf} coinf:{coinf} mark:{mark} index:{index} candles - first timestamp: {first_time}, last timestamp: {last_time}')
    except Exception as e: 
        print(e)
        print(f'symbol:{symbol} usdf:{usdf} coinf:{coinf} mark:{mark} index:{index} candles')
        raise(e)
    del candle_db_
    return output