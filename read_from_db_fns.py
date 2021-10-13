from DB_class_symbols import symbols_db
from DB_class_candles import candle_db
from DB_class_oi import oi_db
from DB_class_funding import funding_db
from db_helpers import get_directories_from_param_path

import json

# param_path='/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'

def read_symbols_from_db(param_path, raw_dump=False):
    # symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange = get_directories_from_param_path(param_path)
    r = get_directories_from_param_path(param_path)
    exchange = r['exchange']
    symbols_dir =r['symbols_dir']

    db_symbols=symbols_db(DB_DIRECTORY=symbols_dir,DB_NAME='symbols.db', EXCHANGE=exchange, LOGGER=None, READ_ONLY=True)
    return db_symbols.get_last(raw_dump=raw_dump)

def read_oi_from_db(param_path, symbol, oi_interval, coin_futs,usd_futs, n, first_n=False, last_n=False):
    # symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange = get_directories_from_param_path(param_path)
    r = get_directories_from_param_path(param_path)
    exchange = r['exchange']
    usd_futs_oi_dir =r['usd_futs_oi_dir']
    coin_futs_oi_dir =r['coin_futs_oi_dir']

    if first_n == last_n: return
    if n<=0: return

    if usd_futs:
        db_name=f'{symbol}_{oi_interval}_usdf_oi.db'
        db_dir=f'{usd_futs_oi_dir}{oi_interval}/'
        db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange)        
    elif coin_futs: 
        db_name=f'{symbol}_{oi_interval}_coinf_oi.db'
        db_dir=f'{coin_futs_oi_dir}{oi_interval}/'  
        db_args_dict=dict(TYPE='coin_futs', EXCHANGE=exchange)

    oi_db_=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=symbol, INTERVAL=oi_interval, READ_ONLY=True, **db_args_dict )

    if first_n: 
        results = oi_db_.query(f"SELECT oi, oi_time_string FROM OI_TABLE ORDER BY oi_time ASC LIMIT {n}")
        output = [json.loads(r[0]) for r in results] 
    elif last_n: 
        results = oi_db_.query(f"SELECT oi, oi_time_string FROM OI_TABLE ORDER BY oi_time DESC LIMIT {n}")
        results.reverse()
        
    output = [json.loads(r[0]) for r in results]
    first_time = results[0][1]
    last_time = results[-1][1]  
    print(f'symbol:{symbol} oi - first timestamp: {first_time}, last timestamp: {last_time}')
    del oi_db_
    return output

def read_funding_from_db(param_path, symbol, n, usd_futs, coin_futs, first_n=False, last_n=False):
    # symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange = get_directories_from_param_path(param_path)

    # symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange = get_directories_from_param_path(param_path)
    r = get_directories_from_param_path(param_path)
    exchange = r['exchange']
    usd_futs_funding_dir =r['usd_futs_funding_dir']
    coin_futs_funding_dir =r['coin_futs_funding_dir']

    if first_n == last_n: return
    if n<=0: return

    if usd_futs:
        db_name=f'{symbol}_usdf_funding.db'
        dir_=usd_futs_funding_dir
        db_args_dict=dict(TYPE='funding', EXCHANGE=exchange)
    elif coin_futs: 
        db_name=f'{symbol}_coinf_funding.db'
        dir_=coin_futs_funding_dir
        db_args_dict=dict(TYPE='funding', EXCHANGE=exchange)
        
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
    first_time = results[0][1]
    last_time = results[-1][1]  
    print(f'symbol:{symbol} funding - first timestamp: {first_time}, last timestamp: {last_time}')
    del funding_db_
    return output

def read_candle_from_db(param_path, symbol,candle_interval,usd_futs, coin_futs, mark, index, n, first_n=False, last_n=False):
    # symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange = get_directories_from_param_path(param_path)

    r = get_directories_from_param_path(param_path)
    exchange = r['exchange']
    usd_futs_candles_dir=r['usd_futs_candles_dir']
    usd_futs_index_candles_dir=r['usd_futs_index_candles_dir']
    usd_futs_mark_candles_dir=r['usd_futs_mark_candles_dir']
    coin_futs_candles_dir=r['coin_futs_candles_dir']
    coin_futs_index_candles_dir=r['coin_futs_index_candles_di']
    coin_futs_mark_candles_dir=r['coin_futs_mark_candles_dir']
    spot_candles_dir=r['spot_candles_dir']   

    if first_n == last_n: return
    if n<=0: return

    if not usd_futs and not coin_futs and not mark and not index:
        db_name=f'{symbol}_{candle_interval}_spot_candles.db'
        dir_=spot_candles_dir
        db_args_dict=dict(TYPE='spot', EXCHANGE=exchange)
        
    elif usd_futs: 
        if not mark and not index:
            db_name=f'{symbol}_{candle_interval}_usdf_candles.db'
            dir_=usd_futs_candles_dir
            db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange)
        if mark and not index:
            db_name=f'{symbol}_{candle_interval}_usdf_mark.db'
            dir_=usd_futs_mark_candles_dir
            db_args_dict=dict(TYPE='usd_futs_mark', EXCHANGE=exchange)
        elif not mark and index:
            db_name=f'{symbol}_{candle_interval}_usdf_index.db'
            dir_=usd_futs_index_candles_dir
            db_args_dict=dict(TYPE='usd_futs_index', EXCHANGE=exchange)

    elif coin_futs:     
        if not mark and not index:
            db_name=f'{symbol}_{candle_interval}_coinf_candles.db'  
            dir_=coin_futs_candles_dir         
            db_args_dict=dict(TYPE='coin_futs', EXCHANGE=exchange)
        if mark and not index:
            db_name=f'{symbol}_{candle_interval}_coinf_mark.db'
            dir_=coin_futs_mark_candles_dir
            db_args_dict=dict(TYPE='coin_futs_mark', EXCHANGE=exchange)
        elif not mark and index:
            db_name=f'{symbol}_{candle_interval}_coinf_index.db'    
            dir_=coin_futs_index_candles_dir
            db_args_dict=dict(TYPE='coin_futs_index', EXCHANGE=exchange)

    db_dir = f'{dir_}{candle_interval}/'     

    candle_db_=candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, INTERVAL=candle_interval, SYMBOL=symbol, READ_ONLY=True, **db_args_dict)    

    if first_n: 
        results = candle_db_.query(f"SELECT candle, open_time_string FROM CANDLE_TABLE ORDER BY open_time ASC LIMIT {n}")
        output = [json.loads(r[0]) for r in results] 
    elif last_n: 
        results = candle_db_.query(f"SELECT candle, open_time_string FROM CANDLE_TABLE ORDER BY open_time DESC LIMIT {n}")
        results.reverse()
        
    output = [json.loads(r[0]) for r in results]
    first_time = results[0][1]
    last_time = results[-1][1]  
    print(f'symbol:{symbol} usd_futs:{usd_futs} coin_futs:{coin_futs} mark:{mark} index:{index} candles - first timestamp: {first_time}, last timestamp: {last_time}')
    del candle_db_
    return output