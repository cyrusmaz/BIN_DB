from SymbolsDBClass import symbols_db
from CandleDBClass import candle_db
from OIDBClass import oi_db
from FundingDBClass import funding_db
from db_helpers import get_directories_from_param_path


import json

param_path='/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'

def read_symbols_from_db(param_path):
    symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange = get_directories_from_param_path(param_path)

    db_symbols=symbols_db(DB_DIRECTORY=symbols_dir,DB_NAME='symbols.db', EXCHANGE=exchange, LOGGER=None, READ_ONLY=True)
    return db_symbols.get_last()

def read_oi_from_db(param_path, symbol, oi_interval, n, first_n=False, last_n=False):
    symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange = get_directories_from_param_path(param_path)

    if first_n == last_n: return
    if n<=0: return

    dir_=usd_futs_oi_dir
    db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange)
    db_name=f'{symbol}_{oi_interval}_oi.db'
    db_dir=f'{dir_}{oi_interval}/'
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



def read_funding_from_db(param_path, symbol, n, first_n=False, last_n=False):
    symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange = get_directories_from_param_path(param_path)

    if first_n == last_n: return
    if n<=0: return

    dir_=usd_futs_funding_dir
    db_args_dict=dict(TYPE='funding', EXCHANGE=exchange)
    db_name=f'{symbol}_funding.db'
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



def read_candle_from_db(param_path, symbol,candle_interval,type_, n, first_n=False, last_n=False):
    symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange = get_directories_from_param_path(param_path)

    if first_n == last_n: return
    if n<=0: return

    if type_=='usd_futs':
        db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange)
        dir_=usd_futs_candles_dir

    elif type_=='spot':
        db_args_dict=dict(TYPE='spot', EXCHANGE=exchange)
        dir_=spot_candles_dir

    db_name=f'{symbol}_{candle_interval}_candle.db'
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
    print(f'symbol:{symbol} {type_} candles - first timestamp: {first_time}, last timestamp: {last_time}')
    del candle_db_
    return output