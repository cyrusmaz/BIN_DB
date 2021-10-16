

from SymbolsDBClass import symbols_db
from CandleDBClass import candle_db
from OIDBClass import oi_db
from FundingDBClass import funding_db
from db_helpers import get_directories_from_param_path



# import json

param_path='/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'



def delete_oi_from_db(param_path, symbol, oi_interval, n, first_n=False, last_n=False):
    symbols_dir, usdf_candles_dir, spot_candles_dir, usdf_oi_dir, usdf_funding_dir, exchange = get_directories_from_param_path(param_path)
    if first_n == last_n: return
    if n<=0: return
    dir_=usdf_oi_dir
    db_args_dict=dict(TYPE='usdf', EXCHANGE=exchange)
    db_name=f'{symbol}_{oi_interval}_oi.db'
    db_dir=f'{dir_}{oi_interval}/'
    oi_db_=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=symbol, INTERVAL=oi_interval, READ_ONLY=False, **db_args_dict )

    if first_n: 
        oi_db_.execute(f"DELETE FROM OI_TABLE WHERE oi_time IN (SELECT oi_time FROM OI_TABLE ORDER BY oi_time ASC LIMIT {n})")
        print(f'symbol:{symbol} oi - first {n} rows deleted ')
    elif last_n: 
        oi_db_.execute(f"DELETE FROM OI_TABLE WHERE oi_time IN (SELECT oi_time FROM OI_TABLE ORDER BY oi_time DESC LIMIT {n})")
        print(f'symbol:{symbol} oi - last {n} rows deleted ')
    del oi_db_




# delete_oi_from_db(param_path=param_path, symbol='ATOMUSDT', oi_interval='5m', n=1000, first_n=False, last_n=True)

