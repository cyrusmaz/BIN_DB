from read_from_db_fns import *

param_path='/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'

read_symbols_from_db(param_path=param_path)



symbol='BTCUSDT'
oi_interval='5m'
n=5

read_oi_from_db(param_path=param_path, symbol=symbol, oi_interval=oi_interval, n=n, first_n=False, last_n=True)
read_oi_from_db(param_path=param_path, symbol=symbol, oi_interval=oi_interval, n=n, first_n=True, last_n=False)

read_funding_from_db(param_path=param_path, symbol=symbol, n=n, first_n=False, last_n=True)
read_funding_from_db(param_path=param_path, symbol=symbol, n=n, first_n=True, last_n=False)


candle_interval='5m'

read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, type_='spot', n=n, first_n=False, last_n=True)
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, type_='spot', n=n, first_n=True, last_n=False)

read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval,  type_='usd_futs', n=n, first_n=False, last_n=True)
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval,  type_='usd_futs', n=n, first_n=True, last_n=False)



import pandas as pd
symbol='ATOMUSDT'
oi_interval='5m'
n=5

df=read_oi_from_db(param_path=param_path, symbol=symbol, oi_interval=oi_interval, n=5, first_n=False, last_n=True)
pd.DataFrame(df)

symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange = get_directories_from_param_path(param_path)

dir_=usd_futs_oi_dir
db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange)
db_name=f'{symbol}_{oi_interval}_oi.db'
db_dir=f'{dir_}{oi_interval}/'
oi_db_=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=symbol, INTERVAL=oi_interval, READ_ONLY=False, **db_args_dict )
oi_db_.execute(f"DELETE FROM OI_TABLE WHERE oi_time IN (SELECT oi_time ASC LIMIT {n})")

