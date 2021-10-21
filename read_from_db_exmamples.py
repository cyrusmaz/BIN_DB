param_path='/home/cm/Documents/PY_DEV/DB_BIN/params.json'
from DB_reader import DB_reader
dbr = DB_reader(param_path=param_path)
dbr.symbols['coinf_details']

symbol='BTCUSDT'
n=5
first_n=True
last_n=False
dbr.get_usdf_funding(symbol=symbol, n=n, first_n=first_n, last_n=last_n, return_df=True, ISO=False, utc=True) 
dbr.get_usdf_candles(symbol=symbol, n=n, interval='5m', first_n=first_n, last_n=last_n, return_df=False, ISO=False, utc=True)
dbr.get_usdf_index(symbol=symbol, n=n, interval='5m', first_n=first_n, last_n=last_n, return_df=False, ISO=False, utc=True)
dbr.get_usdf_mark(symbol=symbol, n=n, interval='5m', first_n=first_n, last_n=last_n, return_df=False, ISO=False, utc=True)

dbr.get_spot_candles(symbol=symbol, n=n, interval='5m', first_n=first_n, last_n=last_n, return_df=False, ISO=False, utc=True)

symbol='BTCUSD_PERP'
dbr.get_coinf_funding(symbol=symbol, n=n, first_n=first_n, last_n=last_n, return_df=True, ISO=False, utc=True)
dbr.get_coinf_candles(symbol=symbol, n=n, interval='5m', first_n=first_n, last_n=last_n, return_df=True, ISO=False, utc=True)
dbr.get_coinf_index(symbol=symbol, n=n, interval='5m', first_n=first_n, last_n=last_n, return_df=True, ISO=False, utc=True)
dbr.get_coinf_mark(symbol=symbol, n=n, interval='5m', first_n=first_n, last_n=last_n, return_df=True, ISO=False, utc=True)

























dbr.symbol_info['spot']['tick_size']['BTCUSDT']
from read_from_db_fns import *
from exchange_info_parse_fns import *
oi_interval ='5m'
read_oi_from_db(param_path=dbr.param_path, symbol=symbol, usd_futs=True, coin_futs=False, oi_interval=oi_interval, n=n, first_n=False, last_n=True)
read_oi_from_db(param_path=param_path, symbol=symbol, usd_futs=True, coin_futs=False, oi_interval=oi_interval, n=n, first_n=True, last_n=False)

read_funding_from_db(param_path=dbr.param_path, symbol=symbol,usd_futs=True, coin_futs=False, n=n, first_n=False, last_n=True)
read_funding_from_db(param_path=param_path, symbol=symbol, n=n, usd_futs=True, coin_futs=False, first_n=True, last_n=False)



# COIN FUTS:
symbol='BTCUSD_PERP'
read_oi_from_db(param_path=param_path, symbol=symbol, usd_futs=False, coin_futs=True, oi_interval=oi_interval, n=n, first_n=False, last_n=True)
read_oi_from_db(param_path=param_path, symbol=symbol, usd_futs=False, coin_futs=True, oi_interval=oi_interval, n=n, first_n=True, last_n=False)

read_funding_from_db(param_path=param_path, symbol=symbol,usd_futs=False, coin_futs=True, n=n, first_n=False, last_n=True)
read_funding_from_db(param_path=param_path, symbol=symbol, n=n, usd_futs=False, coin_futs=True, first_n=True, last_n=False)


candle_interval='5m'

# SPOT
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=False, coin_futs=False, mark=False, index=False, n=n, first_n=False, last_n=True)
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=False, coin_futs=False, mark=False, index=False, n=n, first_n=True, last_n=False)

# USD FUTS
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=True, coin_futs=False, mark=False, index=False, n=n, first_n=False, last_n=True)
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=True, coin_futs=False, mark=False, index=False, n=n, first_n=True, last_n=False)

read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=True, coin_futs=False, mark=True, index=False, n=n, first_n=False, last_n=True)
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=True, coin_futs=False, mark=True, index=False, n=n, first_n=True, last_n=False)

read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=True, coin_futs=False, mark=False, index=True, n=n, first_n=False, last_n=True)
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=True, coin_futs=False, mark=False, index=True, n=n, first_n=True, last_n=False)


symbol='BTCUSD_PERP'
# COIN FUTS: 
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=False, coin_futs=True, mark=False, index=False, n=n, first_n=False, last_n=True)
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=False, coin_futs=True, mark=False, index=False, n=n, first_n=True, last_n=False)

read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=False, coin_futs=True, mark=True, index=False, n=n, first_n=False, last_n=True)
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=False, coin_futs=True, mark=True, index=False, n=n, first_n=True, last_n=False)

symbol='BTCUSD'
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=False, coin_futs=True, mark=False, index=True, n=n, first_n=False, last_n=True)
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval, usd_futs=False, coin_futs=True, mark=False, index=True, n=n, first_n=True, last_n=False)





