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



