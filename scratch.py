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




from fill_wrappers import * 
db=oi_db(DB_DIRECTORY='./', DB_NAME='tests.db', SYMBOL='BTCUSDT', INTERVAL='5m', TYPE='usd_futs', EXCHANGE='exchange2' )

oi_backfill_fn(symbols=['BTCUSDT'],dbs={'BTCUSDT':db}, interval='5m', backfill=None,logger=None)


from read_from_db_fns import *
from delete_from_db_fns import *
param_path='/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'

symbols = read_symbols_from_db(param_path=param_path)
for symbol in symbols['usd_futs']:
    delete_oi_from_db(param_path=param_path, symbol=symbol, oi_interval='5m', n=6000, first_n=False, last_n=True)

# delete_oi_from_db(param_path=param_path, symbol='ETHUSDT', oi_interval='5m', n=3000, first_n=False, last_n=True)
# # delete_oi_from_db(param_path=param_path, symbol='BTCUSDT', oi_interval='5m', n=500, first_n=True, last_n=False)


