from read_from_db_fns import *
from db_helpers import *

param_path='/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'

read_symbols_from_db(param_path=param_path)


import os 
os.chdir('/home/cm/Documents/PY_DEV/DB/BINANCE/CANDLES/USD_FUTS/4h')
files = os.listdir()
news = list(filter(lambda x: len(x.split('candle.db'))==2 and x.split('candle.db')[1]=='',files  ))

for s in news: 
    os.rename(s, s.split('candle.db')[0]+'usdf_candles.db')


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

read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval,  type_='usdf', n=n, first_n=False, last_n=True)
read_candle_from_db(param_path=param_path, symbol=symbol, candle_interval=candle_interval,  type_='usdf', n=n, first_n=True, last_n=False)




from fill_wrappers import * 
db=oi_db(DB_DIRECTORY='./', DB_NAME='tests.db', SYMBOL='BTCUSDT', INTERVAL='5m', TYPE='usdf', EXCHANGE='exchange2' )

oi_backfill_fn(symbols=['BTCUSDT'],dbs={'BTCUSDT':db}, interval='5m', backfill=None,logger=None)


from read_from_db_fns import *
from delete_from_db_fns import *
param_path='/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'

symbols = read_symbols_from_db(param_path=param_path)
for symbol in symbols['usdf']:
    delete_oi_from_db(param_path=param_path, symbol=symbol, oi_interval='5m', n=6000, first_n=False, last_n=True)

# delete_oi_from_db(param_path=param_path, symbol='ETHUSDT', oi_interval='5m', n=3000, first_n=False, last_n=True)
# # delete_oi_from_db(param_path=param_path, symbol='BTCUSDT', oi_interval='5m', n=500, first_n=True, last_n=False)


import pandas as pd


symbol='BTCUSDT'
oi_interval='1h'
candle_interval='1h'
n=5


df = read_candle_from_db(
                param_path=param_path, 
                symbol=symbol, 
                candle_interval=candle_interval, 
                type_='spot', 
                n=n, 
                first_n=False, 
                last_n=True)

CANDLEMAP = [
    'open_utc', 
    'open', 
    'high', 
    'low', 
    'close',
    'volume',
    'close_utc',
    'quote_asset_volume',
    'num_trades', 
    'taker_buy_base', 
    'taker_buy_quote',
    'ignore']

df = pd.DataFrame(
    data=df, 
    columns=CANDLEMAP,
    dtype=float,
    )

def long_to_datetime_str(long, utc=True, ISO=False):
    if utc:
        dt=datetime.datetime.fromtimestamp(long/1000, tz=datetime.timezone.utc)
    else:
        dt=datetime.datetime.fromtimestamp(long/1000)
    if ISO: 
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ') 
    else: 
        return dt.strftime('%Y-%m-%d %H:%M:%S') 

df['time']=df['open_utc'].apply(long_to_datetime_str,ISO=True, utc=True)
df.drop(columns=['ignore'], inplace=True)

# df.set_index('time',inplace=True)
# df.sort_index(axis=0, inplace=True)


oi_df=read_oi_from_db(param_path=param_path, symbol=symbol, oi_interval=candle_interval, n=n, first_n=False, last_n=True)
oi_df=pd.DataFrame(oi_df, dtype=float)

oi_df['time'] = oi_df['timestamp'].apply(long_to_datetime_str,ISO=True, utc=True)
oi_df.drop(columns=['symbol'], inplace=True)
# oi_df.set_index('time',inplace=True)
# oi_df.sort_index(axis=0, inplace=True)
oi_df



df=df.merge(oi_df, how='outer', on='time')
df

# df.set_index('time',inplace=True)
# df.sort_index(axis=0, inplace=True)








param_path=param_path
symbol='HOTUSDT'
oi_interval='5m'




# symbols_dir, usdf_candles_dir, spot_candles_dir, usdf_oi_dir, usdf_funding_dir, exchange = get_directories_from_param_path(param_path)
r = get_directories_from_param_path(param_path)
exchange = r['exchange']
usdf_oi_dir =r['usdf_oi_dir']
coinf_oi_dir =r['coinf_oi_dir']

# if usdf:
db_name=f'{symbol}_{oi_interval}_usdf_oi.db'
db_dir=f'{usdf_oi_dir}{oi_interval}/'
db_args_dict=dict(TYPE='usdf', EXCHANGE=exchange)    


# elif coinf: 
db_name=f'{symbol}_{oi_interval}_coinf_oi.db'
db_dir=f'{coinf_oi_dir}{oi_interval}/'  
db_args_dict=dict(TYPE='coinf', EXCHANGE=exchange)

from DB_class_oi import oi_db
oi_db_=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=symbol, INTERVAL=oi_interval, READ_ONLY=False, **db_args_dict )
oi_db_.get_last()
oi_db_.delete_last()

n=5
