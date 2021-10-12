from read_from_db_fns import *

param_path='/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'

symbols = read_symbols_from_db(param_path=param_path)
raw_dump_exchange_infos = read_symbols_from_db(param_path=param_path, raw_dump=True)
raw_dump_exchange_infos.keys()

SYMBOLS = [s['symbol'] for s in raw_dump_exchange_infos['spot']['symbols']]

### FOR usd_futs and coin_futs
pricePrecision = {item['symbol']:item['pricePrecision'] for item in raw_dump_exchange_infos['usd_futs']['symbols']}
quantityPrecision = {item['symbol']:item['quantityPrecision'] for item in raw_dump_exchange_infos['usd_futs']['symbols']}
tickSize = {item['symbol']:item['quantityPrecision'] for item in raw_dump_exchange_infos['usd_futs']['symbols']}

precisions = {item['symbol']:{'pricePrecision':item['pricePrecision'], 'quantityPrecision':item['quantityPrecision'] } for item in raw_dump_exchange_infos['usd_futs']['symbols']}



precisions = {item['symbol']:{'pricePrecision':item['pricePrecision'], 'quantityPrecision':item['quantityPrecision'] } for item in raw_dump_exchange_infos['spot']['symbols']}

with open('data.json', 'w') as outfile:
    json.dump(raw_dump_exchange_infos['usd_futs']['symbols'][0], outfile, indent=4)

raw_dump_exchange_infos['usd_futs']['symbols'][0]



d=raw_dump_exchange_infos['usd_futs']['symbols'][0]

from copy import deepcopy
def get_symbol_info(exchange_info, filterType, info_name, symbols=None):
    if symbols is not None: 
        symbol_infos = list(filter(lambda x: x['symbol'] in symbols, exchange_info['symbols']))
    else: 
        symbol_infos = deepcopy(exchange_info['symbols'])

    output=dict()
    for symbol_info in symbol_infos:
        symbol = symbol_info['symbol']
        # print(symbol)
        try: 
            filter_ = list(filter(lambda x: x['filterType']==filterType, symbol_info['filters']))[0]
            symbol_param = filter_[info_name]
            output[symbol]=symbol_param
        except Exception as e : 
            print(symbol_info['status'])
            print(f'{symbol} - {e}')

    return output 

usd_futs_tick_size = get_symbol_info(
    exchange_info=raw_dump_exchange_infos['usd_futs'], 
    filterType='PRICE_FILTER', 
    parameter_name='tickSize')

spot_tick_size = get_symbol_info(
    exchange_info=raw_dump_exchange_infos['spot'], 
    filterType='PRICE_FILTER', 
    parameter_name='tickSize')

spot_limit_step_size = get_symbol_info(
    exchange_info=raw_dump_exchange_infos['spot'], 
    filterType='LOT_SIZE', 
    parameter_name='stepSize')

spot_market_step_size = get_symbol_info(
    symbols=['BTCUSDT'],
    exchange_info=raw_dump_exchange_infos['spot'], 
    filterType='LOT_SIZE', 
    parameter_name='stepSize')


# float('0.00000000')



# # 'BCCBTC' in symbols['spot']

# # p=list(filter(lambda x: x['symbol']=='BCCBTC', raw_dump_exchange_infos['spot']['symbols']))[0]['filters']
# # list(filter(lambda x: x['filterType']=='MARKET_LOT_SIZE', p))




symbol='BTCUSDT'
oi_interval='5m'
n=5

read_oi_from_db(param_path=param_path, symbol=symbol, usd_futs=True, coin_futs=False, oi_interval=oi_interval, n=n, first_n=False, last_n=True)
read_oi_from_db(param_path=param_path, symbol=symbol, usd_futs=True, coin_futs=False, oi_interval=oi_interval, n=n, first_n=True, last_n=False)

read_funding_from_db(param_path=param_path, symbol=symbol,usd_futs=True, coin_futs=False, n=n, first_n=False, last_n=True)
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

