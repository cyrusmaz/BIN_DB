from read_from_db_fns import *
f

param_path='/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'

symbols = read_symbols_from_db(param_path=param_path)
raw_dump_exchange_infos = read_symbols_from_db(param_path=param_path, raw_dump=True)
type(raw_dump_exchange_infos)
raw_dump_exchange_infos.keys()

SYMBOLS = [s['symbol'] for s in raw_dump_exchange_infos['spot']['symbols']]


with open('data.json', 'w') as outfile:
    json.dump(raw_dump_exchange_infos['usd_futs']['symbols'][0], outfile, indent=4)

raw_dump_exchange_infos['spot']['symbols'][0]

symbols['coin_futs_details']

d=raw_dump_exchange_infos['usd_futs']['symbols'][0]

# from copy import deepcopy
# def get_symbol_info(exchange_info, filterType, info_name, symbols=None):
#     """FINDS THE SYMBOLS INFO FROM FILTERS IN EXCHANGEINFO"""
#     """E.G. USED FOR FINDING TICK SIZE, STEP SIZE"""
#     if symbols is not None: 
#         symbol_infos = list(filter(lambda x: x['symbol'] in symbols, exchange_info['symbols']))
#     else: 
#         symbol_infos = deepcopy(exchange_info['symbols'])
#         # symbol_infos = list(filter(lambda x: x['status']=='TRADING', symbol_infos))

#     output=dict()
#     for symbol_info in symbol_infos:
#         symbol = symbol_info['symbol']
#         # print(symbol)
#         try: 
#             filter_ = list(filter(lambda x: x['filterType']==filterType, symbol_info['filters']))[0]
#             symbol_param = float(filter_[info_name])
            
#         except Exception as e : 
#             print(symbol_info['status'])
#             print(f'{symbol} - {e}')
#             symbol_param=None

#         output[symbol]=symbol_param
#     return output 




# spot_tick_size = get_symbol_info(
#     exchange_info=raw_dump_exchange_infos['spot'], 
#     filterType='PRICE_FILTER', 
#     info_name='tickSize')

# spot_limit_step_size = get_symbol_info(
#     exchange_info=raw_dump_exchange_infos['spot'], 
#     filterType='LOT_SIZE', 
#     info_name='stepSize')

# spot_limit_max_qty = get_symbol_info(
#     exchange_info=raw_dump_exchange_infos['spot'], 
#     filterType='LOT_SIZE', 
#     info_name='maxQty')    

# spot_limit_min_qty = get_symbol_info(
#     exchange_info=raw_dump_exchange_infos['spot'], 
#     filterType='LOT_SIZE', 
#     info_name='minQty')        



raw_dump_exchange_infos['coin_futs_details']

######################### USD FUTS
# TICK SIZE
usd_futs_tick_size = get_symbol_info(exchange_info=raw_dump_exchange_infos['usd_futs'], filterType='PRICE_FILTER', info_name='tickSize')

# LIMIT STEP SIZE
usd_futs_limit_step_size = get_symbol_info(exchange_info=raw_dump_exchange_infos['usd_futs'], filterType='LOT_SIZE', info_name='stepSize')
# LIMIT MAXQTY
usd_futs_limit_max_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['usd_futs'], filterType='LOT_SIZE', info_name='maxQty')    
# LIMIT MINQTY
usd_futs_limit_min_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['usd_futs'], filterType='LOT_SIZE', info_name='minQty')        

# MKT STEP SIZE
usd_futs_market_step_size = get_symbol_info(exchange_info=raw_dump_exchange_infos['usd_futs'], filterType='MARKET_LOT_SIZE', info_name='stepSize')
# MKT MAXQTY
usd_futs_market_max_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['usd_futs'], filterType='MARKET_LOT_SIZE', info_name='maxQty')    
# MKT MINQTY
usd_futs_market_min_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['usd_futs'], filterType='MARKET_LOT_SIZE', info_name='minQty')      
######################### USD FUTS

######################### COIN FUTS
coin_futs_tick_size = get_symbol_info(exchange_info=raw_dump_exchange_infos['coin_futs'], filterType='PRICE_FILTER', info_name='tickSize')

# LIMIT STEP SIZE
coin_futs_limit_step_size = get_symbol_info(exchange_info=raw_dump_exchange_infos['coin_futs'], filterType='LOT_SIZE', info_name='stepSize')
# LIMIT MAXQTY
coin_futs_limit_max_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['coin_futs'], filterType='LOT_SIZE', info_name='maxQty')    
# LIMIT MINQTY
coin_futs_limit_min_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['coin_futs'], filterType='LOT_SIZE', info_name='minQty')        

# MKT STEP SIZE
coin_futs_market_step_size = get_symbol_info(exchange_info=raw_dump_exchange_infos['coin_futs'], filterType='MARKET_LOT_SIZE', info_name='stepSize')
# MKT MAXQTY
coin_futs_market_max_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['coin_futs'], filterType='MARKET_LOT_SIZE', info_name='maxQty')    
# MKT MINQTY
coin_futs_market_min_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['coin_futs'], filterType='MARKET_LOT_SIZE', info_name='minQty')      
######################### COIN FUTS


######################### SPOT
spot_tick_size = get_symbol_info(exchange_info=raw_dump_exchange_infos['spot'], filterType='PRICE_FILTER', info_name='tickSize')

# LIMIT STEP SIZE
spot_limit_step_size = get_symbol_info(exchange_info=raw_dump_exchange_infos['spot'], filterType='LOT_SIZE', info_name='stepSize')
# LIMIT MAXQTY
spot_limit_max_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['spot'], filterType='LOT_SIZE', info_name='maxQty')    
# LIMIT MINQTY
spot_limit_min_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['spot'], filterType='LOT_SIZE', info_name='minQty')        

# MKT STEP SIZE
spot_market_step_size = get_symbol_info(exchange_info=raw_dump_exchange_infos['spot'], filterType='MARKET_LOT_SIZE', info_name='stepSize')
# MKT MAXQTY
spot_market_max_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['spot'], filterType='MARKET_LOT_SIZE', info_name='maxQty')    
# MKT MINQTY
spot_market_min_qty = get_symbol_info(exchange_info=raw_dump_exchange_infos['spot'], filterType='MARKET_LOT_SIZE', info_name='minQty')      
######################### SPOT




# spot_market_step_size = get_symbol_info(
#     # symbols=['BTCUSDT'],
#     exchange_info=raw_dump_exchange_infos['spot'], 
#     filterType='MARKET_LOT_SIZE', 
#     info_name='stepSize')


# raw_dump_exchange_infos['spot']['symbols'][10]

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

