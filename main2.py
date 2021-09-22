
#TODO
# have the log record inception time when candlebackfill/oibackfill has reached inception



from API_RATES import FUTS_CANDLE_RATE_LIMIT, FUTS_OI_RATE_LIMIT, FUTS_FUNDING_RATE_LIMIT, SPOT_CANDLE_RATE_LIMIT
import asyncio 
# import aiohttp
# import os
import time
# from math import ceil

from db_helpers import *
from async_fns import get_infos
from SymbolsDBClass import symbols_db
# from CandleDBClass import candle_db
# from OIDBClass import oi_db
# from FundingDBClass import funding_db


from fill_wrappers import * 
# import argparse



# EXCHANGE = 'binance'
# wait = lambda: time.sleep(60)
import json

db_parameters = dict(
    EXCHANGE             = 'binance',
    EXCHANG_TYPES        = ['usd_futs', 'coin_futs', 'spot'],
    DB_DIRECTORY         = '/mnt/nvme1n1/DB/BINANCE/',
    DB_DIRECTORY_CANDLES = '/mnt/nvme1n1/DB/BINANCE/CANDLES/',
    DB_DIRECTORY_OI      = '/mnt/nvme1n1/DB/BINANCE/OI/',
    DB_DIRECTORY_FUNDING = '/mnt/nvme1n1/DB/BINANCE/FUNDING/',
    DB_DIRECTORY_SYMBOLS = '/mnt/nvme1n1/DB/BINANCE/SYMBOLS/',
    # UPDATE_INTERVAL      = 5, 
    CANDLE_INTERVAL      = '5m',
    OI_INTERVAL          = '5m',
    SPOT_CANDLE_QUOTES   = ['USDT', 'BTC', 'ETH', ], 
    LOG_DIRECTORY        = '/mnt/nvme1n1/DB/BINANCE/LOGS/',

    SPOT_CANDLE_LIMIT=1000,
    SPOT_CANDLE_RATE_LIMIT=1200,

    FUTS_CANDLE_LIMIT=499,
    FUTS_CANDLE_RATE_LIMIT=2400/2,

    FUTS_OI_LIMIT=500,
    FUTS_OI_RATE_LIMIT=2400,

    FUTS_FUNDING_LIMIT=1000,
    FUTS_FUNDING_RATE_LIMIT=2400,

)

# with open('/mnt/nvme1n1/DB/BINANCE/params.json','w') as f: 
#     json.dump(db_parameters,f, indent=3)

# with open('/mnt/nvme1n1/DB/BINANCE/params.json','r') as f: 
#     s= json.load(f)

###### START MAIN 

# set up logger
logger = logger_setup(LOG_DIR=db_parameters['LOG_DIRECTORY'])

# extract candle and oi intervals
candle_interval = db_parameters['CANDLE_INTERVAL']
oi_interval = db_parameters['OI_INTERVAL']

# extract candles directory
usd_futs_candles_dir = db_parameters['DB_DIRECTORY_CANDLES']+'USD_FUTS/'
spot_candles_dir = db_parameters['DB_DIRECTORY_CANDLES']+'SPOT/'

# extract oi and fundiing directories
usd_futs_oi_dir = db_parameters['DB_DIRECTORY_OI']
usd_futs_funding_dir =  db_parameters['DB_DIRECTORY_FUNDING']

# extract exchange and exchange_types 
exchange=db_parameters['EXCHANGE']

# set up symbols database 
db_symbols=symbols_db(DB_DIRECTORY=db_parameters['DB_DIRECTORY_SYMBOLS'],DB_NAME='symbols.db',EXCHANGE='binance', LOGGER=logger)

# grab symbols from exchange and update the db
# exchange_types = db_parameters['EXCHANG_TYPES']
# exchange_infos = asyncio.run(get_infos(types=exchange_types, logger=logger))
# new_symbols_dict = get_symbols(exchange_infos=exchange_infos, logger=logger)
# db_symbols.update_symbols_db(new_symbols_dict)

# grab last symbol insert from database 
symbols = db_symbols.get_last()

# instantiate symbols of interest (spot and usd futs)
usd_futs_symbols_of_interest = symbols['usd_futs']
spot_symbols_of_interest=symbols['spot']


# quote_filter('BNB', symbols['spot'])
# base_filter('BNB', symbols['spot'])

# quote_filter('USDT', symbols['usd_futs'])
# requote_map('USDT', 'ETH', symbols['usd_futs'])
# requote_map('BUSD', 'ETH', symbols['usd_futs'])

SPOT_CANDLE_LIMIT=db_parameters['SPOT_CANDLE_LIMIT']
SPOT_CANDLE_RATE_LIMIT=db_parameters['SPOT_CANDLE_RATE_LIMIT']
FUTS_CANDLE_LIMIT=db_parameters['FUTS_CANDLE_LIMIT']
FUTS_CANDLE_RATE_LIMIT=db_parameters['FUTS_CANDLE_RATE_LIMIT']
FUTS_OI_LIMIT=db_parameters['FUTS_OI_LIMIT']
FUTS_OI_RATE_LIMIT=db_parameters['FUTS_OI_RATE_LIMIT']
FUTS_FUNDING_LIMIT=db_parameters['FUTS_FUNDING_LIMIT']
FUTS_FUNDING_RATE_LIMIT=db_parameters['FUTS_FUNDING_RATE_LIMIT']   


# logger.critical('test')

####### FUTS OI
symbols_exist, dbs_exist,symbols_dne,dbs_dne = prepare_for_oi_fetch(
    dir_=usd_futs_oi_dir, 
    symbols=usd_futs_symbols_of_interest, 
    oi_interval=oi_interval, 
    check_existence=False,
    db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange))

# dbs_exist['FTTBUSD'].query("""SELECT * FROM INFO_TABLE""")

## FORWARD FILL OI FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
oi_fill_wrapper(
    symbols=symbols_exist, 
    dbs=dbs_exist, 
    interval=oi_interval, 
    forward=True, 
    batch_size=FUTS_OI_RATE_LIMIT/4, 
    logger=logger)

## BACKFILL OI FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
oi_fill_wrapper(
    symbols=symbols_exist, 
    dbs=dbs_exist, 
    interval=oi_interval, 
    forward=False, 
    batch_size=FUTS_OI_RATE_LIMIT/4, 
    logger=logger)    

# BACKFILL OI FOR THOSE SYMBOLS FOR WHICH get_last() returns None
oi_fill_wrapper(
    symbols=symbols_dne, 
    dbs=dbs_dne, 
    interval=oi_interval, 
    forward=False, 
    batch_size=FUTS_OI_RATE_LIMIT/4, 
    logger=logger)   
####### FUTS OI




####### FUTS FUNDING
symbols, dbs_funding = prepare_for_funding_fetch(
    dir_=usd_futs_funding_dir, 
    symbols=usd_futs_symbols_of_interest, 
    db_args_dict=dict(TYPE='funding', EXCHANGE=exchange))

funding_fill_wrapper(
    symbols=symbols, 
    dbs=dbs_funding, 
    batch_size=FUTS_FUNDING_RATE_LIMIT/4, 
    logger=None)
####### FUTS FUNDING





# dbs_exist['ZRXUSDT'].query("SELECT * FROM CANDLE_TABLE WHERE type='usd_futs'")
# dbs_exist['ZRXUSDT'].execute("DELETE FROM CANDLE_TABLE WHERE type='usd_futs'")
# candle_interval='5m'
# symbols_exist,dbs_exist,symbols_dne,dbs_dne = prepare_for_candle_fetch(
#     dir_=spot_candles_dir, 
#     symbols=spot_symbols_of_interest, 
#     candle_interval=candle_interval, 
#     db_args_dict=dict(TYPE='spot', EXCHANGE=exchange),
#     check_existence=False)

# for k,v in dbs_exist.items():
#     v.execute("DELETE FROM CANDLE_TABLE WHERE type='usd_futs'")



####### SPOT CANDLES
symbols_exist,dbs_exist,symbols_dne,dbs_dne = prepare_for_candle_fetch(
    dir_=spot_candles_dir, 
    symbols=spot_symbols_of_interest, 
    candle_interval=candle_interval, 
    check_existence=True,
    db_args_dict=dict(TYPE='spot', EXCHANGE=exchange))

len(symbols_exist)
len(dbs_exist)

len(symbols_dne)
len(dbs_dne)

## FORWARD FILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
candle_fill_wrapper(
    symbols=symbols_exist, 
    dbs=dbs_exist, 
    interval=candle_interval, 
    futs=False, 
    forward=True, 
    batch_size=SPOT_CANDLE_RATE_LIMIT/4, 
    logger=logger)

## BACKFILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
candle_fill_wrapper(
    symbols=symbols_exist, 
    dbs=dbs_exist, 
    interval=candle_interval, 
    futs=False, 
    forward=False, 
    batch_size=SPOT_CANDLE_RATE_LIMIT/4, 
    logger=logger)

## BACKFILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns None
candle_fill_wrapper(
    symbols=symbols_dne, 
    dbs=dbs_dne, 
    interval=candle_interval, 
    futs=False, 
    # backfill=3,
    forward=False, 
    batch_size=SPOT_CANDLE_RATE_LIMIT/4, 
    logger=logger)
####### SPOT CANDLES 




####### FUTS CANDLES
symbols_exist,dbs_exist,symbols_dne,dbs_dne = prepare_for_candle_fetch(
    dir_=usd_futs_candles_dir, 
    symbols=usd_futs_symbols_of_interest, 
    candle_interval=candle_interval, 
    check_existence=True,
    db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange))

len(symbols_exist)
len(dbs_exist)

len(symbols_dne)
len(dbs_dne)

## FORWARD FILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
candle_fill_wrapper(
    symbols=symbols_exist, 
    dbs=dbs_exist, 
    interval=candle_interval, 
    futs=True, 
    forward=True, 
    batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
    logger=logger)

## BACKFILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
candle_fill_wrapper(
    symbols=symbols_exist, 
    dbs=dbs_exist, 
    interval=candle_interval, 
    futs=True, 
    forward=False, 
    batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
    logger=logger)

## BACKFILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns None
candle_fill_wrapper(
    symbols=symbols_dne, 
    dbs=dbs_dne, 
    interval=candle_interval, 
    futs=True, 
    # backfill=3,
    forward=False, 
    batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
    logger=logger)

####### FUTS CANDLES







