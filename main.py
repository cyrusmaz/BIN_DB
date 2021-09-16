

from API_RATES import FUTS_CANDLE_RATE_LIMIT, FUTS_OI_RATE_LIMIT, FUTS_FUNDING_RATE_LIMIT, SPOT_CANDLE_RATE_LIMIT
# import asyncio 
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

# from candles_backfill import candles_backfill_fn
# from candles_forwardfill import candles_forwardfill_fn

# from oi_backfill import oi_backfill_fn
# from oi_forwardfill import oi_forwardfill_fn
# from funding_forwardfill import funding_forwardfill_fn

from fill_wrappers import * 




EXCHANGE = 'binance'
# wait = lambda: time.sleep(60)


db_parameters = dict(
    EXCHANG_TYPES        = ['usd_futs', 'coin_futs', 'spot'],
    DB_DIRECTORY         = '/home/cm/Documents/PY_DEV/DB/BINANCE/',
    DB_DIRECTORY_CANDLES = '/home/cm/Documents/PY_DEV/DB/BINANCE/CANDLES/',
    DB_DIRECTORY_OI      = '/home/cm/Documents/PY_DEV/DB/BINANCE/OI/',
    DB_DIRECTORY_FUNDING = '/home/cm/Documents/PY_DEV/DB/BINANCE/FUNDING/',
    DB_DIRECTORY_SYMBOLS = '/home/cm/Documents/PY_DEV/DB/BINANCE/SYMBOLS/',
    UPDATE_INTERVAL      = 5, 
    CANDLE_INTERVAL      = '5m',
    OI_INTERVAL          = '5m',
    SPOT_CANDLE_QUOTES   = ['USDT', 'BTC', 'ETH', ], 
    LOG_DIRECTORY        = '/home/cm/Documents/PY_DEV/DB/BINANCE/LOGS/'
)
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

# set up symbols database 
db_symbols=symbols_db(DB_DIRECTORY=db_parameters['DB_DIRECTORY_SYMBOLS'],DB_NAME='symbols.db',EXCHANGE='binance', LOGGER=logger)

# grab symbols from exchange 
# exchange_infos = asyncio.run(get_infos(types=EXCHANG_TYPES, logger=logger))
# new_symbols_dict = get_symbols(exchange_infos=exchange_infos, logger=logger)

# update symbols database 
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

# logger.critical('test')

####### FUTS OI
# oi_interval='5m'

db_exists_oi_symbols = []
db_dne_oi_symbols = []
db_forward_fill_oi_db = {}
db_backfill_oi_db = {}
# ESTABLISH WHICH SYMBOL HAS AN EXISTING DB WITH AT LEAST ONE RECORD, AND WHICH DOESN'T
for s in usd_futs_symbols_of_interest:
    db_name=f'{s}_{oi_interval}_oi.db'
    db_dir=f'{usd_futs_oi_dir}{oi_interval}/'
    db=oi_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=s, INTERVAL=oi_interval, TYPE='usd_futs', EXCHANGE=EXCHANGE)

    if db.get_last() is not None: 
        db_exists_oi_symbols.append(s)
        db_forward_fill_oi_db[s]=db
    else:
        db_dne_oi_symbols.append(s)
        db_backfill_oi_db[s]=db




## FORWARD FILL OI FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
oi_fill_wrapper(
    symbols=db_exists_oi_symbols, 
    dbs=db_forward_fill_oi_db, 
    interval=oi_interval, 
    forward=True, 
    batch_size=FUTS_OI_RATE_LIMIT/4, 
    logger=logger)

## BACKFILL OI FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
oi_fill_wrapper(
    symbols=db_exists_oi_symbols, 
    dbs=db_forward_fill_oi_db, 
    interval=oi_interval, 
    forward=False, 
    batch_size=FUTS_OI_RATE_LIMIT/4, 
    logger=logger)    

# BACKFILL OI FOR THOSE SYMBOLS FOR WHICH get_last() returns None
oi_fill_wrapper(
    symbols=db_dne_oi_symbols, 
    dbs=db_backfill_oi_db, 
    interval=oi_interval, 
    forward=False, 
    batch_size=FUTS_OI_RATE_LIMIT/4, 
    logger=logger)   
####### FUTS OI




####### FUTS FUNDING
db_funding_db = {}
for s in usd_futs_symbols_of_interest:
    db_name=f'{s}_funding.db'
    db_funding_db[s]=funding_db(SYMBOL=s, DB_DIRECTORY=usd_futs_funding_dir, DB_NAME=db_name, TYPE='funding', EXCHANGE=EXCHANGE)

funding_fill_wrapper(
    symbols=usd_futs_symbols_of_interest, 
    dbs=db_funding_db, 
    batch_size=FUTS_FUNDING_RATE_LIMIT/4, 
    logger=None)
####### FUTS FUNDING




####### SPOT CANDLES
# candle_interval='15m'

db_exists_candles_symbols = []
db_dne_candles_symbols = []
db_forward_fill_candles_db = {}
db_backfill_candles_db = {}
# ESTABLISH WHICH SYMBOL HAS AN EXISTING DB WITH AT LEAST ONE RECORD, AND WHICH DOESN'T
for s in spot_symbols_of_interest:
    db_name=f'{s}_{candle_interval}_candle.db'
    db_dir = f'{spot_candles_dir}{candle_interval}/'
    db=candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=s, INTERVAL=candle_interval, TYPE='spot', EXCHANGE=EXCHANGE)
    if db.get_last() is not None:
        db_forward_fill_candles_db[s]=db
        db_exists_candles_symbols.append(s)
    else: 
        db_backfill_candles_db[s]=db
        db_dne_candles_symbols.append(s)
# s
len(db_exists_candles_symbols)
len(db_forward_fill_candles_db)

len(db_dne_candles_symbols)
len(db_backfill_candles_db)

## FORWARD FILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
candle_fill_wrapper(
    symbols=db_exists_candles_symbols, 
    dbs=db_forward_fill_candles_db, 
    interval=candle_interval, 
    futs=False, 
    forward=True, 
    batch_size=SPOT_CANDLE_RATE_LIMIT/4, 
    logger=logger)

## BACKFILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns None
candle_fill_wrapper(
    symbols=db_dne_candles_symbols, 
    dbs=db_backfill_candles_db, 
    interval=candle_interval, 
    futs=False, 
    # backfill=3,
    forward=False, 
    batch_size=SPOT_CANDLE_RATE_LIMIT/4, 
    logger=logger)

## BACKFILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
candle_fill_wrapper(
    symbols=db_exists_candles_symbols, 
    dbs=db_forward_fill_candles_db, 
    interval=candle_interval, 
    futs=False, 
    forward=False, 
    batch_size=SPOT_CANDLE_RATE_LIMIT/4, 
    logger=logger)
# SPOT CANDLES 




####### FUTS CANDLES
db_exists_candles_symbols = []
db_dne_candles_symbols = []
db_forward_fill_candles_db = {}
db_backfill_candles_db = {}
# ESTABLISH WHICH SYMBOL HAS AN EXISTING DB WITH AT LEAST ONE RECORD, AND WHICH DOESN'T
for s in usd_futs_symbols_of_interest:
    db_name=f'{s}_{candle_interval}_candle.db'
    db_dir = f'{usd_futs_candles_dir}{candle_interval}/'
    db=candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, SYMBOL=s, INTERVAL=candle_interval, TYPE='usd_futs', EXCHANGE=EXCHANGE)
    if db.get_last() is not None:
        db_forward_fill_candles_db[s]=db
        db_exists_candles_symbols.append(s)
    else: 
        db_backfill_candles_db[s]=db
        db_dne_candles_symbols.append(s)

len(db_exists_candles_symbols)
len(db_forward_fill_candles_db)

len(db_dne_candles_symbols)
len(db_backfill_candles_db)


## FORWARD FILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
candle_fill_wrapper(
    symbols=db_exists_candles_symbols, 
    dbs=db_forward_fill_candles_db, 
    interval=candle_interval, 
    futs=True, 
    forward=True, 
    batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
    logger=logger)

## BACKFILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns None
candle_fill_wrapper(
    symbols=db_dne_candles_symbols, 
    dbs=db_backfill_candles_db, 
    interval=candle_interval, 
    futs=True, 
    # backfill=3,
    forward=False, 
    batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
    logger=logger)

## BACKFILL CANDLES FOR THOSE SYMBOLS FOR WHICH get_last() returns not None
candle_fill_wrapper(
    symbols=db_exists_candles_symbols, 
    dbs=db_forward_fill_candles_db, 
    interval=candle_interval, 
    futs=True, 
    forward=False, 
    batch_size=SPOT_CANDLE_RATE_LIMIT/4, 
    logger=logger)
# FUTS CANDLES 







