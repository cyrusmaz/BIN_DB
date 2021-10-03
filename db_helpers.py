# from binance.client import Client
import datetime
from math import ceil
import logging

import numpy as np
import pandas as pd
import os
import json


def get_directories_from_param_path(param_path):
    with open(param_path,'r') as f: 
        db_parameters= json.load(f)

    symbols_dir = db_parameters['DB_DIRECTORY_SYMBOLS']
    usd_futs_candles_dir = db_parameters['DB_DIRECTORY_CANDLES']+'USD_FUTS/'
    spot_candles_dir = db_parameters['DB_DIRECTORY_CANDLES']+'SPOT/'
    # extract oi and fundiing directories
    usd_futs_oi_dir = db_parameters['DB_DIRECTORY_OI']
    usd_futs_funding_dir =  db_parameters['DB_DIRECTORY_FUNDING']
    # extract exchange and exchange_types 
    exchange=db_parameters['EXCHANGE']
    # exchange_types = db_parameters['EXCHANG_TYPES']
    return symbols_dir, usd_futs_candles_dir, spot_candles_dir, usd_futs_oi_dir, usd_futs_funding_dir, exchange


def batch_symbols_fn(symbols, batch_size):
    """produce an iterato symbols. batch_size has to be <= rate limit"""
    for j in range(ceil(len(symbols)/batch_size)):
        batch_symbols = symbols[int(j*batch_size):int((j+1)*batch_size)]
        yield batch_symbols


def get_symbols(exchange_infos, logger):
    spot_trading = list(filter(lambda x: x['status']== 'TRADING', exchange_infos['spot']['symbols']))
    spot_trading_symbols = [d['symbol'] for d in spot_trading]

    spot_NOT_trading = list(filter(lambda x: x['status']!= 'TRADING', exchange_infos['spot']['symbols']))
    spot_NOT_trading_symbols = [d['symbol'] for d in spot_NOT_trading]

    usd_futs_trading = list(filter(lambda x: x['status']== 'TRADING', exchange_infos['usd_futs']['symbols']))
    usd_futs_trading_symbols = [d['symbol'] for d in usd_futs_trading]

    usd_futs_NOT_trading = list(filter(lambda x: x['status']!= 'TRADING', exchange_infos['usd_futs']['symbols']))
    usd_futs_NOT_trading_symbols = [d['symbol'] for d in usd_futs_NOT_trading]
    if logger is not None:
        logger.info(
            dict(
                origin='get_symbols', 
                payload=dict(
                    spot_trading = len(spot_trading),
                    spot_NOT_trading =len(spot_NOT_trading),
                    usd_futs_trading = len(usd_futs_trading),
                    usd_futs_NOT_trading =len(usd_futs_NOT_trading)                    

                )))

    """CONSUMES DICT OF EXCHANGE INFOS AND EXTRACT SYMBOLS"""
    result = {}
    for t in exchange_infos.keys(): 
        result[t] = [d['symbol'] for d in exchange_infos[t]['symbols']]


    result['spot']=spot_trading_symbols
    result['usd_futs']=usd_futs_trading_symbols

    return result 

def logger_setup(LOG_DIR):
    if not os.path.exists(LOG_DIR):
        print(f"CREATING DIRECTORY: {LOG_DIR}")
        os.makedirs(LOG_DIR)

    today = datetime.datetime.today().strftime('%Y-%m-%d')
    log_destination = LOG_DIR+today+'_log.txt'

    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler = logging.FileHandler(log_destination)        
    handler.setFormatter(formatter)

    name='BINANCE_DB_LOGGER'
    level=logging.INFO
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger




def long_to_datetime(long, utc=True):
    if utc:
        return datetime.datetime.fromtimestamp(long/1000, tz=datetime.timezone.utc)
    else:
        return datetime.datetime.fromtimestamp(long/1000)


def interval_length_ms(interval):
    if interval=='1m': return 60000
    elif interval=="3m": return 60000*3
    elif interval=="5m" : return 60000*5
    elif interval=="15m": return 60000*15
    elif interval=="30m": return 60000*30
    elif interval=="1h": return 60000*60
    elif interval=="2h": return 60000*120
    elif interval=="4h": return 60000*240
    elif interval=="6h": return 60000*360
    elif interval=="8h": return 60000*480
    elif interval=="12h": return 60000*12*60
    elif interval=="1d": return 60000*24*60
    elif interval=="3d": return 60000*3*24*60
    elif interval=="1w": return 60000*7*24*60



def time_inconsistencies(series, interval, return_str=True, utc=True):
    interval_len = interval_length_ms(interval)
    n=pd.Series(np.diff(series,1)!=interval_len)
    numeric_output = [[series[i],series[i+1]]for i in  n[n].index.values]
    if return_str is False: 
        return numeric_output
    else: 
        return [ [long_to_datetime_str(s, utc) for s in t] for t in numeric_output    ]

def equidistant_timestamps(series, interval):
    interval_len = interval_length_ms(interval)
    d=series[1]-series[0]
    if sum(np.diff(series,1)!=interval_len)!=0:
        return False
    else: 
        return True

# def get_all_symbols():
#     x=Client(None, None)
#     spot_info=x.get_exchange_info()
#     all_spot_symbols=[s['symbol'] for s in spot_info['symbols']]
#     usdt =list(filter(lambda x: len(x.split('USDT'))==2 and x.split('USDT')[1]=='', all_spot_symbols))
#     btc =list(filter(lambda x: len(x.split('BTC'))==2 and x.split('BTC')[1]=='', all_spot_symbols))
#     eth =list(filter(lambda x: len(x.split('ETH'))==2 and x.split('ETH')[1]=='', all_spot_symbols))
#     bnb =list(filter(lambda x: len(x.split('BNB'))==2 and x.split('BNB')[1]=='', all_spot_symbols))
#     return usdt, btc, eth, bnb

quote_filter = lambda quote, symbols: list(filter(lambda x: len(x.split(quote))==2 and x.split(quote)[1]=='', symbols))
base_filter = lambda quote, symbols: list(filter(lambda x: len(x.split(quote))==2 and x.split(quote)[0]=='', symbols))
requote_map = lambda old_quote, new_quote, symbols: list(map(lambda x: x.split(old_quote)[0]+new_quote if len(x.split(old_quote))==2 else None, symbols ))


# def get_futs_symbols():
#     x=Client(None, None)
#     spot_info=x.get_exchange_info()
#     futs_info=x.futures_exchange_info()
#     all_spot_symbols=[s['symbol'] for s in spot_info['symbols']]
#     futs=[s['symbol'] for s in futs_info['symbols']]
#     def btc(s):
#         return s.split('USDT')[0]+'BTC'

#     def eth(s):
#         return s.split('USDT')[0]+'ETH'
    
#     def bnb(s):
#         return s.split('USDT')[0]+'BNB'        

#     futs_btc=[btc(s) for s in futs]
#     futs_eth=[eth(s) for s in futs]
#     futs_bnb=[bnb(s) for s in futs]

#     futs_usdt=list(filter(lambda x: x in all_spot_symbols, futs))
#     futs_btc=list(filter(lambda x: x in all_spot_symbols, futs_btc))
#     futs_eth=list(filter(lambda x: x in all_spot_symbols, futs_eth))
#     futs_bnb=list(filter(lambda x: x in all_spot_symbols, futs_bnb))
#     return futs, futs_usdt, futs_btc, futs_eth, futs_bnb, all_spot_symbols



def datetime_to_long(year, month, day, hour=0, minute=0, second=0):
    dt = datetime.datetime(year, month, day, hour=0, minute=0, second=0, microsecond=0)
    return (dt - datetime.datetime.fromtimestamp(0)).total_seconds()*1000

def long_to_datetime_str(long, utc=True):
    if long is None: return ''
    if utc:
        return datetime.datetime.fromtimestamp(long/1000, tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    else:
        return datetime.datetime.fromtimestamp(long/1000).strftime('%Y-%m-%d %H:%M:%S')        

def datetime_to_str(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')

# def now_utc_long():
#     return (datetime.datetime.now() - datetime.datetime.fromtimestamp(0)).total_seconds()*1000
