from db_helpers import *
# from API_RATES import *
from async_fns import get_candles

import asyncio
import time
# from copy import deepcopy
from math import ceil
import datetime

def candles_backfill_fn(symbols,  interval, backfill, usd_futs, coin_futs, mark,limit, rate_limit, index, dbs=None, logger=None, memory_efficient=True):
    """ for backfilling candles start at present and go backward 
        until either backfill is reached or no new data comes in on each subsequent request"""

    # if not usd_futs and not coin_futs:
    #     limit=SPOT_CANDLE_LIMIT
    #     rate_limit = SPOT_CANDLE_RATE_LIMIT

    # elif usd_futs and not coin_futs: 
    #     limit = USD_FUTS_CANDLE_LIMIT
    #     rate_limit=USD_FUTS_CANDLE_RATE_LIMIT
    
    # elif not usd_futs and coin_futs: 
    #     limit = COIN_FUTS_CANDLE_LIMIT
    #     rate_limit = COIN_FUTS_CANDLE_RATE_LIMIT


    # symbols = deepcopy(symbols)
    if symbols is None or len(symbols)==0:
        return None

    interval_len = interval_length_ms(interval)
    j=0
    
    start_time = datetime.datetime.now()
    current_minute_weight = 0
    total_requests_per_symbol =0 

    # endTimes=[data[symbol][0][0]- interval_len for symbol in symbols]
    last_insert_print = {k:None for k in symbols}
    
    endTimes_prev =[]

    for symbol in symbols:
        if dbs is not None: 
            last_candle = dbs[symbol].get_first()
            if last_candle is not None:
                endTime = last_candle[0]
                endTimes_prev.append(endTime- interval_len)  
            else:
                endTimes_prev.append(None)                  
        else:
            endTimes_prev.append(None)


    if len(endTimes_prev)==0:
        endTimes_prev=None

    data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, endTimes=endTimes_prev, usd_futs=usd_futs, coin_futs=coin_futs, mark=mark, index=index, logger=logger)))
    current_minute_weight += len(symbols)
    

    for s in range(len(symbols)):
        # last_insert_print[symbol] = data[symbols[s]][0][0]
        if len(data[symbols[s]])>0: last_insert_print[symbols[s]]=data[symbols[s]][0][0]
        if dbs is not None: 
            if len(data[symbols[s]])>0:
                if len(endTimes_prev)>0 and data[symbols[s]][0][0]!=endTimes_prev[s]:
                    dbs[symbols[s]].insert_multiple(data[symbols[s]])

    pops = list(filter(lambda x: len(data[symbols.copy()[x]])==0, range(len(symbols))))
    pops = [symbols[p] for p in pops]

    if len(pops)>0: 
        if logger is not None: 
            logger.info(
                dict(
                    origin='candles_backfill_fn',
                    payload=dict(
                        reason='backfilling from inception - zero results',
                        interval=interval,
                        usd_futs=usd_futs, coin_futs=coin_futs, 
                        num_dropped_symbols=len(pops),
                        dropped_symbols=pops,
                        )))  

        for s in pops: 
            endTimes_prev.pop(symbols.index(s))
            if dbs is not None:
                del dbs[s]
            # symbols.pop(symbols.index(s))
            del symbols[symbols.index(s)]
            # symbols.remove(s)

    total_requests_per_symbol += 1

    j+=1
    # print(symbols)
    print('j={}'.format(j))
    print('current_minute_weight: {}'.format(current_minute_weight))
    print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    
    endTimes_prev = []

    if backfill is None: while_condition=lambda: True 
    else: while_condition=lambda: ceil(backfill/limit)>total_requests_per_symbol

    while while_condition() is True:
        # print(j)
        start_time = datetime.datetime.now()
        if current_minute_weight + len(symbols) >= rate_limit:
            sleep_time = 61 - (datetime.datetime.now()-start_time).total_seconds()
            sleep_time = max(sleep_time, 30)
            print(f'{datetime.datetime.now()} - sleeping for {sleep_time} seconds')
            print(f'symbols remaining: {len(symbols)}')
            time.sleep(sleep_time)
            current_minute_weight = 0
            start_time = datetime.datetime.now()

        endTimes=[data[symbol][0][0]- interval_len for symbol in symbols]
        # print(f'endTimes:{endTimes}')

        if len(endTimes_prev)==0: 
            endTimes_prev = endTimes
        elif len(endTimes_prev)>0:
            pops = []
            for s in range(len(symbols)):
                # print(f'{s}, endtimeprev:{endTimes_prev[s]} endtime: {endTimes[s]}')
                if endTimes[s]==endTimes_prev[s] or len(new_data[symbols[s]])<limit:
                    # if new endtime == previous endtime that means there is no more data, stop requesting it. 
                    pops.append(symbols[s])
            if len(pops)>0: 
                if logger is not None: 
                    logger.info(
                        dict(
                            origin='candles_backfill_fn',
                            payload=dict(
                                reason='reached inception time',
                                interval=interval,
                                usd_futs=usd_futs, coin_futs=coin_futs, 
                                num_dropped_symbols=len(pops),
                                dropped_symbols=pops,
                                )))  

                for s in pops.copy(): 
                    print(f'REACHED BEGINNING OF: {s}: {long_to_datetime_str(endTimes_prev[symbols.index(s)])}')
                    if dbs is not None:
                        del dbs[s]                    
                    endTimes_prev.pop(symbols.index(s))
                    endTimes.pop(symbols.index(s))
                    # symbols.pop(symbols.index(s))
                    # symbols.remove(s)
                    del symbols[symbols.index(s)]

        if len(symbols)==0:
            break

        new_data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, endTimes=endTimes, usd_futs=usd_futs, coin_futs=coin_futs,  mark=mark, index=index, logger=logger)))
        current_minute_weight += len(symbols)

        total_requests_per_symbol += 1
        
        # print(symbols)
        j+=1
        print('j={}'.format(j))
        print('current_minute_weight: {}'.format(current_minute_weight))
        print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

        for symbol in symbols:
            if len(new_data[symbol])>0: last_insert_print[symbol]=new_data[symbol][0][0]
            if dbs is not None: 
                dbs[symbol].insert_multiple(new_data[symbol])
            # data[symbol]=new_data[symbol]
            data[symbol]=new_data[symbol]+data[symbol]

            if memory_efficient is False:
                data[symbol]=new_data[symbol]+data[symbol]
            else: 
                data[symbol]=new_data[symbol]+data[symbol][:1]

        endTimes_prev=endTimes

    data = {k:v[-backfill:] for k,v in data.items()} if backfill is not None else data
    for k,v in data.items():
        print(f'candles_backfill_fn:{k} usd_futs:{usd_futs} coin_futs:{coin_futs}, mark={mark}, index={index}, interval:{interval} inserted:{len(v)} last:{long_to_datetime_str(last_insert_print[k])}')
        
    return data




# # import json
# # eth_btc = candles_backfill_fn(['BTCUSDT', 'ETHUSDT'], interval='5m', backfill=None)

# # with open('ETH_BTC5.json', 'w') as f:
# #     json.dump(eth_btc, f, indent=4)

# import os
# os.getcwd()


# from CandleDBClass import candle_db




# DB_DIRECTORY='/home/cm/Documents/PY_DEV/BINANCE_CANDLEGRAB/db/'

# # DB_NAME ='chill3.db' 


# btc=candle_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME='f.db', SYMBOL='BTCUSDT', INTERVAL='1d', TYPE='FUTS', EXCHANGE='binance')
# avax=candle_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME='ff.db', SYMBOL='AVAXUSDT', INTERVAL='1d', TYPE='FUTS', EXCHANGE='binance')


# btc.get_last()
# avax.get_last()

# btc.delete_last() #3
# avax.delete_last() # 7

# candles_backfill_fn(['sdfsdF', 'AVAXUSDT'],dbs={'sdfsdF':btc, 'AVAXUSDT':avax}, interval='4h', backfill=100, futs=False, logger=logger)


# symbols=['sdfsdF', 'AVAXUSDT']
# data = asyncio.run(get_candles(**dict(symbols=symbols, interval='5m', limit=5, endTimes=None, futs=False)))

# # s=candles_backfill_fn(['BTCUSDT', 'AVAXUSDT'],dbs=None, interval='1d', backfill=100)