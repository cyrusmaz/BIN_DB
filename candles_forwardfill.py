


import asyncio
import time
from copy import deepcopy
from math import ceil
from db_helpers import *
from API_RATES import *
from async_fns import get_candles



def candles_forwardfill_fn(symbols, dbs=None, interval='1m', backfill=8, futs=False, logger=None):
    """ for backfilling candles start at present and go backward 
        until either backfill is reached or no new data comes in on each subsequent request"""

    if futs is False:
        limit=SPOT_CANDLE_LIMIT
        rate_limit = SPOT_CANDLE_RATE_LIMIT
    if futs is True: 
        limit = FUTS_CANDLE_LIMIT
        rate_limit=FUTS_CANDLE_RATE_LIMIT


    # symbols = deepcopy(symbols)
    if symbols is None or len(symbols)==0:
        return None

    interval_len = interval_length_ms(interval)
    j=0
    
    start_time = datetime.datetime.now()
    current_minute_weight = 0
    total_requests_per_symbol =0 

    startTimes_prev =[]
    last_insert_print = {k:None for k in symbols}

    for symbol in symbols:
        if dbs is not None: 
            last_candle = dbs[symbol].get_last()

            dbs[symbol].delete_last()
            startTime = last_candle[0]
            startTimes_prev.append(startTime)
   
    data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, startTimes=startTimes_prev, futs=futs, logger=logger)))

    for symbol in symbols:
        if len(data[symbol])>0: last_insert_print[symbol]=data[symbol][-1][0]
        if dbs is not None:   
            dbs[symbol].insert_multiple(data[symbol])

    pops = list(filter(lambda x: len(data[symbols[x]])<limit, range(len(symbols))))
    pops = [symbols[p] for p in pops]

    if len(pops)>0: 
        if logger is not None: 
            logger.info(
                dict(
                    origin='candles_forwardfill_fn',
                    payload=dict(
                        reason='filling into present - zero results',
                        interval=interval,
                        futs=futs,
                        num_dropped_symbols=len(pops),
                        dropped_symbols=pops,
                        )))  

        for s in pops: 
            startTimes_prev.pop(symbols.index(s))
            if dbs is not None:
                del dbs[s]
            # symbols.pop(symbols.index(s))
            del symbols[symbols.index(s)]
            # symbols.remove(s)

    total_requests_per_symbol += 1

    current_minute_weight += len(symbols)
    j+=1
    # print(symbols)
    print('j={}'.format(j))
    print('current_minute_weight: {}'.format(current_minute_weight))
    print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

    while len(symbols)>0:
        # print(j)
        start_time = datetime.datetime.now()
        if current_minute_weight + len(symbols) >= rate_limit:
            sleep_time = 61 - (datetime.datetime.now()-start_time).total_seconds()
            sleep_time = max(sleep_time, 30)
            print('sleeping for {} seconds'.format(sleep_time))
            print(f'symbols remaining: {len(symbols)}')
            time.sleep(sleep_time)
            current_minute_weight = 0
            start_time = datetime.datetime.now()

        startTimes=[data[symbol][-1][0] for symbol in symbols]

        if len(startTimes_prev)==0: 
            startTimes_prev = startTimes
        elif len(startTimes_prev)>0:
            pops = []
            for s in range(len(symbols)):

                if len(data[symbols[s]])<limit:
                    pops.append(symbols[s])

            if len(pops)>0: 
                if logger is not None: 
                    logger.info(
                        dict(
                            origin='candles_forwardfill_fn',
                            payload=dict(
                                reason='reached present time',
                                interval=interval,
                                futs=futs,
                                num_dropped_symbols=len(pops),
                                dropped_symbols=pops,
                                )))  

                for s in pops: 
                    print(f'REACHED BEGINNING OF: {s}: {long_to_datetime_str(startTimes_prev[symbols.index(s)])}')
                    if dbs is not None:
                        del dbs[s]                    
                    # print(f'REACHED BEGINNING OF: {s}')
                    startTimes_prev.pop(symbols.index(s))
                    startTimes.pop(symbols.index(s))
                    symbols.pop(symbols.index(s))

        if len(symbols)==0:
            break

        data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, startTimes=startTimes, futs=futs, logger=logger)))

        total_requests_per_symbol += 1
        current_minute_weight += len(symbols)

        j+=1
        print('j={}'.format(j))
        print('current_minute_weight: {}'.format(current_minute_weight))
        print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

        for symbol in symbols:
            # last_insert_print[symbol]=data[symbol][-1][0]

            if len(data[symbol])>0: last_insert_print[symbol]=data[symbol][-1][0]

            if dbs is not None: 
                dbs[symbol].delete_last()
                dbs[symbol].insert_multiple(data[symbol])

        startTimes_prev=startTimes        

    # data = {k:v[-backfill:] for k,v in data.items()} if backfill is not None else data
    for k,v in data.items():


        print(f'candles_forwardfill_fn:{k} futs:{futs} interval:{interval} inserted:{len(v)} last entry:{long_to_datetime_str(last_insert_print[k])}')
        
    return data






def candles_forwardfill_fn_OG(symbols, dbs=None, interval='1m', futs=False, logger=None):
    """ for backfilling candles, start at present and go backward 
        until either backfill is reached or no new data comes in on each subsequent request"""

    if futs is False:
        limit=SPOT_CANDLE_LIMIT
        rate_limit = SPOT_CANDLE_RATE_LIMIT
    if futs is True: 
        limit = FUTS_CANDLE_LIMIT
        rate_limit=FUTS_CANDLE_RATE_LIMIT


    # symbols = deepcopy(symbols)
    if symbols is None or len(symbols)==0:
        return None

    j=0
    
    start_time = datetime.datetime.now()
    current_minute_weight = 0
    total_requests_per_symbol =0 

    startTimes =[]

    for symbol in symbols:
        if dbs is not None: 
            last_candle = dbs[symbol].get_last()

            dbs[symbol].delete_last()
            startTime = last_candle[0]
            startTimes.append(startTime)
   
    data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, startTimes=startTimes, futs=futs, logger=logger)))
    current_minute_weight += len(symbols)

    for symbol in symbols:
        if dbs is not None:   
            dbs[symbol].insert_multiple(data[symbol])

    total_requests_per_symbol += 1

    
    j+=1
    print(symbols)
    print('j={}'.format(j))
    print('current_minute_weight: {}'.format(current_minute_weight))
    print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    
    # endTimes_prev = None
    startTimes_prev = None

    while True:
        start_time = datetime.datetime.now()
        if current_minute_weight + len(symbols) >= rate_limit:
            sleep_time = 61 - (datetime.datetime.now()-start_time).total_seconds()
            sleep_time = max(sleep_time, 30)
            print('sleeping for {} seconds'.format(sleep_time))
            print(f'symbols remaining: {len(symbols)}')
            time.sleep(sleep_time)
            current_minute_weight = 0
            start_time = datetime.datetime.now()

        startTimes=[data[symbol][-1][0] for symbol in symbols]
        print('START TIMES')
        print(startTimes)

        if startTimes_prev is None: 
            startTimes_prev = startTimes

        elif len(startTimes_prev)>0:
            pops = []
            for s in range(len(symbols)):
                if len(data[symbols[s]])==1:
                    pops.append(symbols[s])

            if len(pops)>0: 
                for s in pops: 
                    if dbs is not None:
                        del dbs[s]
                        
                    print(f'REACHED BEGINNING OF: {s}')
                    startTimes_prev.pop(symbols.index(s))
                    startTimes.pop(symbols.index(s))
                    symbols.pop(symbols.index(s))

        if len(symbols)==0:
            # print("HEREHEHEREHEHERHE")
            break

        for e in startTimes:
            print(long_to_datetime_str(e))
            print(e)            
    
        data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, startTimes=startTimes, futs=futs, logger=logger)))

        total_requests_per_symbol += 1
        current_minute_weight += len(symbols)

        print(symbols)
        j+=1
        print('j={}'.format(j))
        print('current_minute_weight: {}'.format(current_minute_weight))
        print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

        for symbol in symbols:
            if dbs is not None: 
                dbs[symbol].delete_last()
                dbs[symbol].insert_multiple(data[symbol])
                # data[symbol]=data[symbol]
                # data[symbol]=data[symbol]+data[symbol]
            # else: 
            #     data[symbol]=data[symbol]+data[symbol]

        startTimes_prev=startTimes

    # data = {k:v[-backfill:] for k,v in data.items()} if backfill is not None else data
    for k,v in data.items():
        print(f'{k}: {len(v)} {interval} candles, futs:{futs} ()')
    return data





# from CandleDBClass import candle_db




# DB_DIRECTORY='/home/cm/Documents/PY_DEV/BINANCE_CANDLEGRAB/db/'

# # DB_NAME ='chill3.db' 


# btc=candle_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME='f.db', SYMBOL='BTCUSDT', INTERVAL='1d', TYPE='FUTS')
# avax=candle_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME='ff.db', SYMBOL='AVAXUSDT', INTERVAL='1d', TYPE='FUTS')

# btc.get_last()
# avax.get_last()

# candles_forwardfill_fn(['BTCUSDT', 'AVAXUSDT'],dbs={'BTCUSDT':btc, 'AVAXUSDT':avax}, interval='4h', futs=True, logger=logger)


# # s=backfill_candles_fn(['BTCUSDT', 'AVAXUSDT'],dbs=None, interval='1d', backfill=None)