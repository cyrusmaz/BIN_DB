from db_helpers import *
from API_RATES import *
from async_fns import get_futs_stats

import asyncio
from copy import deepcopy
import time


def oi_forwardfill_fn(symbols, dbs=None, interval='1m', backfill=8, futs=False, logger=None):
    """ for forward filling OI, start at present and go backward 
        until either backfill is reached or no new data comes in on each subsequent request"""

    limit = FUTS_CANDLE_LIMIT
    rate_limit=FUTS_CANDLE_RATE_LIMIT

    symbols = deepcopy(symbols)
    if symbols is None or len(symbols)==0:
        return None

 
    j=0
    
    start_time = datetime.datetime.now()
    current_minute_weight = 0
    total_requests_per_symbol =0 

    startTimes_prev =[]
    last_insert_print = {k:None for k in symbols}


    for symbol in symbols:
        if dbs is not None: 
            last_entry = dbs[symbol].get_last()

            dbs[symbol].delete_last()
            startTime = last_entry['timestamp']
            startTimes_prev.append(startTime)
   
    data = asyncio.run(get_futs_stats(**dict(symbols=symbols, stat='oi', limit=limit,period=interval, startTimes=startTimes_prev,  endTimes=None, logger=logger)))
    total_requests_per_symbol += 1

    current_minute_weight += len(symbols)
    j+=1    
    # new_data = deepcopy(data)
    
    for symbol in symbols:
        if len(data[symbol])>0: last_insert_print[symbol] = data[symbol][-1]['timestamp']

        if dbs is not None:   
            dbs[symbol].insert_multiple(data[symbol])

    pops = list(filter(lambda x: len(data[symbols[x]])<limit, range(len(symbols))))
    pops = [symbols[p] for p in pops]

    if len(pops)>0: 
        if logger is not None: 
            logger.info(
                dict(
                    origin='oi_forwardfill_fn',
                    payload=dict(
                        reason='filling into present - zero results',
                        interval=interval,
                        # futs=futs,
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

        # startTimes=[data[symbol][-1][0] for symbol in symbols]
        startTimes=[data[symbol][-1]['timestamp'] for symbol in symbols]

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
                            origin='oi_forwardfill_fn',
                            payload=dict(
                                reason='reached present time',
                                interval=interval,
                                # futs=futs,
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

        data = asyncio.run(get_futs_stats(**dict(symbols=symbols, stat='oi', limit=limit,period=interval, startTimes=startTimes,  endTimes=None, logger=logger)))

        total_requests_per_symbol += 1
        current_minute_weight += len(symbols)

        j+=1
        print('j={}'.format(j))
        print('current_minute_weight: {}'.format(current_minute_weight))
        print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

        for symbol in symbols:
            # last_insert_print[symbol] = data[symbols[s]][-1]['timestamp']
            if len(data[symbol])>0: last_insert_print[symbol] = data[symbol][-1]['timestamp']
            if dbs is not None: 
                dbs[symbol].delete_last()
                dbs[symbol].insert_multiple(data[symbol])

        startTimes_prev=startTimes        

    # data = {k:v[-backfill:] for k,v in data.items()} if backfill is not None else data
    for k,v in data.items():
        # print(f'(oi_forwardfill_fn) {k} inserted {len(v)} last entry: {long_to_datetime_str(last_insert_print[symbol])}')
        print(f'oi_forwardfill_fn:{k} interval:{interval} inserted:{len(v)} last entry:{long_to_datetime_str(last_insert_print[k])}')
                
    return data




# def oi_forwardfill_fn_OG(symbols, dbs=None, interval='5m', logger=None):
#     """ for forward filling OI, start at present and go backward 
#         until either backfill is reached or no new data comes in on each subsequent request"""

#     limit = FUTS_CANDLE_LIMIT
#     rate_limit=FUTS_CANDLE_RATE_LIMIT

#     symbols = deepcopy(symbols)
#     if symbols is None or len(symbols)==0:
#         return None

#     j=0
    
#     start_time = datetime.datetime.now()
#     current_minute_weight = 0
#     total_requests_per_symbol =0 

#     startTimes =[]

#     for symbol in symbols:
#         if dbs is not None: 
#             last_entry = dbs[symbol].get_last()

#             dbs[symbol].delete_last()
#             startTime = last_entry['timestamp']
#             startTimes.append(startTime)
   
#     data = asyncio.run(get_futs_stats(**dict(symbols=symbols, stat='oi', limit=limit,period=interval, startTimes=startTimes,  endTimes=None, logger=logger)))
#     # new_data = deepcopy(data)

#     for symbol in symbols:
#         if dbs is not None:   
#             dbs[symbol].insert_multiple(data[symbol])

#     total_requests_per_symbol += 1

#     current_minute_weight += len(symbols)
#     j+=1
#     print(symbols)
#     print('j={}'.format(j))
#     print('current_minute_weight: {}'.format(current_minute_weight))
#     print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    
#     # endTimes_prev = None
#     startTimes_prev = None

#     while True:
#         start_time = datetime.datetime.now()
#         if current_minute_weight + len(symbols) >= rate_limit:
#             sleep_time = 61 - (datetime.datetime.now()-start_time).total_seconds()
#             sleep_time = max(sleep_time, 30)
#             print('oi_forwardfill_fn sleeping for {} seconds'.format(sleep_time))
#             print(f'symbols remaining: {len(symbols)}')
#             time.sleep(sleep_time)
#             current_minute_weight = 0
#             start_time = datetime.datetime.now()

        
#         # input(data['BTCUSDT'][-1][0])

#         startTimes=[data[symbol][-1]['timestamp'] for symbol in symbols]

        
#         print('START TIMES')
#         print(startTimes)

#         if startTimes_prev is None: 
#             startTimes_prev = startTimes

#         elif len(startTimes_prev)>0:
#             pops = []
#             for s in range(len(symbols)):
#                 if len(data[symbols[s]])==1:
#                     pops.append(symbols[s])

#             if len(pops)>0: 
#                 for s in pops: 
#                     print(f'REACHED BEGINNING OF: {s}')
#                     startTimes_prev.pop(symbols.index(s))
#                     startTimes.pop(symbols.index(s))
#                     symbols.pop(symbols.index(s))

#         if len(symbols)==0:
#             # print("HEREHEHEREHEHERHE")
#             break

#         for e in startTimes:
#             print(long_to_datetime_str(e))
#             print(e)            
    
#         data = asyncio.run(get_futs_stats(**dict(symbols=symbols, stat='oi', limit=limit,period=interval, startTimes=startTimes,  endTimes=None, logger=logger)))

#         total_requests_per_symbol += 1
#         current_minute_weight += len(symbols)

#         print(symbols)
#         j+=1
#         print('j={}'.format(j))
#         print('current_minute_weight: {}'.format(current_minute_weight))
#         print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

#         for symbol in symbols:
#             if dbs is not None: 
#                 dbs[symbol].delete_last()
#                 dbs[symbol].insert_multiple(data[symbol])
#                 # data[symbol]=data[symbol]
#                 # data[symbol]=data[symbol]+data[symbol]
#             # else: 
#             #     data[symbol]=data[symbol]+data[symbol]

#         startTimes_prev=startTimes

#     # data = {k:v[-backfill:] for k,v in data.items()} if backfill is not None else data
#     for k,v in data.items():
#         print(f'{k}: {len(v)} {interval} candles')
#     return data




# # oi_forwardfill_fn(['BTCUSDT'], dbs=dict(BTCUSDT=avax))

# # oi_forwardfill_fn()