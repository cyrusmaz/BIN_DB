from db_helpers import *
# from API_RATES import *
from async_fns import get_futs_stats

import asyncio
import time
from copy import deepcopy
import datetime








def funding_forwardfill_fn(symbols,usdf, coinf, limit, rate_limit, dbs=None, startTimes=None, logger=None, memory_efficient=True):
    """ 
        if startTimes is None then start at the beginning and go forward until new requests have length 1
        if startTimes is not None then start at startTimes and go forward until new requests have length 1
        
        dbs is dict(symbol:FundingDBClass, ... )
        
    """
    # limit = FUTS_FUNDING_LIMIT   
    # rate_limit = FUTS_FUNDING_RATE_LIMIT   

    # symbols = deepcopy(symbols)
    if symbols is None or len(symbols)==0:
        return None

    # interval_len = interval_length_ms(interval)
    j=0
    
    start_time = datetime.datetime.now()
    current_minute_weight = 0
    total_requests_per_symbol =0 

    # startTimes_prev =[]

    # for symbol in symbols:
    #     if dbs is not None: 
    #         last_candle = dbs[symbol].get_last()

    #         dbs[symbol].delete_last()
    #         startTime = last_candle[0]
    #         startTimes_prev.append(startTime)
   
    # data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, startTimes=startTimes_prev, futs=futs, logger=logger)))

    # for symbol in symbols:
    #     if dbs is not None:   
    #         dbs[symbol].insert_multiple(data[symbol])
    last_insert_print = {k:None for k in symbols}

    if startTimes is None: startTimes=[1]*len(symbols)
    if dbs is not None: startTimes = [dbs[s].get_last()['fundingTime'] for s in symbols]

    data = asyncio.run(get_futs_stats(**dict(symbols=symbols, stat='funding', limit=limit, usdf=usdf, coinf=coinf, startTimes=startTimes,  endTimes=None, logger=logger)))
    new_data = deepcopy(data)
    total_requests_per_symbol += 1
    current_minute_weight += len(symbols)

    for symbol in symbols:
        if len(new_data[symbol])>0: last_insert_print[symbol]=new_data[symbol][-1]['fundingTime']
        if dbs is not None: 
            dbs[symbol].delete_last()   
            dbs[symbol].insert_multiple(new_data[symbol])
            

    total_requests_per_symbol += 1
    current_minute_weight += len(symbols)


    pops = list(filter(lambda x: len(data[symbols[x]])<limit, range(len(symbols))))
    pops = [symbols[p] for p in pops]

    if len(pops)>0: 
        if logger is not None: 
            logger.info(
                dict(
                    origin='funding_forwardfill_fn',
                    payload=dict(
                        reason='filling into present - zero results',
                        # interval=interval,
                        # futs=futs,
                        usdf=usdf, coinf=coinf,
                        num_dropped_symbols=len(pops),
                        dropped_symbols=pops,
                        )))  

        for s in pops: 
            startTimes.pop(symbols.index(s))
            if dbs is not None:
                del dbs[s]
            # symbols.pop(symbols.index(s))
            del symbols[symbols.index(s)]
            # symbols.remove(s)


    j+=1
    # print(symbols)
    print('j={}'.format(j))
    print('current_minute_weight: {}'.format(current_minute_weight))
    print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    
    startTimes_prev = []


    while len(symbols)>0:
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

        # startTimes=[data[symbol][-1][0] for symbol in symbols]
        startTimes=[data[symbol][-1]['fundingTime'] for symbol in symbols]

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
                            origin='funding_forwardfill_fn',
                            payload=dict(
                                reason='reached present time',
                                # interval=interval,
                                # futs=futs,
                                usdf=usdf, coinf=coinf,
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

        # data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, startTimes=startTimes, futs=futs, logger=logger)))

        # total_requests_per_symbol += 1
        # current_minute_weight += len(symbols)

        new_data = asyncio.run(get_futs_stats(**dict(symbols=symbols,stat='funding',usdf=usdf, coinf=coinf,  limit=limit, startTimes=startTimes, logger=logger)))
        total_requests_per_symbol += 1
        current_minute_weight += len(symbols)        

        j+=1
        print('j={}'.format(j))
        print('current_minute_weight: {}'.format(current_minute_weight))
        print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

        # for symbol in symbols:
        #     if dbs is not None: 
        #         dbs[symbol].delete_last()
        #         dbs[symbol].insert_multiple(data[symbol])

        for symbol in symbols:
            if len(new_data[symbol])>0: last_insert_print[symbol]=new_data[symbol][-1]['fundingTime']
            if dbs is not None: 
                print('now inserting new pull')
                # if 
                dbs[symbol].insert_multiple(new_data[symbol][1:])

                data[symbol]=new_data[symbol]
            else:
                data[symbol]=data[symbol][:-1]+new_data[symbol]


        startTimes_prev=startTimes        

    for k,v in data.items():
        # print(f'(funding_forwardfill_fn) {k}, last entry: {long_to_datetime_str(last_insert_print[k])}')
        # print(f'funding_forwardfill_fn:{k} inserted:{len(v)} last entry:{long_to_datetime_str(last_insert_print[k])}') 
        print(f'funding_forwardfill_fn:{k} usdf={usdf}, coinf={coinf}, inserted:{len(v)} last:{long_to_datetime_str(last_insert_print[k])}')            
    return data






# def funding_forwardfill_fn_OG(symbols, dbs=None, startTimes=None, logger=None):
#     """ 
#         if startTimes is None then start at the beginning and go forward until new requests have length 1
#         if startTimes is not None then start at startTimes and go forward until new requests have length 1
        
#         dbs is dict(symbol:FundingDBClass, ... )
        
#     """

#     limit = FUTS_FUNDING_LIMIT   
#     rate_limit = FUTS_FUNDING_RATE_LIMIT   

#     symbols = deepcopy(symbols)
#     if symbols is None or len(symbols)==0:
#         return None
        
#     j=0

#     start_time = datetime.datetime.now()
#     current_minute_weight = 0
#     total_requests_per_symbol =0 

#     if startTimes is None: startTimes=[1]*len(symbols)
#     if dbs is not None: startTimes = [dbs[s].get_last()['fundingTime'] for s in symbols]

#     data = asyncio.run(get_futs_stats(**dict(symbols=symbols, stat='funding', limit=limit, startTimes=startTimes,  endTimes=None, logger=logger)))
#     new_data = deepcopy(data)

#     for symbol in symbols:
#         if dbs is not None: 
#             dbs[symbol].delete_last()   
#             dbs[symbol].insert_multiple(new_data[symbol])

#     total_requests_per_symbol += 1
#     current_minute_weight += len(symbols)
#     j+=1
#     print(symbols)
#     print('j={}'.format(j))
#     print('current_minute_weight: {}'.format(current_minute_weight))
#     print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    
#     startTimes_prev = None

#     while True:
#         start_time = datetime.datetime.now()
#         if current_minute_weight + len(symbols) >= rate_limit:
#             sleep_time = 61 - (datetime.datetime.now()-start_time).total_seconds()
#             sleep_time = max(sleep_time, 30)
#             print('funding_forwardfill_fn sleeping for {} seconds'.format(sleep_time))
#             print(f'symbols remaining: {len(symbols)}')
#             time.sleep(sleep_time)
#             current_minute_weight = 0
#             start_time = datetime.datetime.now()

#         startTimes=[data[symbol][-1]['fundingTime'] for symbol in symbols]
#         print('START TIMES')
#         print(startTimes)

#         if startTimes_prev is None: 
#             startTimes_prev = startTimes

#         elif len(startTimes_prev)>0:
#             pops = []
#             for s in range(len(symbols)):
#                 if len(new_data[symbols[s]])==1:
#                     pops.append(symbols[s])

#             if len(pops)>0: 
#                 for s in pops: 
#                     print(f'REACHED END OF: {s}')
#                     startTimes_prev.pop(symbols.index(s))
#                     startTimes.pop(symbols.index(s))
#                     symbols.pop(symbols.index(s))

#         if len(symbols)==0:
#             break

#         for e in startTimes:
#             print(long_to_datetime_str(e))
#             print(e)            

#         new_data = asyncio.run(get_futs_stats(**dict(symbols=symbols,stat='funding',  limit=limit, startTimes=startTimes, logger=logger)))
#         total_requests_per_symbol += 1
#         current_minute_weight += len(symbols)

#         print(symbols)
#         j+=1
#         print('j={}'.format(j))
#         print('current_minute_weight: {}'.format(current_minute_weight))
#         print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

#         for symbol in symbols:
#             if dbs is not None: 
#                 print('now inserting new pull')
#                 # if 
#                 dbs[symbol].insert_multiple(new_data[symbol][1:])

#                 data[symbol]=new_data[symbol]
#             else:
#                 data[symbol]=data[symbol][:-1]+new_data[symbol]
            

#         startTimes_prev=startTimes

#     for k,v in data.items():
#         print(f'{k}: {len(v)} funding rates')

#     return data




# from FundingDBClass import funding_db

# DB_DIRECTORY='/home/cm/Documents/PY_DEV/BINANCE_CANDLEGRAB/db/'
# avax=funding_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME='AVAXUSDT1.db', SYMBOL='AVAX', TYPE='USDT_FUTS')
# btc=funding_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME='BTCUSDT1.db', SYMBOL='BTC', TYPE='USDT_FUTS')

# avax.delete_last()
# btc.delete_last()

# forwardfill_candles_fn(symbols=['AVAXUSDT', 'BTCUSDT'], dbs={'AVAXUSDT': avax, 'BTCUSDT':btc})

