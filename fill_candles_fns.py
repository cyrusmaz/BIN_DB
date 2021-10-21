from db_helpers import *
from async_fns import get_candles

import asyncio
import time
from math import ceil
import datetime

def candles_backfill_fn(
    symbols,  interval, backfill, 
    usdf, coinf, mark,limit, 
    rate_limit, index, dbs=None, 
    logger=None, memory_efficient=True):
    """ for backfilling candles start at present and go backward 
        until either backfill is reached or no new data comes in on each subsequent request"""
    if symbols is None or len(symbols)==0:
        return None

    interval_len = interval_length_ms(interval)
    j=0
    
    start_time = datetime.datetime.now()
    current_minute_weight = 0
    total_requests_per_symbol =0 

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

    data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, endTimes=endTimes_prev, usdf=usdf, coinf=coinf, mark=mark, index=index, logger=logger)))
    current_minute_weight += len(symbols)
    

    for s in range(len(symbols)):
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
                        usdf=usdf, coinf=coinf, 
                        mark=mark, index=index,
                        num_dropped_symbols=len(pops),
                        dropped_symbols=pops,
                        )))  

        for s in pops: 
            endTimes_prev.pop(symbols.index(s))
            if dbs is not None:
                del dbs[s]
            del symbols[symbols.index(s)]

    total_requests_per_symbol += 1

    j+=1
    print('j={}'.format(j))
    print('current_minute_weight: {}'.format(current_minute_weight))
    print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    
    endTimes_prev = []

    if backfill is None: while_condition=lambda: True 
    else: while_condition=lambda: ceil(backfill/limit)>total_requests_per_symbol

    while while_condition() is True:
        print(j)
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

        if len(endTimes_prev)==0: 
            endTimes_prev = endTimes
        elif len(endTimes_prev)>0:
            pops = []
            for s in range(len(symbols)):
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
                                usdf=usdf, coinf=coinf,
                                mark=mark, index=index,
                                num_dropped_symbols=len(pops),
                                dropped_symbols=pops,
                                )))  

                for s in pops.copy(): 
                    print(f'REACHED BEGINNING OF: {s}: {long_to_datetime_str(endTimes_prev[symbols.index(s)])}')
                    if dbs is not None:
                        del dbs[s]                    
                    endTimes_prev.pop(symbols.index(s))
                    endTimes.pop(symbols.index(s))
                    del symbols[symbols.index(s)]

        if len(symbols)==0:
            break

        new_data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, endTimes=endTimes, usdf=usdf, coinf=coinf,  mark=mark, index=index, logger=logger)))
        current_minute_weight += len(symbols)

        total_requests_per_symbol += 1

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
        print(f'candles_backfill_fn:{k} usdf:{usdf} coinf:{coinf}, mark={mark}, index={index}, interval:{interval} inserted:{len(v)} last:{long_to_datetime_str(last_insert_print[k])}')
        
    return data


def candles_forwardfill_fn(
    symbols, interval, 
    usdf, coinf, mark, index, 
    limit, rate_limit, 
    startTimes_dict=None, dbs=None, logger=None):
    """ for backfilling candles start at present and go backward 
        until either backfill is reached or no new data comes in on each subsequent request"""
    if symbols is None or len(symbols)==0:
        return None

    j=0
    
    start_time = datetime.datetime.now()
    current_minute_weight = 0
    total_requests_per_symbol =0 

    startTimes_prev =[]
    last_insert_print = {k:None for k in symbols}

    if startTimes_dict is None: 
        for symbol in symbols:
            if dbs is not None: 
                last_candle = dbs[symbol].get_last()
                startTime = last_candle[0]
                startTimes_prev.append(startTime)
    else: 
        for symbol in symbols:
            startTime = startTimes_dict[symbol]
            startTimes_prev.append(startTime)     
   
    data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, startTimes=startTimes_prev, usdf=usdf, coinf=coinf, mark=mark, index=index, logger=logger)))

    for symbol in symbols:
        if len(data[symbol])>0: last_insert_print[symbol]=data[symbol][-1][0]
        if dbs is not None:   
            dbs[symbol].delete_last()
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
                        usdf=usdf, coinf=coinf,
                        mark=mark, index=index,
                        num_dropped_symbols=len(pops),
                        dropped_symbols=pops,
                        )))  

        for s in pops: 
            startTimes_prev.pop(symbols.index(s))
            if dbs is not None:
                del dbs[s]
            del symbols[symbols.index(s)]

    total_requests_per_symbol += 1

    current_minute_weight += len(symbols)
    j+=1
    # print(symbols)
    print('j={}'.format(j))
    print('current_minute_weight: {}'.format(current_minute_weight))
    print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

    while len(symbols)>0:
        print(j)
        start_time = datetime.datetime.now()
        if current_minute_weight + len(symbols) >= rate_limit:
            sleep_time = 61 - (datetime.datetime.now()-start_time).total_seconds()
            sleep_time = max(sleep_time, 30)
            print(f'{datetime.datetime.now()} - sleeping for {sleep_time} seconds')
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
                                usdf=usdf, coinf=coinf,
                                mark=mark, index=index,
                                num_dropped_symbols=len(pops),
                                dropped_symbols=pops,
                                )))  

                for s in pops: 
                    print(f'REACHED BEGINNING OF: {s}: {long_to_datetime_str(startTimes_prev[symbols.index(s)])}')
                    if dbs is not None:
                        del dbs[s]                    
                    startTimes_prev.pop(symbols.index(s))
                    startTimes.pop(symbols.index(s))
                    symbols.pop(symbols.index(s))

        if len(symbols)==0:
            break

        data = asyncio.run(get_candles(**dict(symbols=symbols, interval=interval, limit=limit, startTimes=startTimes, usdf=usdf, coinf=coinf, mark=mark, index=index, logger=logger)))

        total_requests_per_symbol += 1
        current_minute_weight += len(symbols)

        j+=1
        print('j={}'.format(j))
        print('current_minute_weight: {}'.format(current_minute_weight))
        print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

        for symbol in symbols:
            if len(data[symbol])>0: last_insert_print[symbol]=data[symbol][-1][0]
            if dbs is not None: 
                dbs[symbol].delete_last()
                dbs[symbol].insert_multiple(data[symbol])
        startTimes_prev=startTimes        

    for k,v in data.items():
        print(f'candles_forwardfill_fn:{k} usdf:{usdf}, coinf={coinf}, mark={mark}, index={index}, interval:{interval} inserted:{len(v)} last entry:{long_to_datetime_str(last_insert_print[k])}')
        
    return data

