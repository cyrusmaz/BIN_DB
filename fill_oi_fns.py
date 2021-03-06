from db_helpers import *
# from API_RATES import *
from async_fns import get_futs_stats

import asyncio
import time
import datetime
from math import ceil

def oi_backfill_fn(symbols, interval, backfill, usdf, coinf,limit,rate_limit, dbs=None,logger=None, memory_efficient=True,
            coinf_details=None):
    """ for backfilling OI, start at present and go backward 
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
                endTime = last_candle['timestamp']
                endTimes_prev.append(endTime- interval_len)  
            else:
                endTimes_prev.append(None)                  
        else:
            endTimes_prev.append(None)

    if len(endTimes_prev)==0:
        endTimes_prev=None

    data = asyncio.run(
        get_futs_stats(**dict(symbols=symbols, stat='oi', 
        period=interval, limit=limit,
        usdf=usdf, coinf=coinf, 
        endTimes=endTimes_prev, logger=logger,
        coinf_details=coinf_details)))
    current_minute_weight += len(symbols)
    total_requests_per_symbol += 1
    j+=1

    for s in range(len(symbols)):
        if len(data[symbols[s]])>0: last_insert_print[symbols[s]] = data[symbols[s]][0]['timestamp']
        if dbs is not None: 
            if len(data[symbols[s]])>0:
                if len(endTimes_prev)>0 and data[symbols[s]][0]['timestamp']!=endTimes_prev[s]:
                    dbs[symbols[s]].insert_multiple(data[symbols[s]])
            
    pops = list(filter(lambda x: len(data[symbols[x]])==0, range(len(symbols))))
    pops = [symbols[p] for p in pops]

    if len(pops)>0: 
        if logger is not None: 
            logger.info(
                dict(
                    origin='oi_backfill_fn',
                    payload=dict(
                        reason='backfilling from inception - zero results',
                        interval=interval,
                        # futs=futs,
                        usdf=usdf, coinf=coinf,
                        num_dropped_symbols=len(pops),
                        dropped_symbols=pops,
                        )))  

        for s in pops: 
            endTimes_prev.pop(symbols.index(s))
            if dbs is not None:
                del dbs[s]
            del symbols[symbols.index(s)]

    print('j={}'.format(j))
    print('current_minute_weight: {}'.format(current_minute_weight))
    print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    
    endTimes_prev = []

    if backfill is None: while_condition=lambda: True 
    else: while_condition=lambda: ceil(backfill/limit)>total_requests_per_symbol

    while while_condition() is True:
        start_time = datetime.datetime.now()
        if current_minute_weight + len(symbols) >= rate_limit:
            sleep_time = 61 - (datetime.datetime.now()-start_time).total_seconds()
            sleep_time = max(sleep_time, 30)
            print(f'{datetime.datetime.now()} - sleeping for {sleep_time} seconds')
            print(f'symbols remaining: {len(symbols)}')
            time.sleep(sleep_time)
            current_minute_weight = 0
            start_time = datetime.datetime.now()

        endTimes=[data[symbol][0]['timestamp']- interval_len for symbol in symbols]

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
                            origin='oi_backfill_fn',
                            payload=dict(
                                reason='reached inception time',
                                interval=interval,
                                usdf=usdf, coinf=coinf,
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

        new_data = asyncio.run(
            get_futs_stats(**dict(
                symbols=symbols, stat='oi',
                usdf=usdf, coinf=coinf, 
                period=interval, limit=limit, 
                endTimes=endTimes, logger=logger,
                coinf_details=coinf_details)))

        total_requests_per_symbol += 1
        current_minute_weight += len(symbols)

        j+=1
        print('j={}'.format(j))
        print('current_minute_weight: {}'.format(current_minute_weight))
        print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

        for symbol in symbols:
            if not (type(new_data[symbol]) is dict and 'msg' in new_data[symbol].keys() and 'code' in new_data[symbol].keys()):
                # if len(data[symbols[s]])>0: last_insert_print[symbol] = data[symbols[s]][0]['timestamp']
                if len(new_data[symbol])>0: last_insert_print[symbol] = new_data[symbol][0]['timestamp']
                    
                if dbs is not None: 
                    dbs[symbol].insert_multiple(new_data[symbol])
            # data[symbol]=new_data[symbol]
            # data[symbol]=new_data[symbol]+data[symbol]

            if memory_efficient is False:
                data[symbol]=new_data[symbol]+data[symbol]
            else: 
                data[symbol]=new_data[symbol]+data[symbol][:1]

        endTimes_prev=endTimes

    data = {k:v[-backfill:] for k,v in data.items()} if backfill is not None else data
    for k,v in data.items():
        print(f'oi_backfill_fn:{k} interval:{interval} usdf={usdf} coinf={coinf} inserted:{len(v)} last entry:{long_to_datetime_str(last_insert_print[k])}')   
        
    return data


def oi_forwardfill_fn(symbols, interval, usdf, coinf, limit,rate_limit, startTimes_dict=None, dbs=None, logger=None, memory_efficient=True,
            coinf_details=None):
    """ for backfilling OI, start at present and go backward 
        until either backfill is reached or no new data comes in on each subsequent request"""

    if symbols is None or len(symbols)==0:
        return None

    interval_len = interval_length_ms(interval)
    j=0
    
    start_time = datetime.datetime.now()
    current_minute_weight = 0
    total_requests_per_symbol =0 
    last_insert_print = {k:None for k in symbols}

    startTimes_prev =[]
    if startTimes_dict is None: 
        for symbol in symbols:
            if dbs is not None: 
                last_entry = dbs[symbol].get_last()
                startTime = last_entry['timestamp']
                startTimes_prev.append(startTime)
                print(f'FIRST LAST ENTRY TIMESTAMP: {symbol}: {long_to_datetime_str(startTime)} - {startTime} ')
    else: 
        for symbol in symbols:
            startTime = startTimes_dict[symbol]
            startTimes_prev.append(startTime)     
                   
    data = asyncio.run(
        get_futs_stats(**dict(
            symbols=symbols, stat='oi', 
            period=interval, limit=limit, 
            usdf=usdf, coinf=coinf, 
            startTimes=startTimes_prev, 
            endTimes=None, logger=logger,
            coinf_details=coinf_details)))
    current_minute_weight += len(symbols)
    total_requests_per_symbol += 1
    j+=1

    for symbol in symbols:
        if len(data[symbol])>0: last_insert_print[symbol] = data[symbol][0]['timestamp']
        if dbs is not None: 
            if len(data[symbol])>0:
                dbs[symbol].delete_last()
                dbs[symbol].insert_multiple(data[symbol])

    print('j={}'.format(j))
    print('current_minute_weight: {}'.format(current_minute_weight))
    print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

    # if backfill is None: while_condition=lambda: True 
    # else: while_condition=lambda: ceil(backfill/limit)>total_requests_per_symbol

    while True:
        # print(f'len(startTimes_prev):{len(startTimes_prev)}, len(symbols):{len(symbols)}, len(dbs):{len(dbs)}')
        # print(j)
        # input('ll##&$*(#&%*(#&(%#&*( j+')
        start_time = datetime.datetime.now()
        if current_minute_weight + len(symbols) >= rate_limit:
            sleep_time = 61 - (datetime.datetime.now()-start_time).total_seconds()
            sleep_time = max(sleep_time, 30)
            print(f'{datetime.datetime.now()} - sleeping for {sleep_time} seconds')
            print(f'symbols remaining: {len(symbols)}')
            time.sleep(sleep_time)
            current_minute_weight = 0
            start_time = datetime.datetime.now()

        endTimes=[data[symbol][0]['timestamp']- interval_len for symbol in symbols]
        pops=[]
        for symbol, startTime,endTime in zip(symbols,startTimes_prev, endTimes):
            if startTime>endTime or len(data[symbol])==0:
                pops.append(symbol)

        if len(pops)>0: 
            
            # print(pops)
            # input('***************************************************************************************pops')
            if logger is not None: 
                logger.info(
                    dict(
                        origin='oi_forwardfill_fn',
                        payload=dict(
                            reason='reached inception time',
                            interval=interval,
                            usdf=usdf, coinf=coinf,
                            # futs=futs,
                            num_dropped_symbols=len(pops),
                            dropped_symbols=pops,
                            )))  

            for s in pops.copy(): 
                print(f'REACHED BEGINNING OF: {s}: {long_to_datetime_str(endTimes[symbols.index(s)])}')
                if dbs is not None:
                    del dbs[s]                    
                # endTimes.pop(symbols.index(s))
                startTimes_prev.pop(symbols.index(s))
                endTimes.pop(symbols.index(s))
                del symbols[symbols.index(s)]

        if len(symbols)==0:
            break

        new_data = asyncio.run(
            get_futs_stats(**dict(
                symbols=symbols, stat='oi',
                usdf=usdf, coinf=coinf, 
                period=interval, startTimes=startTimes_prev, 
                limit=limit, endTimes=endTimes, 
                logger=logger,
                coinf_details=coinf_details)))

        total_requests_per_symbol += 1
        current_minute_weight += len(symbols)
        
        j+=1
        print('j={}'.format(j))
        print('current_minute_weight: {}'.format(current_minute_weight))
        print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

        for symbol in symbols:
            if not (type(new_data[symbol]) is dict and 'msg' in new_data[symbol].keys() and 'code' in new_data[symbol].keys()):
                # if len(data[symbols[s]])>0: last_insert_print[symbol] = data[symbols[s]][0]['timestamp']
                if len(new_data[symbol])>0: last_insert_print[symbol] = new_data[symbol][0]['timestamp']
                    
                if dbs is not None: 
                    # dbs[symbol].delete_last()
                    dbs[symbol].insert_multiple(new_data[symbol])
            # data[symbol]=new_data[symbol]
            # data[symbol]=new_data[symbol]+data[symbol]

            if memory_efficient is False:
                data[symbol]=new_data[symbol]+data[symbol]
            else: 
                data[symbol]=new_data[symbol]+data[symbol][:1]

        # endTimes_prev=deepcopy(endTimes)

    for k,v in data.items():
        print(f'oi_forwardfill_fn:{k} interval:{interval} usdf={usdf} coinf={coinf} inserted:{len(v)} last entry:{long_to_datetime_str(last_insert_print[k])}')   
        
    return data
