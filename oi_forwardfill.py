from db_helpers import *
# from API_RATES import *
from async_fns import get_futs_stats

import asyncio
import time
import datetime

def oi_forwardfill_fn(symbols, interval, usd_futs, coin_futs, limit,rate_limit, startTimes_dict=None, dbs=None, logger=None, memory_efficient=True,
            coin_futs_details=None):
    """ for backfilling OI, start at present and go backward 
        until either backfill is reached or no new data comes in on each subsequent request"""

    # limit = FUTS_OI_LIMIT  
    # rate_limit = FUTS_OI_RATE_LIMIT


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
    # endTimes_prev =[]

    # for symbol in symbols:
    #     if dbs is not None: 
    #         last_candle = dbs[symbol].get_first()
    #         if last_candle is not None:
    #             endTime = last_candle['timestamp']
    #             endTimes_prev.append(endTime- interval_len)  
    #         else:
    #             endTimes_prev.append(None)                  
    #     else:
    #         endTimes_prev.append(None)


    startTimes_prev =[]
    if startTimes_dict is None: 
        for symbol in symbols:
            if dbs is not None: 
                last_entry = dbs[symbol].get_last()

                # dbs[symbol].delete_last()
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
            usd_futs=usd_futs, coin_futs=coin_futs, 
            startTimes=startTimes_prev, 
            endTimes=None, logger=logger,
            coin_futs_details=coin_futs_details)))
    current_minute_weight += len(symbols)
    total_requests_per_symbol += 1
    j+=1

    for s in range(len(symbols)):
        # last_insert_print[symbol] = data[symbols[s]][0]['timestamp']
        if len(data[symbols[s]])>0: last_insert_print[symbols[s]] = data[symbols[s]][0]['timestamp']
        if dbs is not None: 
            if len(data[symbols[s]])>0:
                # print(data[symbols[s]])
                # if len(startTimes_prev)>0 and data[symbols[s]][0]['timestamp']!=startTimes_prev[s]:
                dbs[symbol].delete_last()
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
                        usd_futs=usd_futs, coin_futs=coin_futs,
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
    endTimes_prev = []

    # if backfill is None: while_condition=lambda: True 
    # else: while_condition=lambda: ceil(backfill/limit)>total_requests_per_symbol

    while True:
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

        # endTimes=[data[symbol][0][0]- interval_len for symbol in symbols]
        endTimes=[data[symbol][0]['timestamp']- interval_len for symbol in symbols]
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
                            origin='oi_backfill_fn',
                            payload=dict(
                                reason='reached inception time',
                                interval=interval,
                                usd_futs=usd_futs, coin_futs=coin_futs,
                                # futs=futs,
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

        new_data = asyncio.run(
            get_futs_stats(**dict(
                symbols=symbols, stat='oi',
                usd_futs=usd_futs, coin_futs=coin_futs, 
                period=interval, startTimes=startTimes_prev, 
                limit=limit, endTimes=endTimes, 
                logger=logger,
                coin_futs_details=coin_futs_details)))

        total_requests_per_symbol += 1
        current_minute_weight += len(symbols)
        
        # print(symbols)
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

    # data = {k:v[-backfill:] for k,v in data.items()} if backfill is not None else data
    for k,v in data.items():
        # print(f'(oi_backfill_fn) {k} inserted {len(v)} last entry: {long_to_datetime_str(last_insert_print[symbol])}')

        print(f'oi_backfill_fn:{k} interval:{interval} usd_futs={usd_futs} coin_futs={coin_futs} inserted:{len(v)} last entry:{long_to_datetime_str(last_insert_print[k])}')   
        
    return data