from db_helpers import *
# from API_RATES import *
from async_fns import get_futs_stats

import asyncio
import time
import datetime
from math import ceil

def oi_backfill_fn(symbols, interval, backfill, usd_futs, coin_futs,limit,rate_limit, dbs=None,logger=None, memory_efficient=True,
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
        usd_futs=usd_futs, coin_futs=coin_futs, 
        endTimes=endTimes_prev, logger=logger,
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
                period=interval, limit=limit, 
                endTimes=endTimes, logger=logger,
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

    data = {k:v[-backfill:] for k,v in data.items()} if backfill is not None else data
    for k,v in data.items():
        # print(f'(oi_backfill_fn) {k} inserted {len(v)} last entry: {long_to_datetime_str(last_insert_print[symbol])}')

        print(f'oi_backfill_fn:{k} interval:{interval} usd_futs={usd_futs} coin_futs={coin_futs} inserted:{len(v)} last entry:{long_to_datetime_str(last_insert_print[k])}')   
        
    return data









# def oi_backfill_fn_OG(symbols, dbs=None, interval='5m', backfill=8, logger=None):
#     """ for backfilling OI, start at present and go backward 
#         until either backfill is reached or no new data comes in on each subsequent request"""


#     limit = FUTS_OI_LIMIT  
#     rate_limit = FUTS_OI_RATE_LIMIT

#     symbols = deepcopy(symbols)
#     if symbols is None or len(symbols)==0:
#         return None

#     interval_len = interval_length_ms(interval)
#     j=0
    
#     start_time = datetime.datetime.now()
#     current_minute_weight = 0
#     total_requests_per_symbol =0 

#     data = asyncio.run(get_futs_stats(**dict(symbols=symbols, stat='oi', period=interval, limit=limit, endTimes=None, logger=logger)))
#     # new_data = deepcopy(data)



#     pops=[]
#     for symbol in symbols:
#         if dbs is not None: 
#             dbs[symbol].insert_multiple(data[symbol])  
#             # try: 
#             #     dbs[symbol].insert_multiple(data[symbol])
#             # except: 
#             #     print(f"CANNOT INSERT OI FOR SYMBOL {symbol}")
#             #     pops.append(symbol)

#     # if len(pops)>0: 
#     #     if logger is not None: 
#     #         logger.warning(dict(origin='oi_backfill_fn', payload=f'cannot insert OI for: {pops}'))
#     #     for s in pops: 
#     #         print(f'REMOVING: {s}')
#     #         symbols.pop(symbols.index(s))

#     total_requests_per_symbol += 1
#     current_minute_weight += len(symbols)
#     j+=1
#     print(symbols)
#     print('j={}'.format(j))
#     print('current_minute_weight: {}'.format(current_minute_weight))
#     print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    
#     endTimes_prev = None

#     if backfill is None: while_condition=lambda: True 
#     else: while_condition=lambda: ceil(backfill/limit)>total_requests_per_symbol

#     while while_condition() is True:
#         start_time = datetime.datetime.now()
#         if current_minute_weight + len(symbols) >= rate_limit:
#             sleep_time = 61 - (datetime.datetime.now()-start_time).total_seconds()
#             sleep_time = max(sleep_time, 30)
#             print('oi_backfill_fn sleeping for {} seconds'.format(sleep_time))
#             print(f'symbols remaining: {len(symbols)}')
#             time.sleep(sleep_time)
#             current_minute_weight = 0
#             start_time = datetime.datetime.now()


#         # for s in range(len(symbols)):
#         #     # print(f'{s}, endtimeprev:{endTimes_prev[s]} endtime: {endTimes[s]}')
#         #     if len(new_data[s]):
#         #         # if new endtime == previous endtime that means there is no more data, stop requesting it. 
#         #         pops.append(symbols[s])
#         # if len(pops)>0: 
#         #     for s in pops: 
#         #         print(f'REACHED END OF: {s}')
#         #         endTimes_prev.pop(symbols.index(s))
#         #         endTimes.pop(symbols.index(s))
#         #         symbols.pop(symbols.index(s))





#         endTimes=[data[symbol][0]['timestamp']- interval_len for symbol in symbols]

#         if endTimes_prev is None: 
#             endTimes_prev = endTimes

#         elif len(endTimes_prev)>0:
#             pops = []
#             for s in range(len(symbols)):
#                 # print(f'{s}, endtimeprev:{endTimes_prev[s]} endtime: {endTimes[s]}')
#                 if endTimes[s]==endTimes_prev[s]:
#                     # if new endtime == previous endtime that means there is no more data, stop requesting it. 
#                     pops.append(symbols[s])
#             if len(pops)>0: 
#                 for s in pops: 
#                     print(f'REACHED END OF: {s}')
#                     endTimes_prev.pop(symbols.index(s))
#                     endTimes.pop(symbols.index(s))
#                     symbols.pop(symbols.index(s))

#         if len(symbols)==0:
#             # print("HEREHEHEREHEHERHE")
#             break

#         for e in endTimes:
#             print(long_to_datetime_str(e))
#             print(e)            
    
#         new_data = asyncio.run(get_futs_stats(**dict(symbols=symbols, stat='oi', period=interval, limit=limit, endTimes=endTimes, logger=logger)))

#         total_requests_per_symbol += 1
#         current_minute_weight += len(symbols)

#         print(symbols)
#         j+=1
#         print('j={}'.format(j))
#         print('current_minute_weight: {}'.format(current_minute_weight))
#         print('total_requests_per_symbol: {}'.format(total_requests_per_symbol))    

#         for symbol in symbols:
#             if not (type(new_data[symbol]) is dict and 'msg' in new_data[symbol].keys() and 'code' in new_data[symbol].keys()):
#                 if dbs is not None: 
#                     dbs[symbol].insert_multiple(new_data[symbol])
#                 data[symbol]=new_data[symbol]+data[symbol]

#         # keep track of the endtimes of previous grab
#         endTimes_prev=endTimes

#     data = {k:v[-backfill:] for k,v in data.items()} if backfill is not None else data
#     for k,v in data.items():
#         print(f'{k}: {len(v)} {interval} bars')
#     return data




# # from OIDBClass import oi_db
# # DB_DIRECTORY='/home/cm/Documents/PY_DEV/BINANCE_CANDLEGRAB/db/'
# # avax=oi_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME='avax_oi.db', SYMBOL='AVAX', TYPE='USDT_FUTS', EXCHANGE='test')
# # aave=oi_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME='aave_oi.db', SYMBOL='AAVE', TYPE='USDT_FUTS', EXCHANGE='test')
# # algo=oi_db(DB_DIRECTORY=DB_DIRECTORY, DB_NAME='algo_oi.db', SYMBOL='ALGO', TYPE='USDT_FUTS', EXCHANGE='test')

# # symbols=['fyck', 'AAVEUSDT', 'ALGOUSDT']
# # oi2=oi_backfill_fn(symbols=symbols, dbs={'fyck':avax, 'AAVEUSDT':aave, 'ALGOUSDT':algo},  interval='5m', backfill=None,logger=logger)



# # logger.info(dict(ahe='asd'))


# # data = asyncio.run(get_futs_stats(**dict(symbols=symbols, stat='oi', interval='5m', limit=5, endTimes=None)))
# # data['fyck']
# # data['AAVEUSDT']


# # fdat = asyncio.run(get_futs_stats(**dict(symbols=symbols, stat='funding', interval='5m', limit=5, endTimes=None)))
# # fdat