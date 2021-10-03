

import aiohttp
import asyncio
import json
from db_helpers import long_to_datetime_str

# CANDLES
async def get_candles_worker(symbol, interval, limit, startTime=None, endTime=None, logger=None, futs=False):
    print(f'async get_candles_worker({symbol}, {interval}, limit={limit}, startTime={long_to_datetime_str(startTime)}, endTime={long_to_datetime_str(endTime)}, futs={futs})')
    if futs is True: 
        klines_endpoint = 'https://fapi.binance.com/fapi/v1/klines?symbol={}&interval={}&limit={}'
    else: 
        klines_endpoint = 'https://api.binance.com/api/v1/klines?symbol={}&interval={}&limit={}'
    url = klines_endpoint.format(symbol, interval, limit)
    if startTime is not None: 
        url = url + '&startTime={}'.format(startTime)
    if endTime is not None: 
        url = url + '&endTime={}'.format(endTime)        
    output = {}
    # async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
    #     async with session.get(url) as resp:
    #         resp = await resp.json(loads=json.loads)
    #         output[symbol]=resp
    # return output

    try: 
        async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
            async with session.get(url) as resp:
                resp = await resp.json(loads=json.loads)
                output[symbol]=resp
        return output
    except Exception as e:
        if logger is not None: 
            logger.critical(dict(origin='get_candles_worker', payload=dict(error=e, symbol=symbol,futs=futs, interval=interval, url=url)))
        raise e


async def get_candles(symbols, interval='1m',limit=1000, startTimes=None, endTimes=None, futs=False, logger=None, **kwargs):
    if endTimes is None:
        endTimes=[None]*len(symbols)
    if startTimes is None:
        startTimes=[None]*len(symbols)        

    try:
        result = await asyncio.gather(*[get_candles_worker(symbol=symbol, interval=interval, limit=limit, startTime=startTime, endTime=endTime, futs=futs) for symbol, startTime,endTime in zip(symbols,startTimes,endTimes)])
    except Exception as e:
        logger.critical(dict(origin='get_candles', payload=e))
        raise e

    output = {}
    for d in result:
        output.update(d)

    pops=[]
    # dict_returns = []
    for s in symbols:

        if type(output[s]) is dict and 'code' in output[s].keys():
            pops.append(s)
    if len(pops)>0:
        if logger is not None: 
            logger.critical(dict(
                origin='get_candles', 
                payload= dict(
                interval=interval, 
                futs=futs, 
                num_symbols_dropped=len(pops),
                unique_msgs=list(set([json.dumps(output[s]) for s in pops])), 
                # all_msgs = [output[s] for s in pops], 
                # symbols_dropped=pops,
                # startTimes=startTimes, 
                # endTimes=endTimes 
                )))     

        for s in pops:
            symbols.pop(symbols.index(s))
            del output[s]

    return output

# FUNDING AND OI
async def get_futs_stat_worker(symbol, stat=None, period=None, limit=500, startTime=None, endTime=None, logger=None, **kwargs):
    print(f'async get_futs_stat_worker({symbol}, stat={stat},period={period}, limit={limit}, startTime={long_to_datetime_str(startTime)}, endTime={long_to_datetime_str(endTime)})')
    if stat == 'funding':
        endpoint = "https://fapi.binance.com/fapi/v1/fundingRate?symbol={}&limit={}"
        url = endpoint.format(symbol, limit)
    elif stat == 'oi':
        endpoint = "https://fapi.binance.com/futures/data/openInterestHist?symbol={}&period={}&limit={}"
        url = endpoint.format(symbol, period,limit)
    # elif stat == 'top_acc':
    #     endpoint = "https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol={}&period={}&limit={}"  
    #     url = endpoint.format(symbol, period,limit)      
    # elif stat == 'top_pos':
    #     endpoint = "https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={}&period={}&limit={}"  
    #     url = endpoint.format(symbol, period,limit)      
    # elif stat == 'global_acc':
    #     endpoint = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={}&period={}&limit={}"    
    #     url = endpoint.format(symbol, period,limit)    

    if startTime is not None: 
        url = url + '&startTime={}'.format(startTime)
    if endTime is not None: 
        url = url + '&endTime={}'.format(endTime)        
    output = {}
    try: 
        # print(url)
        async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
            async with session.get(url) as resp:
                resp = await resp.json(loads=json.loads)
                output[symbol]=resp
        return output
    except Exception as e:
        if logger is not None: 
            logger.critical(dict(origin='get_futs_stat_worker', payload=dict(error=e, symbol=symbol, url=url)))
        raise e


async def get_futs_stats(symbols, stat, limit, period='5m',startTimes=None, endTimes=None, logger=None, **kwargs):
    if endTimes is None:
        endTimes=[None]*len(symbols)
    if startTimes is None:
        startTimes=[None]*len(symbols)        
    try:
        result = await asyncio.gather(*[get_futs_stat_worker(symbol=symbol, stat=stat, period=period,limit=limit, startTime=startTime, endTime=endTime) for symbol, startTime,endTime in zip(symbols,startTimes,endTimes)])
    except Exception as e:
        logger.critical(dict(origin='get_futs_stats', payload=e))
        raise e
    # return result
    output = {}
    for d in result:
        output.update(d)

    pops_empty_output=[]
    pops_dict_output=[]
    for s in symbols:
        if type(output[s]) is dict and 'code' in output[s].keys():
            pops_dict_output.append(s)
        if len(output[s])==0:
            pops_empty_output.append(s)
    if len(pops_dict_output)>0:
            if logger is not None: 
                logger.critical(dict(
                    origin='get_futs_stats', 
                    payload= dict(
                        stat=stat, 
                        num_symbols_dropped=len(pops_dict_output),
                        unique_msgs=list(set([json.dumps(output[s]) for s in pops_dict_output])), 
                        # all_msgs = [output[s] for s in pops], 
                        # symbols_dropped=pops_dict_output,
                        # startTimes=startTimes, 
                        # endTimes=endTimes 
                        )))     

            for s in pops_dict_output:
                symbols.pop(symbols.index(s))
                del output[s]

    if len(pops_empty_output)>0:
        if logger is not None: 
            logger.info(
                dict(
                    origin='get_futs_stats', 
                    payload=dict(
                        stat=stat, 
                        num_symbols_dropped=len(pops_empty_output),
                        symbols_dropped=pops_empty_output)
                        ))
        for s in pops_empty_output:
            symbols.pop(symbols.index(s))
            del output[s]


    return output    

## GET EXCHANGE INFOS
async def get_info(type_):
    if type_=='usd_futs': 
        url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    elif type_=='coin_futs': 
        url = 'https://dapi.binance.com/dapi/v1/exchangeInfo'
    elif type_=='spot': 
        url = 'https://api.binance.com/api/v3/exchangeInfo'        
    
    output = {}
    async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
        async with session.get(url) as resp:
            resp = await resp.json(loads=json.loads)
            output[type_]=resp
    return output

async def get_infos(types, logger):
    try:
        result = await asyncio.gather(*[get_info(type_=t) for t in types])
    except Exception as e:
        logger.critical(dict(origin='get_futs_stats', payload=e))
        raise e

    output = {}
    for d in result:
        output.update(d)
    return output
