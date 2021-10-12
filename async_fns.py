

import aiohttp
import asyncio
import json
from db_helpers import long_to_datetime_str

# CANDLES
async def get_candles_worker(symbol, interval, limit, startTime=None, endTime=None, logger=None, usd_futs=False, coin_futs=False, index=False, mark=False):
    print(f'async get_candles_worker({symbol}, {interval}, limit={limit}, startTime={long_to_datetime_str(startTime)}, endTime={long_to_datetime_str(endTime)}, usd_futs={usd_futs}, coin_futs={coin_futs}, index={index}, mark={mark})')
    if not usd_futs and not coin_futs and not index and not mark: 
        klines_endpoint = 'https://api.binance.com/api/v1/klines?symbol={}&interval={}&limit={}'
    elif usd_futs: 
        if not index and not mark: 
            klines_endpoint = 'https://fapi.binance.com/fapi/v1/klines?symbol={}&interval={}&limit={}'
        if index and not mark:
            klines_endpoint = 'https://fapi.binance.com/fapi/v1/indexPriceKlines?pair={}&interval={}&limit={}'
        elif not index and mark:
            klines_endpoint = 'https://fapi.binance.com/fapi/v1/markPriceKlines?symbol={}&interval={}&limit={}'
    elif coin_futs: 
        if not index and not mark: 
            klines_endpoint = 'https://dapi.binance.com/dapi/v1/klines?symbol={}&interval={}&limit={}'
        if index and not mark:
            klines_endpoint = 'https://dapi.binance.com/dapi/v1/indexPriceKlines?pair={}&interval={}&limit={}'
        elif not index and mark:
            klines_endpoint = 'https://dapi.binance.com/dapi/v1/markPriceKlines?symbol={}&interval={}&limit={}'        

    url = klines_endpoint.format(symbol, interval, limit)
    # print(url)
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
            logger.critical(dict(origin='get_candles_worker', payload=dict(error=e, symbol=symbol, usd_futs=usd_futs, coin_futs=coin_futs, interval=interval, url=url)))
        raise e


async def get_candles(symbols, interval='1m',limit=1000, startTimes=None, endTimes=None, usd_futs=False, coin_futs=False, index=False, mark=False, logger=None, **kwargs):
    if endTimes is None:
        endTimes=[None]*len(symbols)
    if startTimes is None:
        startTimes=[None]*len(symbols)        

    try:
        result = await asyncio.gather(*[get_candles_worker(
            symbol=symbol, 
            interval=interval, 
            limit=limit, 
            startTime=startTime, 
            endTime=endTime, 
            usd_futs=usd_futs, 
            coin_futs=coin_futs,
            index=index, 
            mark=mark) for symbol, startTime,endTime in zip(symbols,startTimes,endTimes)])

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
                usd_futs=usd_futs, 
                coin_futs=coin_futs,
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
async def get_futs_stat_worker(symbol, stat=None, period=None, limit=500, usd_futs=False, coin_futs=False, startTime=None, endTime=None, logger=None, coin_futs_details=None, **kwargs):
    print(f'async get_futs_stat_worker({symbol}, stat={stat},period={period}, limit={limit},usd_futs={usd_futs}, coin_futs={coin_futs}, startTime={long_to_datetime_str(startTime)}, endTime={long_to_datetime_str(endTime)})')
    if usd_futs:
        if stat == 'funding':
            endpoint = "https://fapi.binance.com/fapi/v1/fundingRate?symbol={}&limit={}"
            url = endpoint.format(symbol, limit)
        elif stat == 'oi':
            endpoint = "https://fapi.binance.com/futures/data/openInterestHist?symbol={}&period={}&limit={}"
            url = endpoint.format(symbol, period,limit)
    elif coin_futs:
        if stat == 'funding':
            endpoint = "https://dapi.binance.com/dapi/v1/fundingRate?symbol={}&limit={}"
            url = endpoint.format(symbol, limit)
        elif stat == 'oi':
            endpoint = "https://dapi.binance.com/futures/data/openInterestHist?pair={}&contractType={}&period={}&limit={}"
            url = endpoint.format(coin_futs_details['pair'],coin_futs_details['contractType'], period,limit)

    if startTime is not None: 
        url = url + '&startTime={}'.format(startTime)
    if endTime is not None: 
        url = url + '&endTime={}'.format(endTime)        
    output = {}
    try: 
        # print(url)
        async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
            async with session.get(url) as resp:
                # print(url)
                resp = await resp.json(loads=json.loads)
                output[symbol]=resp
        return output
    except Exception as e:
        if logger is not None: 
            logger.critical(dict(origin='get_futs_stat_worker', payload=dict(error=e, symbol=symbol, stat=stat, period=period,limit=limit, usd_futs=usd_futs, coin_futs=coin_futs, startTime=startTime, endTime=endTime, url=url)))
        raise e


async def get_futs_stats(
    symbols, stat, limit, 
    period='5m',usd_futs=False, coin_futs=False, 
    startTimes=None, endTimes=None, 
    logger=None, coin_futs_details=None,**kwargs):

    if endTimes is None:
        endTimes=[None]*len(symbols)
    if startTimes is None:
        startTimes=[None]*len(symbols)        
    try:
        result = await asyncio.gather(*[
            get_futs_stat_worker(
                symbol=symbol, stat=stat, 
                period=period,limit=limit, 
                usd_futs=usd_futs, coin_futs=coin_futs, 
                startTime=startTime, endTime=endTime,
                coin_futs_details=coin_futs_details[symbol] if coin_futs_details is not None else None) for symbol, startTime,endTime in zip(symbols,startTimes,endTimes)])
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
                        usd_futs=usd_futs, 
                        coin_futs=coin_futs,
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
                        usd_futs=usd_futs, 
                        coin_futs=coin_futs,
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
