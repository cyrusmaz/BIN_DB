from copy import deepcopy

def get_symbol_info(exchange_info, filterType, info_name, symbols=None):
    """FINDS THE SYMBOLS INFO FROM FILTERS IN EXCHANGEINFO"""
    """E.G. USED FOR FINDING TICK SIZE, STEP SIZE"""
    if symbols is not None: 
        symbol_infos = list(filter(lambda x: x['symbol'] in symbols, exchange_info['symbols']))
    else: 
        symbol_infos = deepcopy(exchange_info['symbols'])
    output=dict()
    for symbol_info in symbol_infos:
        symbol = symbol_info['symbol']
        try: 
            filter_ = list(filter(lambda x: x['filterType']==filterType, symbol_info['filters']))[0]
            symbol_param = float(filter_[info_name])
        except Exception as e : 
            # print(f"{symbol} status: {symbol_info['status']}")
            # print(f'{symbol} - {e}')
            symbol_param=None
        output[symbol]=symbol_param
    return output 

def get_symbols(exchange_infos, logger):
    spot_trading = list(filter(lambda x: x['status']== 'TRADING', exchange_infos['spot']['symbols']))
    spot_trading_symbols = [d['symbol'] for d in spot_trading]

    spot_NOT_trading = list(filter(lambda x: x['status']!= 'TRADING', exchange_infos['spot']['symbols']))
    spot_NOT_trading_symbols = [d['symbol'] for d in spot_NOT_trading]

    usdf_trading = list(filter(lambda x: x['status']== 'TRADING', exchange_infos['usdf']['symbols']))
    usdf_trading_symbols = [d['symbol'] for d in usdf_trading]

    usdf_NOT_trading = list(filter(lambda x: x['status']!= 'TRADING', exchange_infos['usdf']['symbols']))
    usdf_NOT_trading_symbols = [d['symbol'] for d in usdf_NOT_trading]

    if logger is not None:
        logger.info(
            dict(
                origin='get_symbols', 
                payload=dict(
                    spot_trading = len(spot_trading),
                    spot_NOT_trading =len(spot_NOT_trading),
                    usdf_trading = len(usdf_trading),
                    usdf_NOT_trading =len(usdf_NOT_trading),                                   
                )))

    """CONSUMES DICT OF EXCHANGE INFOS AND EXTRACT SYMBOLS"""
    result = {}
    for t in exchange_infos.keys(): 
        result[t] = [d['symbol'] for d in exchange_infos[t]['symbols']]

    result['spot']=spot_trading_symbols
    result['usdf']=usdf_trading_symbols

    result['spot_not_trading']=spot_NOT_trading_symbols
    result['usdf_not_trading']=usdf_NOT_trading_symbols
    
    result['coinf_details'] = {d['symbol']:dict(pair=d['pair'], contractType=d['contractType']) for d in exchange_infos['coinf']['symbols']}
    result['usdf_details'] = {d['symbol']:dict(pair=d['pair'], contractType=d['contractType']) for d in exchange_infos['usdf']['symbols']}

    return result 