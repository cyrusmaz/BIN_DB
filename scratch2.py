raw_exchange_infos['coin_futs']


spot_tick_size = get_symbol_info(
    exchange_info=raw_exchange_infos['coin_futs'], 
    filterType='PRICE_FILTER', 
    parameter_name='tickSize')


[s['symbol'] for s in raw_exchange_infos['coin_futs']['symbols']]


raw_exchange_infos['coin_futs']['symbols'][4].keys()