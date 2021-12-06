from read_from_db_fns import *
from exchange_info_parse_fns import *

import pandas as pd
from db_helpers import long_to_datetime_str
from functools import reduce


class DB_reader():
    def __init__(self,param_path=None):
        self.candle_map_spot_usdf = [
            'open_utc', 
            'open', 
            'high', 
            'low', 
            'close',
            'base_volume',
            'close_utc',
            'quote_volume',
            'num_trades', 
            'taker_buy_base', 
            'taker_buy_quote',
            'ignore']

        self.candle_map_coinf = [
            'open_utc', 
            'open', 
            'high', 
            'low', 
            'close',
            'quote_volume',
            'close_utc',
            'base_volume',
            'num_trades', 
            'taker_buy_quote', 
            'taker_buy_base',
            'ignore']            

        self.index_mark_map = [
            'open_time',
            'open',
            'high',
            'low',
            'close',
            'ignore1',
            'close_time',
            'ignore2',
            'number_of_basic_data',
            'ignore3',
            'ignore4',
            'ignore5']

        self.funding_map = [
            'symbol', 
            'fundingTime', 
            'fundingRate']

        self.coinf_oi_map = [
            "pair",
            "contractType",
            "sumOpenInterest",
            "sumOpenInterestValue",
            "timestamp"
            ]

        self.usdf_oi_map = [
            "symbol",
            "sumOpenInterest",
            "sumOpenInterestValue",
            "timestamp"
            ]            

        self.symbols = None
        self.exchange_info_raw = None
        self.param_path = param_path
        self.symbol_info = dict()

        self.update_times = dict()
        if self.param_path is not None: self.update()

    def update(self, param_path=None):
        if param_path is not None: self.param_path=param_path
        if self.param_path is None: return

        self.symbols = read_symbols_from_db(param_path=self.param_path)
        self.exchange_info_raw = read_symbols_from_db(param_path=self.param_path, raw_dump=True)        

        self.parse_symbols_info()
        self.update_times={k:long_to_datetime_str(long=v['serverTime'],utc=False, ISO=False) for k,v in self.exchange_info_raw.items()}
        print('update_times: ')
        print(self.update_times)

    def get_relevant_symbols_for_lists(self, type_=None, symbols=[], bases=[], quotes=[]):
        intermediate_result = []
        if type_ is not None: 
            for s in symbols: 
                intermediate_result.append(
                    self.get_relevant_symbols( 
                        type_=type_, symbol=s, base=None, quote=None, return_details=False))

        if bases is not None: 
            for base in bases: 
                intermediate_result.append(
                    self.get_relevant_symbols(
                        type_=None, symbol=None, base=base, quote=None, return_details=False))

        if quotes is not None: 
            for quote in quotes: 
                intermediate_result.append(
                    self.get_relevant_symbols( 
                        type_=None, symbol=None, base=None, quote=quote, return_details=False))

        return reduce(lambda x, y: {s:x[s]+y[s] for s in x.keys()}, intermediate_result)

    def get_relevant_symbols(self, type_=None, symbol=None, base=None, quote=None, return_details=False):
        """FOR SINGLE SYMBOL OR BASE OR QUOTE"""
        if base is None and quote is None:
            """NO BASE NO QUOTE JUST SYMBOL AND TYPE"""
            base = self.symbol_info[type_][symbol]['base']

            spot_list = list(filter(lambda x: 
                self.symbol_info['spot'][x]['base']==base and x in self.symbols['spot'],
                list(self.symbol_info['spot'].keys())))

            usdf_list = list(filter(lambda x: 
                self.symbol_info['usdf'][x]['base']==base and x in self.symbols['usdf'],
                list(self.symbol_info['usdf'].keys())))

            coinf_list = list(filter(lambda x: 
                self.symbol_info['coinf'][x]['base']==base and x in self.symbols['coinf'],
                list(self.symbol_info['coinf'].keys())))

            #### BASE USDF PERP
            usdf_perp_list = list(filter(lambda x: 
                self.symbol_info['usdf'][x]['base']==base and x in self.symbols['usdf'] and self.symbol_info['usdf'][x]['contract_type']=='PERPETUAL',
                list(self.symbol_info['usdf'].keys())))

            #### BASE USDF NON-PERP
            usdf_non_perp_list = list(filter(lambda x: 
                self.symbol_info['usdf'][x]['base']==base and x in self.symbols['usdf'] and self.symbol_info['usdf'][x]['contract_type']!='PERPETUAL',
                list(self.symbol_info['usdf'].keys())))



            #### BASE COINF PERP
            coinf_perp_list = list(filter(lambda x: 
                self.symbol_info['coinf'][x]['base']==base and x in self.symbols['coinf'] and self.symbol_info['coinf'][x]['contract_type']=='PERPETUAL',
                list(self.symbol_info['coinf'].keys())))

            #### BASE COINF NON-PERP
            coinf_non_perp_list = list(filter(lambda x: 
                self.symbol_info['coinf'][x]['base']==base and x in self.symbols['coinf'] and self.symbol_info['coinf'][x]['contract_type']!='PERPETUAL',
                list(self.symbol_info['coinf'].keys())))                

        elif base is not None and quote is None: 
            """BASE ONLY"""
            spot_list = list(filter(lambda x: 
                self.symbol_info['spot'][x]['base']==base and x in self.symbols['spot'],
                list(self.symbol_info['spot'].keys())))

            usdf_list = list(filter(lambda x: 
                self.symbol_info['usdf'][x]['base']==base and x in self.symbols['usdf'],
                list(self.symbol_info['usdf'].keys())))

            coinf_list = list(filter(lambda x: 
                self.symbol_info['coinf'][x]['base']==base and x in self.symbols['coinf'],
                list(self.symbol_info['coinf'].keys())))

            #### BASE USDF PERP
            usdf_perp_list = list(filter(lambda x: 
                self.symbol_info['usdf'][x]['base']==base and x in self.symbols['usdf'] and self.symbol_info['usdf'][x]['contract_type']=='PERPETUAL',
                list(self.symbol_info['usdf'].keys())))

            #### BASE USDF NON-PERP
            usdf_non_perp_list = list(filter(lambda x: 
                self.symbol_info['usdf'][x]['base']==base and x in self.symbols['usdf'] and self.symbol_info['usdf'][x]['contract_type']!='PERPETUAL',
                list(self.symbol_info['usdf'].keys())))

            #### BASE COINF PERP
            coinf_perp_list = list(filter(lambda x: 
                self.symbol_info['coinf'][x]['base']==base and x in self.symbols['coinf'] and self.symbol_info['coinf'][x]['contract_type']=='PERPETUAL',
                list(self.symbol_info['coinf'].keys())))

            #### BASE COINF NON-PERP
            coinf_non_perp_list = list(filter(lambda x: 
                self.symbol_info['coinf'][x]['base']==base and x in self.symbols['coinf'] and self.symbol_info['coinf'][x]['contract_type']!='PERPETUAL',
                list(self.symbol_info['coinf'].keys())))




        elif base is None and quote is not None: 
            """QUOTE ONLY"""
            spot_list = list(filter(lambda x: 
                self.symbol_info['spot'][x]['quote']==quote and x in self.symbols['spot'],
                list(self.symbol_info['spot'].keys())))

            usdf_list = list(filter(lambda x: 
                self.symbol_info['usdf'][x]['quote']==quote and x in self.symbols['usdf'],
                list(self.symbol_info['usdf'].keys())))

            coinf_list = list(filter(lambda x: 
                self.symbol_info['coinf'][x]['quote']==quote and x in self.symbols['coinf'],
                list(self.symbol_info['coinf'].keys())))

            #### QUOTE USDF PERP
            usdf_perp_list = list(filter(lambda x: 
                self.symbol_info['usdf'][x]['quote']==quote and x in self.symbols['usdf'] and self.symbol_info['usdf'][x]['contract_type']=='PERPETUAL',
                list(self.symbol_info['usdf'].keys())))

            #### QUOTE USDF NON-PERP
            usdf_non_perp_list = list(filter(lambda x: 
                self.symbol_info['usdf'][x]['quote']==quote and x in self.symbols['usdf'] and self.symbol_info['usdf'][x]['contract_type']!='PERPETUAL',
                list(self.symbol_info['usdf'].keys())))

            #### QUOTE COINF PERP
            coinf_perp_list = list(filter(lambda x: 
                self.symbol_info['coinf'][x]['quote']==quote and x in self.symbols['coinf'] and self.symbol_info['coinf'][x]['contract_type']=='PERPETUAL',
                list(self.symbol_info['coinf'].keys())))

            #### QUOTE COINF NON-PERP
            coinf_non_perp_list = list(filter(lambda x: 
                self.symbol_info['coinf'][x]['quote']==quote and x in self.symbols['coinf'] and self.symbol_info['coinf'][x]['contract_type']!='PERPETUAL',
                list(self.symbol_info['coinf'].keys())))                

                

        if return_details:
            return dict(
                usdf={symbol:self.symbol_info['usdf'][symbol] for symbol in usdf_list}, 
                spot={symbol:self.symbol_info['spot'][symbol] for symbol in spot_list}, 
                coinf={symbol:self.symbol_info['coinf'][symbol] for symbol in coinf_list},
                usdf_perp=usdf_perp_list,
                usdf_non_perp = usdf_non_perp_list,
                coinf_perp = coinf_perp_list,
                coinf_non_perp = coinf_non_perp_list )
        else: 
            return dict(
                usdf=usdf_list, 
                spot=spot_list, 
                coinf=coinf_list,
                
                usdf_perp=usdf_perp_list,
                usdf_non_perp = usdf_non_perp_list,
                coinf_perp = coinf_perp_list,
                coinf_non_perp = coinf_non_perp_list                 )            

    def parse_symbols_info(self):
        ######################### USDF FUTS
        self.symbol_info['usdf']= {
            v['symbol']:{
                'contract_type': v['contractType'] if 'contractType' in v.keys() else None,
                'contract_size':v['contractSize'] if 'contractSize' in v.keys() else None,
                'base':v['baseAsset'] if 'baseAsset' in v.keys() else None,
                'quote':v['quoteAsset'] if 'quoteAsset' in v.keys() else None,

                'tick_size': get_symbol_info(exchange_info=self.exchange_info_raw['usdf'], filterType='PRICE_FILTER', info_name='tickSize', symbols=[v['symbol']])[v['symbol']],
                'step_size': {
                    'limit' : get_symbol_info(exchange_info=self.exchange_info_raw['usdf'], filterType='LOT_SIZE', info_name='stepSize', symbols=[v['symbol']])[v['symbol']],
                    'market': get_symbol_info(exchange_info=self.exchange_info_raw['usdf'], filterType='MARKET_LOT_SIZE', info_name='stepSize', symbols=[v['symbol']])[v['symbol']],
                },

                'max_qty': {
                    'limit' : get_symbol_info(exchange_info=self.exchange_info_raw['usdf'], filterType='LOT_SIZE', info_name='maxQty', symbols=[v['symbol']])[v['symbol']],
                    'market': get_symbol_info(exchange_info=self.exchange_info_raw['usdf'], filterType='MARKET_LOT_SIZE', info_name='maxQty', symbols=[v['symbol']])[v['symbol']],
                },
                'min_qty': {
                    'limit' : get_symbol_info(exchange_info=self.exchange_info_raw['usdf'], filterType='LOT_SIZE', info_name='minQty', symbols=[v['symbol']])[v['symbol']],
                    'market': get_symbol_info(exchange_info=self.exchange_info_raw['usdf'], filterType='MARKET_LOT_SIZE', info_name='minQty', symbols=[v['symbol']])[v['symbol']],
                }

                } for v in self.exchange_info_raw['usdf']['symbols']}   
      

        ######################### COIN FUTS
        # self.symbol_info['coinf'] = dict()
        # self.symbol_info['coinf']['tick_size'] = get_symbol_info(exchange_info=self.exchange_info_raw['coinf'], filterType='PRICE_FILTER', info_name='tickSize')
        self.symbol_info['coinf']= {
            v['symbol']:{
                'contract_type': v['contractType'] if 'contractType' in v.keys() else None,
                'contract_size':v['contractSize'] if 'contractSize' in v.keys() else None,
                'base':v['baseAsset'] if 'baseAsset' in v.keys() else None,
                'quote':v['quoteAsset'] if 'quoteAsset' in v.keys() else None,

                'tick_size': get_symbol_info(exchange_info=self.exchange_info_raw['coinf'], filterType='PRICE_FILTER', info_name='tickSize', symbols=[v['symbol']])[v['symbol']],
                'step_size': {
                    'limit' : get_symbol_info(exchange_info=self.exchange_info_raw['coinf'], filterType='LOT_SIZE', info_name='stepSize', symbols=[v['symbol']])[v['symbol']],
                    'market': get_symbol_info(exchange_info=self.exchange_info_raw['coinf'], filterType='MARKET_LOT_SIZE', info_name='stepSize', symbols=[v['symbol']])[v['symbol']],
                },

                'max_qty': {
                    'limit' : get_symbol_info(exchange_info=self.exchange_info_raw['coinf'], filterType='LOT_SIZE', info_name='maxQty', symbols=[v['symbol']])[v['symbol']],
                    'market': get_symbol_info(exchange_info=self.exchange_info_raw['coinf'], filterType='MARKET_LOT_SIZE', info_name='maxQty', symbols=[v['symbol']])[v['symbol']],
                },
                'min_qty': {
                    'limit' : get_symbol_info(exchange_info=self.exchange_info_raw['coinf'], filterType='LOT_SIZE', info_name='minQty', symbols=[v['symbol']])[v['symbol']],
                    'market': get_symbol_info(exchange_info=self.exchange_info_raw['coinf'], filterType='MARKET_LOT_SIZE', info_name='minQty', symbols=[v['symbol']])[v['symbol']],
                }

                } for v in self.exchange_info_raw['coinf']['symbols']}   

        ######################### SPOT        
        self.symbol_info['spot']= {
            v['symbol']:{
                'contract_type': v['contractType'] if 'contractType' in v.keys() else None,
                'contract_size':v['contractSize'] if 'contractSize' in v.keys() else None,
                'base':v['baseAsset'] if 'baseAsset' in v.keys() else None,
                'quote':v['quoteAsset'] if 'quoteAsset' in v.keys() else None,
                'permissions': v['permissions'],

                'tick_size': get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='PRICE_FILTER', info_name='tickSize', symbols=[v['symbol']])[v['symbol']],
                'step_size': {
                    'limit' : get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='LOT_SIZE', info_name='stepSize', symbols=[v['symbol']])[v['symbol']],
                    'market': get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='MARKET_LOT_SIZE', info_name='stepSize', symbols=[v['symbol']])[v['symbol']],
                },

                'max_qty': {
                    'limit' : get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='LOT_SIZE', info_name='maxQty', symbols=[v['symbol']])[v['symbol']],
                    'market': get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='MARKET_LOT_SIZE', info_name='maxQty', symbols=[v['symbol']])[v['symbol']],
                },
                'min_qty': {
                    'limit' : get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='LOT_SIZE', info_name='minQty', symbols=[v['symbol']])[v['symbol']],
                    'market': get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='MARKET_LOT_SIZE', info_name='minQty', symbols=[v['symbol']])[v['symbol']],
                }

                } for v in self.exchange_info_raw['spot']['symbols']}   

        ######################### SPOT        

    def get_spot_candles(self, symbol, interval, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True, time_index=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            min_time=min_time, max_time=max_time, 
            usdf=False, coinf=False, 
            mark=False, index=False, 
            first_n=first_n, last_n=last_n)
        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.candle_map_spot_usdf,dtype=float)
            df['time']=df['open_utc'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=['ignore'], inplace=True)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                 
            return df
        else: 
            return raw

    def get_usdf_candles(self, symbol, interval, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True,  time_index=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            usdf=True, coinf=False, 
            mark=False, index=False, 
            min_time=min_time, max_time=max_time, 
            first_n=first_n, last_n=last_n)
        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.candle_map_spot_usdf,dtype=float)
            df['time']=df['open_utc'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=['ignore'], inplace=True)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                
            return df
        else: 
            return raw

    def get_coinf_candles(self, symbol, interval, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True, time_index=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            usdf=False, coinf=True, 
            mark=False, index=False,
            min_time=min_time, max_time=max_time,  
            first_n=first_n, last_n=last_n)
        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.candle_map_coinf,dtype=float)
            df['time']=df['open_utc'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=['ignore'], inplace=True)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                
            return df
        else: 
            return raw        

    def get_usdf_index(self, symbol, interval, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True, time_index=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=self.symbols['usdf_details'][symbol]['pair'], n=n, 
            usdf=True, coinf=False, 
            mark=False, index=True, 
            min_time=min_time, max_time=max_time, 
            first_n=first_n, last_n=last_n)
        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.index_mark_map,dtype=float)
            df['time']=df['open_time'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=list(filter(lambda x: 'ignore' in x, self.index_mark_map)), inplace=True)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                
            return df
        else: 
            return raw

    def get_coinf_index(self, symbol, interval, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True, time_index=True):
        """symbol=self.symbols['coinf_details'][symbol]['pair'] UNLIKE THE OTHER 6 CANDLE FNs IN THIS CLASS"""
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=self.symbols['coinf_details'][symbol]['pair'], n=n, 
            usdf=False, coinf=True, 
            mark=False, index=True, 
            min_time=min_time, max_time=max_time, 
            first_n=first_n, last_n=last_n)
        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.index_mark_map,dtype=float)
            df['time']=df['open_time'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=list(filter(lambda x: 'ignore' in x, self.index_mark_map)), inplace=True)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                
            return df
        else: 
            return raw

    def get_usdf_mark(self, symbol, interval, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True, time_index=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            usdf=True, coinf=False, 
            mark=True, index=False, 
            min_time=min_time, max_time=max_time, 
            first_n=first_n, last_n=last_n)
        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.index_mark_map,dtype=float)
            df['time']=df['open_time'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=list(filter(lambda x: 'ignore' in x, self.index_mark_map)), inplace=True)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                
            return df
        else: 
            return raw

    def get_coinf_mark(self, symbol, interval, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True, time_index=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            usdf=False, coinf=True, 
            mark=True, index=False, 
            min_time=min_time, max_time=max_time, 
            first_n=first_n, last_n=last_n)
        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.index_mark_map,dtype=float)
            df['time']=df['open_time'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=list(filter(lambda x: 'ignore' in x, self.index_mark_map)), inplace=True)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                
            return df
        else: 
            return raw

    def get_coinf_funding(self, symbol, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True, time_index=True):
        raw = read_funding_from_db(
            param_path=self.param_path, 
            symbol=symbol, n=n, 
            usdf=False, coinf=True, 
            min_time=min_time, max_time=max_time, 
            first_n=first_n, last_n=last_n)
        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.funding_map,dtype=float)
            df['time']=df['fundingTime'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                
            return df
        else: 
            return raw
       
    def get_usdf_funding(self, symbol, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True, time_index=True):
        raw = read_funding_from_db(
            param_path=self.param_path, 
            symbol=symbol, n=n, 
            usdf=True, coinf=False, 
            min_time=min_time, max_time=max_time, 
            first_n=first_n, last_n=last_n)
        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.funding_map,dtype=float)
            df['time']=df['fundingTime'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                
            return df
        else: 
            return raw

    def get_coinf_oi(self, symbol, interval, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True, time_index=True):
        raw= read_oi_from_db(
            param_path=self.param_path, symbol=symbol, 
            usdf=False, coinf=True, 
            oi_interval=interval, n=n, 
            min_time=min_time, max_time=max_time, 
            first_n=first_n, last_n=last_n)

        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.coinf_oi_map,dtype=float)
            df['time']=df['timestamp'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                
            return df
        else: 
            return raw

    def get_usdf_oi(self, symbol, interval, n=None, first_n=None, last_n=None, min_time=None, max_time=None, return_df=False, ISO=False, utc=True, time_index=True):
        raw= read_oi_from_db(
            param_path=self.param_path, symbol=symbol, 
            usdf=True, coinf=False, 
            oi_interval=interval, n=n, 
            min_time=min_time, max_time=max_time, 
            first_n=first_n, last_n=last_n)
        if return_df: 
            df = pd.DataFrame(data=raw, columns=self.usdf_oi_map,dtype=float)
            df['time']=df['timestamp'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            if time_index: 
                df.set_index('time',inplace=True)
                df.sort_index(axis=0, inplace=True)                
            return df
        else: 
            return raw

    def get_oi_db(self, symbol, oi_interval, coinf, usdf):
        return get_oi_db(param_path=self.param_path, symbol=symbol, oi_interval=oi_interval, coinf=coinf, usdf=usdf)

    def get_candle_db(self, symbol,candle_interval,usdf, coinf, mark, index):
        return get_candle_db(param_path=self.param_path, symbol=symbol,candle_interval=candle_interval,usdf=usdf, coinf=coinf, mark=mark, index=index)        

    def get_funding_db(self, symbol, usdf, coinf):
        return get_funding_db(param_path=self.param_path, symbol=symbol, usdf=usdf, coinf=coinf)