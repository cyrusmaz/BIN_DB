from read_from_db_fns import *
from exchange_info_parse_fns import *

import pandas as pd
from db_helpers import long_to_datetime_str

class DB_reader():
    def __init__(self,param_path=None):
        self.candle_map = [
            'open_utc', 
            'open', 
            'high', 
            'low', 
            'close',
            'volume',
            'close_utc',
            'quote_asset_volume',
            'num_trades', 
            'taker_buy_base', 
            'taker_buy_quote',
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
        if self.param_path is not None: self.update()

    def update(self, param_path=None):
        if param_path is not None: self.param_path=param_path
        if self.param_path is None: return

        self.symbols = read_symbols_from_db(param_path=self.param_path)
        self.exchange_info_raw = read_symbols_from_db(param_path=self.param_path, raw_dump=True)        

        self.parse_symbols_info()

    def parse_symbols_info(self):
        ######################### USD FUTS
        self.symbol_info['usd_futs'] = dict()
        self.symbol_info['usd_futs']['tick_size'] = get_symbol_info(exchange_info=self.exchange_info_raw['usd_futs'], filterType='PRICE_FILTER', info_name='tickSize')

        # LIMIT STEP SIZE
        self.symbol_info['usd_futs']['limit']=dict()
        self.symbol_info['usd_futs']['limit']['step_size']= get_symbol_info(exchange_info=self.exchange_info_raw['usd_futs'], filterType='LOT_SIZE', info_name='stepSize')
        # LIMIT MAXQTY
        self.symbol_info['usd_futs']['limit']['max_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['usd_futs'], filterType='LOT_SIZE', info_name='maxQty')    
        # LIMIT MINQTY
        self.symbol_info['usd_futs']['limit']['min_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['usd_futs'], filterType='LOT_SIZE', info_name='minQty')        

        # MKT STEP SIZE
        self.symbol_info['usd_futs']['market']=dict()
        self.symbol_info['usd_futs']['market']['step_size']= get_symbol_info(exchange_info=self.exchange_info_raw['usd_futs'], filterType='MARKET_LOT_SIZE', info_name='stepSize')
        # MKT MAXQTY
        self.symbol_info['usd_futs']['market']['max_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['usd_futs'], filterType='MARKET_LOT_SIZE', info_name='maxQty')    
        # MKT MINQTY
        self.symbol_info['usd_futs']['market']['min_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['usd_futs'], filterType='MARKET_LOT_SIZE', info_name='minQty')      
        ######################### USD FUTS

        ######################### COIN FUTS
        self.symbol_info['coin_futs'] = dict()
        self.symbol_info['coin_futs']['tick_size'] = get_symbol_info(exchange_info=self.exchange_info_raw['coin_futs'], filterType='PRICE_FILTER', info_name='tickSize')

        # LIMIT STEP SIZE
        self.symbol_info['coin_futs']['limit']=dict()
        self.symbol_info['coin_futs']['limit']['step_size']= get_symbol_info(exchange_info=self.exchange_info_raw['coin_futs'], filterType='LOT_SIZE', info_name='stepSize')
        # LIMIT MAXQTY
        self.symbol_info['coin_futs']['limit']['max_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['coin_futs'], filterType='LOT_SIZE', info_name='maxQty')    
        # LIMIT MINQTY
        self.symbol_info['coin_futs']['limit']['min_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['coin_futs'], filterType='LOT_SIZE', info_name='minQty')        

        # MKT STEP SIZE
        self.symbol_info['coin_futs']['market']=dict()
        self.symbol_info['coin_futs']['market']['step_size']= get_symbol_info(exchange_info=self.exchange_info_raw['coin_futs'], filterType='MARKET_LOT_SIZE', info_name='stepSize')
        # MKT MAXQTY
        self.symbol_info['coin_futs']['market']['max_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['coin_futs'], filterType='MARKET_LOT_SIZE', info_name='maxQty')    
        # MKT MINQTY
        self.symbol_info['coin_futs']['market']['min_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['coin_futs'], filterType='MARKET_LOT_SIZE', info_name='minQty')      
        ######################### COIN FUTS

        ######################### SPOT
        self.symbol_info['spot'] = dict()
        self.symbol_info['spot']['tick_size'] = get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='PRICE_FILTER', info_name='tickSize')

        # LIMIT STEP SIZE
        self.symbol_info['spot']['limit']=dict()
        self.symbol_info['spot']['limit']['step_size']= get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='LOT_SIZE', info_name='stepSize')
        # LIMIT MAXQTY
        self.symbol_info['spot']['limit']['max_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='LOT_SIZE', info_name='maxQty')    
        # LIMIT MINQTY
        self.symbol_info['spot']['limit']['min_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='LOT_SIZE', info_name='minQty')        

        # MKT STEP SIZE
        self.symbol_info['spot']['market']=dict()
        self.symbol_info['spot']['market']['step_size']= get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='MARKET_LOT_SIZE', info_name='stepSize')
        # MKT MAXQTY
        self.symbol_info['spot']['market']['max_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='MARKET_LOT_SIZE', info_name='maxQty')    
        # MKT MINQTY
        self.symbol_info['spot']['market']['min_qty']= get_symbol_info(exchange_info=self.exchange_info_raw['spot'], filterType='MARKET_LOT_SIZE', info_name='minQty')      
        ######################### SPOT        


    def get_spot_candles(self, symbol, interval, n, first_n, last_n, df=False, ISO=False, utc=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            usd_futs=False, coin_futs=False, 
            mark=False, index=False, 
            first_n=first_n, last_n=last_n)
        if df: 
            df = pd.DataFrame(data=raw, columns=self.candle_map,dtype=float)
            df['time']=df['open_utc'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=['ignore'], inplace=True)
            return df
        else: 
            return raw


    def get_usdf_candles(self, symbol, interval, n, first_n, last_n, df=False, ISO=False, utc=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            usd_futs=True, coin_futs=False, 
            mark=False, index=False, 
            first_n=first_n, last_n=last_n)
        if df: 
            df = pd.DataFrame(data=raw, columns=self.candle_map,dtype=float)
            df['time']=df['open_utc'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=['ignore'], inplace=True)
            return df
        else: 
            return raw

    def get_coinf_candles(self, symbol, interval, n, first_n, last_n, df=False, ISO=False, utc=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            usd_futs=False, coin_futs=True, 
            mark=False, index=False, 
            first_n=first_n, last_n=last_n)
        if df: 
            df = pd.DataFrame(data=raw, columns=self.candle_map,dtype=float)
            df['time']=df['open_utc'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=['ignore'], inplace=True)
            return df
        else: 
            return raw        


    def get_usdf_index(self, symbol, interval, n, first_n, last_n, df=False, ISO=False, utc=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            usd_futs=True, coin_futs=False, 
            mark=False, index=True, 
            first_n=first_n, last_n=last_n)
        if df: 
            df = pd.DataFrame(data=raw, columns=self.index_mark_map,dtype=float)
            df['time']=df['open_time'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=list(filter(lambda x: 'ignore' in x, self.index_mark_map)), inplace=True)
            return df
        else: 
            return raw

    def get_coinf_index(self, symbol, interval, n, first_n, last_n, df=False, ISO=False, utc=True):
        """symbol=self.symbols['coin_futs_details'][symbol]['pair'] UNLIKE THE OTHER 6 CANDLE FNs IN THIS CLASS"""
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=self.symbols['coin_futs_details'][symbol]['pair'], n=n, 
            usd_futs=False, coin_futs=True, 
            mark=False, index=True, 
            first_n=first_n, last_n=last_n)
        if df: 
            df = pd.DataFrame(data=raw, columns=self.index_mark_map,dtype=float)
            df['time']=df['open_time'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=list(filter(lambda x: 'ignore' in x, self.index_mark_map)), inplace=True)
            return df
        else: 
            return raw

    def get_usdf_mark(self, symbol, interval, n, first_n, last_n, df=False, ISO=False, utc=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            usd_futs=True, coin_futs=False, 
            mark=True, index=False, 
            first_n=first_n, last_n=last_n)
        if df: 
            df = pd.DataFrame(data=raw, columns=self.index_mark_map,dtype=float)
            df['time']=df['open_time'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=list(filter(lambda x: 'ignore' in x, self.index_mark_map)), inplace=True)
            return df
        else: 
            return raw

    def get_coinf_mark(self, symbol, interval, n, first_n, last_n, df=False, ISO=False, utc=True):
        raw = read_candle_from_db(
            param_path=self.param_path, 
            candle_interval=interval, 
            symbol=symbol, n=n, 
            usd_futs=False, coin_futs=True, 
            mark=True, index=False, 
            first_n=first_n, last_n=last_n)
        if df: 
            df = pd.DataFrame(data=raw, columns=self.index_mark_map,dtype=float)
            df['time']=df['open_time'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            df.drop(columns=list(filter(lambda x: 'ignore' in x, self.index_mark_map)), inplace=True)
            return df
        else: 
            return raw


    def get_coinf_funding(self, symbol, n, first_n, last_n, df=False, ISO=False, utc=True):
        raw = read_funding_from_db(
            param_path=self.param_path, 
            symbol=symbol, n=n, 
            usd_futs=False, coin_futs=True, 
            first_n=first_n, last_n=last_n)
        if df: 
            df = pd.DataFrame(data=raw, columns=self.funding_map,dtype=float)
            df['time']=df['fundingTime'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            return df
        else: 
            return raw
       


    def get_usdf_funding(self, symbol, n, first_n, last_n, df=False, ISO=False, utc=True):
        raw = read_funding_from_db(
            param_path=self.param_path, 
            symbol=symbol, n=n, 
            usd_futs=True, coin_futs=False, 
            first_n=first_n, last_n=last_n)
        if df: 
            df = pd.DataFrame(data=raw, columns=self.funding_map,dtype=float)
            df['time']=df['fundingTime'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            return df
        else: 
            return raw


    def get_coinf_oi(self, symbol, interval, n, first_n, last_n, df=False, ISO=False, utc=True):
        raw= read_oi_from_db(
            param_path=self.param_path, symbol=symbol, 
            usd_futs=False, coin_futs=True, 
            oi_interval=interval, n=n, 
            first_n=first_n, last_n=last_n)

        if df: 
            df = pd.DataFrame(data=raw, columns=self.coinf_oi_map,dtype=float)
            df['time']=df['timestamp'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            return df
        else: 
            return raw

    def get_usdf_oi(self, symbol, interval, n, first_n, last_n, df=False, ISO=False, utc=True):
        raw= read_oi_from_db(
            param_path=self.param_path, symbol=symbol, 
            usd_futs=True, coin_futs=False, 
            oi_interval=interval, n=n, 
            first_n=first_n, last_n=last_n)
        if df: 
            df = pd.DataFrame(data=raw, columns=self.usdf_oi_map,dtype=float)
            df['time']=df['timestamp'].apply(long_to_datetime_str,ISO=ISO, utc=utc)
            return df
        else: 
            return raw