
import asyncio 

from db_helpers import *
from exchange_info_parse_fns import *
from async_fns import get_infos
from DB_class_symbols import symbols_db

from fill_wrappers import * 
import argparse
import json


def oi_grab_fn(
    dir_, backward, forward, db_args_dict, 
    check_existence, usdf, coinf, 
    logger, oi_interval, symbols, limit, 
    rate_limit, coinf_details=None):

    symbols_exist, startTimes_dict, symbols_dne = prepare_for_oi_fetch(
        dir_=dir_, 
        symbols=symbols, 
        oi_interval=oi_interval, 
        usdf=usdf, 
        coinf=coinf,    
        check_existence=check_existence,
        db_args_dict=db_args_dict)    

    if forward: 
        oi_fill_wrapper(
            dir_=dir_, 
            symbols=symbols_exist, 
            startTimes_dict=startTimes_dict,
            interval=oi_interval, 
            forward=True, 
            usdf=usdf, 
            coinf=coinf,                   
            batch_size=rate_limit/4, 
            limit=limit,
            rate_limit=rate_limit,                
            logger=logger,
            db_args_dict=db_args_dict,
            coinf_details=coinf_details)            

    if backward=='exists' or backward=='exist' or backward=='all': 
        oi_fill_wrapper(
            dir_=dir_, 
            symbols=symbols_exist, 
            startTimes_dict=startTimes_dict,
            interval=oi_interval, 
            forward=False, 
            usdf=usdf, 
            coinf=coinf,                   
            batch_size=rate_limit/4, 
            limit=limit,
            rate_limit=rate_limit,                
            logger=logger,
            db_args_dict=db_args_dict,
            coinf_details=coinf_details)    

    if backward=='dne' or backward=='all': 
        oi_fill_wrapper(
            dir_=dir_, 
            symbols=symbols_dne, 
            startTimes_dict=startTimes_dict,
            interval=oi_interval, 
            forward=False, 
            usdf=usdf, 
            coinf=coinf,                   
            batch_size=rate_limit/4, 
            limit=limit,
            rate_limit=rate_limit,                
            logger=logger,
            db_args_dict=db_args_dict,
            coinf_details=coinf_details)  

def candle_grab_fn(
    dir_, backward, forward, db_args_dict, 
    check_existence, usdf, coinf, 
    mark, index, logger, candle_interval, 
    symbols, limit, rate_limit):
    symbols_exist, startTimes_dict, symbols_dne  = prepare_for_candle_fetch(
        dir_=dir_, 
        symbols=symbols, 
        interval=candle_interval, 
        check_existence=check_existence,
        usdf=usdf, 
        coinf=coinf,
        mark=mark,
        index=index,                  
        logger=logger,
        db_args_dict=db_args_dict)     

    if forward: 
        candle_fill_wrapper(
            symbols=symbols_exist, 
            interval=candle_interval, 
            usdf=usdf, 
            coinf=coinf,
            mark=mark,
            index=index,    
            forward=True, 
            limit=limit, 
            rate_limit=rate_limit,
            batch_size=rate_limit/4, 
            db_args_dict=db_args_dict,
            dir_=dir_, 
            startTimes_dict=startTimes_dict,
            logger=logger,
            backfill=None)      

    if backward=='exists' or backward=='exist' or backward=='all': 
        candle_fill_wrapper(
            symbols=symbols_exist, 
            interval=candle_interval, 
            usdf=usdf, 
            coinf=coinf,
            mark=mark,
            index=index,   
            forward=False, 
            limit=limit, 
            rate_limit=rate_limit,            
            batch_size=rate_limit/4, 
            db_args_dict=db_args_dict,          
            dir_=dir_,  
            startTimes_dict=startTimes_dict,     
            logger=logger,
            backfill=None)

    if backward=='dne' or backward=='all': 
        candle_fill_wrapper(
            symbols=symbols_dne, 
            interval=candle_interval, 
            usdf=usdf, 
            coinf=coinf,
            mark=mark,
            index=index, 
            forward=False, 
            limit=limit, 
            rate_limit=rate_limit,            
            batch_size=rate_limit/4, 
            db_args_dict=db_args_dict,     
            dir_=dir_,   
            startTimes_dict=startTimes_dict,         
            logger=logger,
            backfill=None)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Binance Database Update Commandline Tool')

    parser.add_argument('--update_symbols', action='store_true', help='not required - default is False')

    parser.add_argument('--candle_interval', type=str, required=False, help='not required - default is taken from --params_json')

    parser.add_argument('--oi_interval', type=str, required=False, help='not required - default is taken from --params_json')

    parser.add_argument('--nvme1n1', action='store_true', help='not required - default is False')

    parser.add_argument('--get_all', action='store_true', help='not required - default is False. gets usdf/coinf/spot candles + usdf/coinf oi/mark candles/index candles + usdf/coin_perps funding')
    parser.add_argument('--spot_candles', action='store_true', help='not required - default is False')

    parser.add_argument('--usdf_candles', action='store_true', help='not required - default is False')
    parser.add_argument('--usdf_mark', action='store_true', help='not required - default is False')
    parser.add_argument('--usdf_index', action='store_true', help='not required - default is False')  
    parser.add_argument('--usdf_oi', action='store_true', help='not required - default is False')
    parser.add_argument('--usdf_funding', action='store_true', help='not required - default is False')   

    parser.add_argument('--coinf_candles', action='store_true', help='not required - default is False')
    parser.add_argument('--coinf_mark', action='store_true', help='not required - default is False')
    parser.add_argument('--coinf_index', action='store_true', help='not required - default is False')  
    parser.add_argument('--coinf_oi', action='store_true', help='not required - default is False')
    parser.add_argument('--coinf_funding', action='store_true', help='not required - default is False')     

    parser.add_argument('--check_existence', action='store_true', help='not required - default is False')

    parser.add_argument('--forward', action='store_true', help='not required - default is False. either --forward or --backward (or both) must be used')
    parser.add_argument('--backward', type=str, help='one of: exists (or exist), dne, all. either --forward or --backward (or both) must be used')

    parser.add_argument('--custom_symbols', type=lambda s: [item for item in s.split(',')], help='not required - default is False')

    args = parser.parse_args()

    custom_symbols = args.custom_symbols 
    if custom_symbols is not None: 
        print('here')
        custom_symbols = args.custom_symbols 
        print(f'custom symbols: {custom_symbols}')

    param_path = '/mnt/nvme1n1/DB/BINANCE/params.json' if args.nvme1n1 else '/home/cm/Documents/PY_DEV/BIN_DB/params.json'
    # param_path = '/home/cm/Documents/PY_DEV/DB_BIN/params.json'

    # get parameters
    with open(param_path,'r') as f: 
        db_parameters= json.load(f)

    # extract the db directory path
    db_dir = db_parameters['DB_DIRECTORY']
    log_dir = db_dir+'LOGS/'      

    # set up logger
    logger = logger_setup(LOG_DIR=log_dir)
    # extract candle and oi intervals
    candle_interval = db_parameters['CANDLE_INTERVAL'] if args.candle_interval is None else args.candle_interval
    oi_interval = db_parameters['OI_INTERVAL'] if args.oi_interval is None else args.oi_interval


    if args.get_all:
        print(f'******************* GET ALL *******************')
        print(f'UPDATE SYMBOLS')
        print(f'USDF/COINF/SPOT CANDLES')
        print(f'USDF/COINF - MARK/INDEX CANDLES')
        print(f'USDF/COINF OI')
        print(f'USDF/COIN PERPS FUNDING')

    print(f'check_existence: {args.check_existence}')
    print(f'forward: {args.forward}')
    print(f'backward: {args.backward}')

    print(f'candle_interval={candle_interval}')
    print(f'oi_interval={oi_interval}')        
    input('press key to begin')                                        

    ## BUILD DB PATHS:
    paths = get_directories_from_param_path(param_path=None, db_parameters=db_parameters)
    usdf_candles_dir=paths['usdf_candles_dir']
    usdf_index_candles_dir=paths['usdf_index_candles_dir']
    usdf_mark_candles_dir=paths['usdf_mark_candles_dir']
    coinf_candles_dir=paths['coinf_candles_dir']
    coinf_index_candles_dir=paths['coinf_index_candles_di']
    coinf_mark_candles_dir=paths['coinf_mark_candles_dir']
    spot_candles_dir=paths['spot_candles_dir']
    oi_dir=paths['oi_dir']
    usdf_oi_dir=paths['usdf_oi_dir']
    coinf_oi_dir=paths['coinf_oi_dir']
    funding_dir=paths['funding_dir']
    usdf_funding_dir=paths['usdf_funding_dir']
    coinf_funding_dir=paths['coinf_funding_dir']
    symbols_dir=paths['symbols_dir']
    exchange=paths['exchange']


    # extract exchange and exchange_types 
    exchange=db_parameters['EXCHANGE']
    exchange_types = db_parameters['EXCHANGE_TYPES']

    # SPOT LIMITS - CANDLES
    SPOT_CANDLE_LIMIT=db_parameters['SPOT_CANDLE_LIMIT']
    SPOT_CANDLE_RATE_LIMIT=db_parameters['SPOT_CANDLE_RATE_LIMIT']

    # USDF LIMITS - CANDLES, OI, FUNDING
    USDF_CANDLE_LIMIT=db_parameters['USDF_CANDLE_LIMIT']
    USDF_CANDLE_RATE_LIMIT=db_parameters['USDF_CANDLE_RATE_LIMIT']
    USDF_OI_LIMIT=db_parameters['USDF_OI_LIMIT']
    USDF_OI_RATE_LIMIT=db_parameters['USDF_OI_RATE_LIMIT']
    USDF_FUNDING_LIMIT=db_parameters['USDF_FUNDING_LIMIT']
    USDF_FUNDING_RATE_LIMIT=db_parameters['USDF_FUNDING_RATE_LIMIT']    

    # COINF LIMITS - CANDLES, OI, FUNDING
    COINF_CANDLE_LIMIT=db_parameters['COINF_CANDLE_LIMIT']
    COINF_CANDLE_RATE_LIMIT=db_parameters['COINF_CANDLE_RATE_LIMIT']
    COINF_OI_LIMIT=db_parameters['COINF_OI_LIMIT']
    COINF_OI_RATE_LIMIT=db_parameters['COINF_OI_RATE_LIMIT']
    COINF_FUNDING_LIMIT=db_parameters['COINF_FUNDING_LIMIT']
    COINF_FUNDING_RATE_LIMIT=db_parameters['COINF_FUNDING_RATE_LIMIT'] 


    # set up symbols database 
    db_symbols=symbols_db(DB_DIRECTORY=symbols_dir,DB_NAME='symbols.db',EXCHANGE=exchange, LOGGER=logger)

    # grab symbols from exchange and update the db
    if args.update_symbols or args.get_all: 
        exchange_infos_dict = asyncio.run(get_infos(types=exchange_types, logger=logger))
        new_symbols_dict = get_symbols(exchange_infos=exchange_infos_dict, logger=logger)
        # raw_exchange_infos =db_symbols.get_last(raw_dump=True)
        db_symbols.update_symbols_db(new_symbols_dict, exchange_infos_dict)


    # grab last symbol insert from database 
    symbols = db_symbols.get_last()
    # instantiate symbols of interest (spot and usd futs)

    usdf_symbols_of_interest = symbols['usdf']
    spot_symbols_of_interest=symbols['spot']
    coinf_symbols_of_interest=symbols['coinf']
    coinf_symbols_of_interest_details=symbols['coinf_details']   
    usdf_symbols_of_interest_details=symbols['usdf_details']  

    coinf_perps = list(filter(lambda s: len(s.split('_PERP'))==2 and s.split('_PERP')[1]=='',  coinf_symbols_of_interest)) 
    # list(set([s['pair'] for s in coinf_symbols_of_interest_details.values()]))
    coinf_pairs_of_interest = list(set([s['pair'] for s in coinf_symbols_of_interest_details.values()]))
    usdf_pairs_of_interest = list(set([s['pair'] for s in usdf_symbols_of_interest_details.values()]))

    # USDF OI
    if args.usdf_oi or args.get_all: 
        if custom_symbols is not None: usdf_symbols_of_interest=custom_symbols

        oi_grab_fn(
            dir_=usdf_oi_dir,
            symbols=usdf_symbols_of_interest, 
            usdf=True, 
            coinf=False,    
            check_existence=args.check_existence, 
            forward=args.forward,
            backward=args.backward,   
            oi_interval=oi_interval,
            limit=USDF_OI_LIMIT,
            rate_limit=USDF_OI_RATE_LIMIT,                   
            db_args_dict=dict(TYPE='usdf_oi', EXCHANGE=exchange),
            logger=logger,)


    # COINF OI
    if args.coinf_oi or args.get_all: 
        if custom_symbols is not None: coinf_symbols_of_interest=custom_symbols

        oi_grab_fn(
            dir_=coinf_oi_dir,
            symbols=coinf_symbols_of_interest, 
            usdf=False, 
            coinf=True,    
            check_existence=args.check_existence, 
            forward=args.forward,
            backward=args.backward,   
            oi_interval=oi_interval,
            limit=COINF_OI_LIMIT,
            rate_limit=COINF_OI_RATE_LIMIT,                   
            db_args_dict=dict(TYPE='coinf_oi', EXCHANGE=exchange),
            logger=logger,
            coinf_details=coinf_symbols_of_interest_details)





    if args.usdf_funding or args.get_all: 
        if custom_symbols is not None: usdf_symbols_of_interest=custom_symbols

        symbols, dbs_funding = prepare_for_funding_fetch(
            dir_=usdf_funding_dir, 
            symbols=usdf_symbols_of_interest, 
            usdf=True, coinf=False, 
            db_args_dict=dict(TYPE='usdf_funding', EXCHANGE=exchange))

        funding_fill_wrapper(
            symbols=symbols, 
            dbs=dbs_funding, 
            batch_size=USDF_FUNDING_RATE_LIMIT/4, 
            limit=USDF_FUNDING_LIMIT,
            rate_limit=USDF_FUNDING_RATE_LIMIT,              
            usdf=True, coinf=False, 
            logger=logger)        


    if args.coinf_funding or args.get_all: 
        if custom_symbols is not None: coinf_perps=custom_symbols

        symbols, dbs_funding = prepare_for_funding_fetch(
            dir_=coinf_funding_dir, 
            symbols=coinf_perps, 
            usdf=False, coinf=True, 
            db_args_dict=dict(TYPE='coinf_funding', EXCHANGE=exchange))

        funding_fill_wrapper(
            symbols=symbols, 
            dbs=dbs_funding, 
            batch_size=COINF_FUNDING_RATE_LIMIT/4,
            limit=COINF_FUNDING_LIMIT,
            rate_limit=COINF_FUNDING_RATE_LIMIT,  
            usdf=False, coinf=True, 
            logger=logger)        



    # SPOT CANDLES
    if args.spot_candles or args.get_all: 
        if custom_symbols is not None: spot_symbols_of_interest=custom_symbols

        candle_grab_fn(
            dir_=spot_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=dict(TYPE='spot_candles', EXCHANGE=exchange), 
            check_existence=args.check_existence, 
            usdf=False, 
            coinf=False, 
            mark=False, 
            index=False, 
            limit = SPOT_CANDLE_LIMIT,
            rate_limit = SPOT_CANDLE_RATE_LIMIT,
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=spot_symbols_of_interest)

    # USDF CANDLES
    if args.usdf_candles or args.get_all: 
        if custom_symbols is not None: usdf_symbols_of_interest=custom_symbols

        candle_grab_fn(
            dir_=usdf_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=dict(TYPE='usdf_candles', EXCHANGE=exchange), 
            check_existence=args.check_existence, 
            usdf=True, 
            coinf=False, 
            mark=False, 
            index=False, 
            limit = USDF_CANDLE_LIMIT,
            rate_limit = USDF_CANDLE_RATE_LIMIT,          
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=usdf_symbols_of_interest)


    # COINF CANDLES
    if args.coinf_candles or args.get_all: 
        if custom_symbols is not None: coinf_symbols_of_interest=custom_symbols

        candle_grab_fn(
            dir_=coinf_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=dict(TYPE='coinf_candles', EXCHANGE=exchange), 
            check_existence=args.check_existence, 
            usdf=False, 
            coinf=True, 
            mark=False, 
            index=False, 
            limit = COINF_CANDLE_LIMIT,
            rate_limit = COINF_CANDLE_RATE_LIMIT,          
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=coinf_symbols_of_interest)            

    # USDF MARK
    if args.usdf_mark or args.get_all: 
        if custom_symbols is not None: usdf_symbols_of_interest=custom_symbols

        candle_grab_fn(
            dir_=usdf_mark_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=dict(TYPE='usdf_mark', EXCHANGE=exchange), 
            check_existence=args.check_existence, 
            usdf=True, 
            coinf=False, 
            mark=True, 
            index=False, 
            limit = USDF_CANDLE_LIMIT,
            rate_limit = USDF_CANDLE_RATE_LIMIT,              
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=usdf_symbols_of_interest)


    # COINF MARK
    if args.coinf_mark or args.get_all: 
        if custom_symbols is not None: coinf_symbols_of_interest=custom_symbols

        candle_grab_fn(
            dir_=coinf_mark_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=dict(TYPE='coinf_mark', EXCHANGE=exchange), 
            check_existence=args.check_existence, 
            usdf=False, 
            coinf=True, 
            mark=True, 
            index=False, 
            limit = COINF_CANDLE_LIMIT,
            rate_limit = COINF_CANDLE_RATE_LIMIT,              
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=coinf_symbols_of_interest)

    # USDF INDEX 
    if args.usdf_index or args.get_all: 
        if custom_symbols is not None: usdf_pairs_of_interest=custom_symbols

        candle_grab_fn(
            dir_=usdf_index_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=dict(TYPE='usdf_index', EXCHANGE=exchange), 
            check_existence=args.check_existence, 
            usdf=True, 
            coinf=False, 
            mark=False, 
            index=True, 
            limit = USDF_CANDLE_LIMIT,
            rate_limit = USDF_CANDLE_RATE_LIMIT,              
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=usdf_pairs_of_interest)

    # COINF INDEX
    if args.coinf_index or args.get_all: 
        if custom_symbols is not None: coinf_pairs_of_interest=custom_symbols

        candle_grab_fn(
            dir_=coinf_index_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=dict(TYPE='coinf_index', EXCHANGE=exchange),
            check_existence=args.check_existence, 
            usdf=False, 
            coinf=True, 
            mark=False, 
            index=True, 
            limit = COINF_CANDLE_LIMIT,
            rate_limit = COINF_CANDLE_RATE_LIMIT,              
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=coinf_pairs_of_interest)