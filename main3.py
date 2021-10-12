
import asyncio 

from db_helpers import *
from async_fns import get_infos
from SymbolsDBClass import symbols_db

from fill_wrappers import * 
import argparse
import json


def oi_grab_fn(
    dir_, backward, forward, db_args_dict, 
    check_existence, usd_futs, coin_futs, 
    logger, oi_interval, symbols, limit, 
    rate_limit, coin_futs_details=None):

    symbols_exist, startTimes_dict, symbols_dne = prepare_for_oi_fetch(
        dir_=dir_, 
        symbols=symbols, 
        oi_interval=oi_interval, 
        usd_futs=usd_futs, 
        coin_futs=coin_futs,    
        check_existence=check_existence,
        db_args_dict=db_args_dict)    

    if forward: 
        oi_fill_wrapper(
            dir_=dir_, 
            symbols=symbols_exist, 
            startTimes_dict=startTimes_dict,
            # dbs=dbs_exist, 
            interval=oi_interval, 
            forward=True, 
            usd_futs=usd_futs, 
            coin_futs=coin_futs,                   
            batch_size=rate_limit/4, 
            limit=limit,
            rate_limit=rate_limit,                
            logger=logger,
            db_args_dict=db_args_dict,
            coin_futs_details=coin_futs_details)            

    if backward=='exists' or backward=='exist' or backward=='all': 
        oi_fill_wrapper(
            dir_=dir_, 
            symbols=symbols_exist, 
            startTimes_dict=startTimes_dict,
            # dbs=dbs_exist, 
            interval=oi_interval, 
            forward=False, 
            usd_futs=usd_futs, 
            coin_futs=coin_futs,                   
            batch_size=rate_limit/4, 
            limit=limit,
            rate_limit=rate_limit,                
            logger=logger,
            db_args_dict=db_args_dict,
            coin_futs_details=coin_futs_details)    

    if backward=='dne' or backward=='all': 
        oi_fill_wrapper(
            dir_=dir_, 
            symbols=symbols_dne, 
            startTimes_dict=startTimes_dict,
            # dbs=dbs_dne, 
            interval=oi_interval, 
            forward=False, 
            usd_futs=usd_futs, 
            coin_futs=coin_futs,                   
            batch_size=rate_limit/4, 
            limit=limit,
            rate_limit=rate_limit,                
            logger=logger,
            db_args_dict=db_args_dict,
            coin_futs_details=coin_futs_details)  

def candle_grab_fn(
    dir_, backward, forward, db_args_dict, 
    check_existence, usd_futs, coin_futs, 
    mark, index, logger, candle_interval, 
    symbols, limit, rate_limit):
    symbols_exist, startTimes_dict, symbols_dne  = prepare_for_candle_fetch(
        dir_=dir_, 
        symbols=symbols, 
        interval=candle_interval, 
        check_existence=check_existence,
        usd_futs=usd_futs, 
        coin_futs=coin_futs,
        mark=mark,
        index=index,                  
        logger=logger,
        db_args_dict=db_args_dict)     

    if forward: 
        candle_fill_wrapper(
            symbols=symbols_exist, 
            # dbs=dbs_exist, 
            interval=candle_interval, 
            usd_futs=usd_futs, 
            coin_futs=coin_futs,
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
            # dbs=dbs_exist, 
            interval=candle_interval, 
            usd_futs=usd_futs, 
            coin_futs=coin_futs,
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
            # dbs=dbs_dne, 
            interval=candle_interval, 
            usd_futs=usd_futs, 
            coin_futs=coin_futs,
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

    parser.add_argument('--get_all', action='store_true', help='not required - default is False. gets usd_futs/coin_futs/spot candles + usd_futs/coin_futs oi/mark candles/index candles + usd_futs/coin_perps funding')
    parser.add_argument('--spot_candles', action='store_true', help='not required - default is False')

    parser.add_argument('--usd_futs_candles', action='store_true', help='not required - default is False')
    parser.add_argument('--usd_futs_mark', action='store_true', help='not required - default is False')
    parser.add_argument('--usd_futs_index', action='store_true', help='not required - default is False')  
    parser.add_argument('--usd_futs_oi', action='store_true', help='not required - default is False')
    parser.add_argument('--usd_futs_funding', action='store_true', help='not required - default is False')   

    parser.add_argument('--coin_futs_candles', action='store_true', help='not required - default is False')
    parser.add_argument('--coin_futs_mark', action='store_true', help='not required - default is False')
    parser.add_argument('--coin_futs_index', action='store_true', help='not required - default is False')  
    parser.add_argument('--coin_futs_oi', action='store_true', help='not required - default is False')
    parser.add_argument('--coin_futs_funding', action='store_true', help='not required - default is False')     

    parser.add_argument('--check_existence', action='store_true', help='not required - default is False')

    parser.add_argument('--forward', action='store_true', help='not required - default is False. either --forward or --backward (or both) must be used')
    parser.add_argument('--backward', type=str, help='one of: exists (or exist), dne, all. either --forward or --backward (or both) must be used')

    parser.add_argument('--custom_symbols', type=lambda s: [item for item in s.split(',')], help='not required - default is False')

    args = parser.parse_args()

    custom_symbols = args.custom_symbols 
    if custom_symbols is not None: 
        custom_symbols = args.custom_symbols 
        print(f'custom symbols: {custom_symbols}')


    param_path = '/mnt/nvme1n1/DB/BINANCE/params.json' if args.nvme1n1 else '/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'
    # param_path = '/home/cm/Documents/PY_DEV/DB/BINANCE/params.json'

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
        print(f'USD FUTS/COIN FUTS/SPOT CANDLES')
        print(f'USD FUTS/COIN FUTS - MARK/INDEX CANDLES')
        print(f'USD FUTS/COIN FUTS OI')
        print(f'USD FUTS/COIN PERPS FUNDING')

    print(f'check_existence: {args.check_existence}')
    print(f'forward: {args.forward}')
    print(f'backward: {args.backward}')

    print(f'candle_interval={candle_interval}')
    print(f'oi_interval={oi_interval}')        
    input('press key to begin')                                        

    ## BUILD DB PATHS:
    paths = get_directories_from_param_path(param_path=None, db_parameters=db_parameters)
    usd_futs_candles_dir=paths['usd_futs_candles_dir']
    usd_futs_index_candles_dir=paths['usd_futs_index_candles_dir']
    usd_futs_mark_candles_dir=paths['usd_futs_mark_candles_dir']
    coin_futs_candles_dir=paths['coin_futs_candles_dir']
    coin_futs_index_candles_dir=paths['coin_futs_index_candles_di']
    coin_futs_mark_candles_dir=paths['coin_futs_mark_candles_dir']
    spot_candles_dir=paths['spot_candles_dir']
    oi_dir=paths['oi_dir']
    usd_futs_oi_dir=paths['usd_futs_oi_dir']
    coin_futs_oi_dir=paths['coin_futs_oi_dir']
    funding_dir=paths['funding_dir']
    usd_futs_funding_dir=paths['usd_futs_funding_dir']
    coin_futs_funding_dir=paths['coin_futs_funding_dir']
    symbols_dir=paths['symbols_dir']
    exchange=paths['exchange']

    # # build the candles directory paths
    # candles_dir = db_dir+'CANDLES/'
    # usd_futs_candles_dir = candles_dir+'USD_FUTS/'
    # usd_futs_index_candles_dir = candles_dir+'USD_FUTS_INDEX/'
    # usd_futs_mark_candles_dir = candles_dir+'USD_FUTS_MARK/'

    # coin_futs_candles_dir = candles_dir+'COIN_FUTS/'
    # coin_futs_index_candles_dir = candles_dir+'COIN_FUTS_INDEX/'
    # coin_futs_mark_candles_dir = candles_dir+'COIN_FUTS_MARK/'

    # spot_candles_dir = candles_dir+'SPOT/'

    # # build the oi directory paths 
    # oi_dir = db_dir+'OI/'
    # usd_futs_oi_dir = oi_dir+'USD_FUTS/'
    # coin_futs_oi_dir = oi_dir+'COIN_FUTS/'

    # # build the funding directory paths
    # funding_dir = db_dir+'FUNDING/'
    # usd_futs_funding_dir =  oi_dir+'FUNDING/USD_FUTS/'
    # coin_futs_funding_dir =  oi_dir+'FUNDING/COIN_FUTS/'

    # # build the symbols directory path
    # symbols_dir = db_dir+'SYMBOLS/'






    # extract exchange and exchange_types 
    exchange=db_parameters['EXCHANGE']
    exchange_types = db_parameters['EXCHANGE_TYPES']

    # SPOT LIMITS - CANDLES
    SPOT_CANDLE_LIMIT=db_parameters['SPOT_CANDLE_LIMIT']
    SPOT_CANDLE_RATE_LIMIT=db_parameters['SPOT_CANDLE_RATE_LIMIT']

    # USD FUTS LIMITS - CANDLES, OI, FUNDING
    USD_FUTS_CANDLE_LIMIT=db_parameters['USD_FUTS_CANDLE_LIMIT']
    USD_FUTS_CANDLE_RATE_LIMIT=db_parameters['USD_FUTS_CANDLE_RATE_LIMIT']
    USD_FUTS_OI_LIMIT=db_parameters['USD_FUTS_OI_LIMIT']
    USD_FUTS_OI_RATE_LIMIT=db_parameters['USD_FUTS_OI_RATE_LIMIT']
    USD_FUTS_FUNDING_LIMIT=db_parameters['USD_FUTS_FUNDING_LIMIT']
    USD_FUTS_FUNDING_RATE_LIMIT=db_parameters['USD_FUTS_FUNDING_RATE_LIMIT']    

    # COIN FUTS LIMITS - CANDLES, OI, FUNDING
    COIN_FUTS_CANDLE_LIMIT=db_parameters['COIN_FUTS_CANDLE_LIMIT']
    COIN_FUTS_CANDLE_RATE_LIMIT=db_parameters['COIN_FUTS_CANDLE_RATE_LIMIT']
    COIN_FUTS_OI_LIMIT=db_parameters['COIN_FUTS_OI_LIMIT']
    COIN_FUTS_OI_RATE_LIMIT=db_parameters['COIN_FUTS_OI_RATE_LIMIT']
    COIN_FUTS_FUNDING_LIMIT=db_parameters['COIN_FUTS_FUNDING_LIMIT']
    COIN_FUTS_FUNDING_RATE_LIMIT=db_parameters['COIN_FUTS_FUNDING_RATE_LIMIT'] 


    # set up symbols database 
    db_symbols=symbols_db(DB_DIRECTORY=symbols_dir,DB_NAME='symbols.db',EXCHANGE='binance', LOGGER=logger)

    # grab symbols from exchange and update the db
    if args.update_symbols or args.get_all: 
        exchange_infos_dict = asyncio.run(get_infos(types=exchange_types, logger=logger))
        new_symbols_dict = get_symbols(exchange_infos=exchange_infos_dict, logger=logger)
        # raw_exchange_infos =db_symbols.get_last(raw_dump=True)
        db_symbols.update_symbols_db(new_symbols_dict, exchange_infos_dict)


    # grab last symbol insert from database 
    symbols = db_symbols.get_last()
    # instantiate symbols of interest (spot and usd futs)

    usd_futs_symbols_of_interest = symbols['usd_futs']
    spot_symbols_of_interest=symbols['spot']
    coin_futs_symbols_of_interest=symbols['coin_futs']
    coin_futs_symbols_of_interest_details=symbols['coin_futs_details']   

    coin_futs_perps = list(filter(lambda s: len(s.split('_PERP'))==2 and s.split('_PERP')[1]=='',  coin_futs_symbols_of_interest)) 
    # list(set([s['pair'] for s in coin_futs_symbols_of_interest_details.values()]))
    coin_futs_pairs_of_interest = list(set([s['pair'] for s in coin_futs_symbols_of_interest_details.values()]))

    # USD FUTS OI
    if args.usd_futs_oi or args.get_all: 
        if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

        oi_grab_fn(
            dir_=usd_futs_oi_dir,
            symbols=usd_futs_symbols_of_interest, 
            usd_futs=True, 
            coin_futs=False,    
            check_existence=args.check_existence, 
            forward=args.forward,
            backward=args.backward,   
            oi_interval=oi_interval,
            limit=USD_FUTS_OI_LIMIT,
            rate_limit=USD_FUTS_OI_RATE_LIMIT,                   
            db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange),
            logger=logger,)


    # COIN FUTS OI
    if args.coin_futs_oi or args.get_all: 
        if custom_symbols is not None: coin_futs_symbols_of_interest=custom_symbols

        oi_grab_fn(
            dir_=coin_futs_oi_dir,
            symbols=coin_futs_symbols_of_interest, 
            usd_futs=False, 
            coin_futs=True,    
            check_existence=args.check_existence, 
            forward=args.forward,
            backward=args.backward,   
            oi_interval=oi_interval,
            limit=COIN_FUTS_OI_LIMIT,
            rate_limit=COIN_FUTS_OI_RATE_LIMIT,                   
            db_args_dict=dict(TYPE='coin_futs', EXCHANGE=exchange),
            logger=logger,
            coin_futs_details=coin_futs_symbols_of_interest_details)





    if args.usd_futs_funding or args.get_all: 
        if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

        symbols, dbs_funding = prepare_for_funding_fetch(
            dir_=usd_futs_funding_dir, 
            symbols=usd_futs_symbols_of_interest, 
            usd_futs=True, coin_futs=False, 
            db_args_dict=dict(TYPE='funding', EXCHANGE=exchange))

        funding_fill_wrapper(
            symbols=symbols, 
            dbs=dbs_funding, 
            batch_size=USD_FUTS_FUNDING_RATE_LIMIT/4, 
            limit=USD_FUTS_FUNDING_LIMIT,
            rate_limit=USD_FUTS_FUNDING_RATE_LIMIT,              
            usd_futs=True, coin_futs=False, 
            logger=logger)        


    if args.coin_futs_funding or args.get_all: 
        if custom_symbols is not None: coin_futs_perps=custom_symbols

        symbols, dbs_funding = prepare_for_funding_fetch(
            dir_=coin_futs_funding_dir, 
            symbols=coin_futs_perps, 
            usd_futs=False, coin_futs=True, 
            db_args_dict=dict(TYPE='funding', EXCHANGE=exchange))

        funding_fill_wrapper(
            symbols=symbols, 
            dbs=dbs_funding, 
            batch_size=COIN_FUTS_FUNDING_RATE_LIMIT/4,
            limit=COIN_FUTS_FUNDING_LIMIT,
            rate_limit=COIN_FUTS_FUNDING_RATE_LIMIT,  
            usd_futs=False, coin_futs=True, 
            logger=logger)        



    # SPOT CANDLES
    if args.spot_candles or args.get_all: 
        if custom_symbols is not None: spot_symbols_of_interest=custom_symbols

        db_args_dict=dict(TYPE='spot', EXCHANGE=exchange)

        candle_grab_fn(
            dir_=spot_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=db_args_dict, 
            check_existence=args.check_existence, 
            usd_futs=False, 
            coin_futs=False, 
            mark=False, 
            index=False, 
            limit = SPOT_CANDLE_LIMIT,
            rate_limit = SPOT_CANDLE_RATE_LIMIT,
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=spot_symbols_of_interest)

    # USD FUTS CANDLES
    if args.usd_futs_candles or args.get_all: 
        if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

        db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange)

        candle_grab_fn(
            dir_=usd_futs_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=db_args_dict, 
            check_existence=args.check_existence, 
            usd_futs=True, 
            coin_futs=False, 
            mark=False, 
            index=False, 
            limit = USD_FUTS_CANDLE_LIMIT,
            rate_limit = USD_FUTS_CANDLE_RATE_LIMIT,          
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=usd_futs_symbols_of_interest)


    # COIN FUTS CANDLES
    if args.coin_futs_candles or args.get_all: 
        if custom_symbols is not None: coin_futs_symbols_of_interest=custom_symbols

        db_args_dict=dict(TYPE='coin_futs', EXCHANGE=exchange)

        candle_grab_fn(
            dir_=coin_futs_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=db_args_dict, 
            check_existence=args.check_existence, 
            usd_futs=False, 
            coin_futs=True, 
            mark=False, 
            index=False, 
            limit = COIN_FUTS_CANDLE_LIMIT,
            rate_limit = COIN_FUTS_CANDLE_RATE_LIMIT,          
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=coin_futs_symbols_of_interest)            

    # USD FUTS MARK
    if args.usd_futs_mark or args.get_all: 
        if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

        db_args_dict=dict(TYPE='usd_futs_mark', EXCHANGE=exchange)

        candle_grab_fn(
            dir_=usd_futs_mark_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=db_args_dict, 
            check_existence=args.check_existence, 
            usd_futs=True, 
            coin_futs=False, 
            mark=True, 
            index=False, 
            limit = USD_FUTS_CANDLE_LIMIT,
            rate_limit = USD_FUTS_CANDLE_RATE_LIMIT,              
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=usd_futs_symbols_of_interest)


    # COIN FUTS MARK
    if args.coin_futs_mark or args.get_all: 
        if custom_symbols is not None: coin_futs_symbols_of_interest=custom_symbols

        db_args_dict=dict(TYPE='coin_futs_mark', EXCHANGE=exchange)

        candle_grab_fn(
            dir_=coin_futs_mark_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=db_args_dict, 
            check_existence=args.check_existence, 
            usd_futs=False, 
            coin_futs=True, 
            mark=True, 
            index=False, 
            limit = COIN_FUTS_CANDLE_LIMIT,
            rate_limit = COIN_FUTS_CANDLE_RATE_LIMIT,              
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=coin_futs_symbols_of_interest)

    # USD FUTS INDEX 
    if args.usd_futs_index or args.get_all: 
        if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

        db_args_dict=dict(TYPE='usd_futs_index', EXCHANGE=exchange)

        candle_grab_fn(
            dir_=usd_futs_index_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=db_args_dict, 
            check_existence=args.check_existence, 
            usd_futs=True, 
            coin_futs=False, 
            mark=False, 
            index=True, 
            limit = USD_FUTS_CANDLE_LIMIT,
            rate_limit = USD_FUTS_CANDLE_RATE_LIMIT,              
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=usd_futs_symbols_of_interest)

    # COIN FUTS INDEX
    if args.coin_futs_index or args.get_all: 
        if custom_symbols is not None: coin_futs_pairs_of_interest=custom_symbols

        db_args_dict=dict(TYPE='coin_futs_index', EXCHANGE=exchange)

        candle_grab_fn(
            dir_=coin_futs_index_candles_dir, 
            backward=args.backward, 
            forward=args.forward, 
            db_args_dict=db_args_dict, 
            check_existence=args.check_existence, 
            usd_futs=False, 
            coin_futs=True, 
            mark=False, 
            index=True, 
            limit = COIN_FUTS_CANDLE_LIMIT,
            rate_limit = COIN_FUTS_CANDLE_RATE_LIMIT,              
            logger=logger, 
            candle_interval=candle_interval, 
            symbols=coin_futs_pairs_of_interest)