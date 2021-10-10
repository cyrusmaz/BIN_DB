
# from API_RATES import FUTS_CANDLE_RATE_LIMIT, FUTS_OI_RATE_LIMIT, FUTS_FUNDING_RATE_LIMIT, SPOT_CANDLE_RATE_LIMIT
import asyncio 
# import aiohttp
# import os
import time
# from math import ceil

from db_helpers import *
from async_fns import get_infos
from SymbolsDBClass import symbols_db
# from CandleDBClass import candle_db
# from OIDBClass import oi_db
# from FundingDBClass import funding_db


from fill_wrappers import * 
import argparse
import json
import sys


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Binance Database Update Commandline Tool')

    parser.add_argument('--update_symbols', action='store_true', help='not required - default is False')

    parser.add_argument('--candle_interval', type=str, required=False, help='not required - default is taken from --params_json')

    parser.add_argument('--oi_interval', type=str, required=False, help='not required - default is taken from --params_json')

    # parser.add_argument('--params_json', type=str, default='/mnt/nvme1n1/DB/BINANCE/params.json', help='not required - default is /mnt/nvme1n1/DB/BINANCE/params.json')

    parser.add_argument('--nvme1n1', action='store_true', help='not required - default is False')

    parser.add_argument('--oi', action='store_true', help='not required - default is False')
    parser.add_argument('--funding', action='store_true', help='not required - default is False')
    parser.add_argument('--spot_candles', action='store_true', help='not required - default is False')
    parser.add_argument('--usd_futs_candles', action='store_true', help='not required - default is False')
    parser.add_argument('--usd_futs_mark', action='store_true', help='not required - default is False')
    parser.add_argument('--usd_futs_index', action='store_true', help='not required - default is False')    
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

    # set up logger
    logger = logger_setup(LOG_DIR=db_parameters['LOG_DIRECTORY'])
    # extract candle and oi intervals
    candle_interval = db_parameters['CANDLE_INTERVAL'] if args.candle_interval is None else args.candle_interval
    oi_interval = db_parameters['OI_INTERVAL'] if args.oi_interval is None else args.oi_interval
    # extract candles directory
    usd_futs_candles_dir = db_parameters['DB_DIRECTORY_CANDLES']+'USD_FUTS/'

    usd_futs_index_candles_dir = db_parameters['DB_DIRECTORY_CANDLES']+'USD_FUTS_INDEX/'
    usd_futs_mark_candles_dir = db_parameters['DB_DIRECTORY_CANDLES']+'USD_FUTS_MARK/'

    spot_candles_dir = db_parameters['DB_DIRECTORY_CANDLES']+'SPOT/'
    # extract oi and fundiing directories
    usd_futs_oi_dir = db_parameters['DB_DIRECTORY_OI']
    usd_futs_funding_dir =  db_parameters['DB_DIRECTORY_FUNDING']
    # extract exchange and exchange_types 
    exchange=db_parameters['EXCHANGE']
    exchange_types = db_parameters['EXCHANG_TYPES']


    SPOT_CANDLE_LIMIT=db_parameters['SPOT_CANDLE_LIMIT']
    SPOT_CANDLE_RATE_LIMIT=db_parameters['SPOT_CANDLE_RATE_LIMIT']
    FUTS_CANDLE_LIMIT=db_parameters['FUTS_CANDLE_LIMIT']
    FUTS_CANDLE_RATE_LIMIT=db_parameters['FUTS_CANDLE_RATE_LIMIT']
    FUTS_OI_LIMIT=db_parameters['FUTS_OI_LIMIT']
    FUTS_OI_RATE_LIMIT=db_parameters['FUTS_OI_RATE_LIMIT']
    FUTS_FUNDING_LIMIT=db_parameters['FUTS_FUNDING_LIMIT']
    FUTS_FUNDING_RATE_LIMIT=db_parameters['FUTS_FUNDING_RATE_LIMIT']    



    # set up symbols database 
    db_symbols=symbols_db(DB_DIRECTORY=db_parameters['DB_DIRECTORY_SYMBOLS'],DB_NAME='symbols.db',EXCHANGE='binance', LOGGER=logger)

    # grab symbols from exchange and update the db
    if args.update_symbols: 
        exchange_infos_dict = asyncio.run(get_infos(types=exchange_types, logger=logger))
        new_symbols_dict = get_symbols(exchange_infos=exchange_infos_dict, logger=logger)
        db_symbols.update_symbols_db(new_symbols_dict, exchange_infos_dict)

    # grab last symbol insert from database 
    symbols = db_symbols.get_last()
    # instantiate symbols of interest (spot and usd futs)
    usd_futs_symbols_of_interest = symbols['usd_futs']
    spot_symbols_of_interest=symbols['spot']

    if args.oi: 
        if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

        symbols_exist, dbs_exist,symbols_dne,dbs_dne = prepare_for_oi_fetch(
            dir_=usd_futs_oi_dir, 
            symbols=usd_futs_symbols_of_interest, 
            oi_interval=oi_interval, 
            check_existence=args.check_existence,
            db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange))    

        if args.forward: 
            oi_fill_wrapper(
                symbols=symbols_exist, 
                dbs=dbs_exist, 
                interval=oi_interval, 
                forward=True, 
                batch_size=FUTS_OI_RATE_LIMIT/4, 
                logger=logger)            

        if args.backward=='exists' or args.backward=='exist' or args.backward=='all': 
            oi_fill_wrapper(
                symbols=symbols_exist, 
                dbs=dbs_exist, 
                interval=oi_interval, 
                forward=False, 
                batch_size=FUTS_OI_RATE_LIMIT/4, 
                logger=logger)    

        if args.backward=='dne' or args.backward=='all': 
            oi_fill_wrapper(
                symbols=symbols_dne, 
                dbs=dbs_dne, 
                interval=oi_interval, 
                forward=False, 
                batch_size=FUTS_OI_RATE_LIMIT/4, 
                logger=logger)  





    if args.funding: 
        if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

        symbols, dbs_funding = prepare_for_funding_fetch(
            dir_=usd_futs_funding_dir, 
            symbols=usd_futs_symbols_of_interest, 
            db_args_dict=dict(TYPE='funding', EXCHANGE=exchange))

        funding_fill_wrapper(
            symbols=symbols, 
            dbs=dbs_funding, 
            batch_size=FUTS_FUNDING_RATE_LIMIT/4, 
            logger=None)        




    if args.spot_candles: 
        if custom_symbols is not None: spot_symbols_of_interest=custom_symbols

        db_args_dict=dict(TYPE='spot', EXCHANGE=exchange)

        symbols_exist, startTimes_dict, symbols_dne = prepare_for_candle_fetch(
            dir_=spot_candles_dir, 
            symbols=spot_symbols_of_interest, 
            candle_interval=candle_interval, 
            check_existence=args.check_existence,
            futs=False, 
            mark=False,
            index=False,            
            logger=logger,
            db_args_dict=db_args_dict)     

        # print(symbols_exist)
        # print(startTimes_dict)
        # print(symbols_dne)

        if args.forward: 
            candle_fill_wrapper(
                symbols=symbols_exist, 
                # dbs=dbs_exist, 
                interval=candle_interval, 
                futs=False, 
                mark=False,
                index=False,
                forward=True, 
                batch_size=SPOT_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,
                dir_=spot_candles_dir, 
                startTimes_dict=startTimes_dict,
                logger=logger,
                backfill=None)      

        if args.backward=='exists' or args.backward=='exist' or args.backward=='all': 
            candle_fill_wrapper(
                symbols=symbols_exist, 
                # dbs=dbs_exist, 
                interval=candle_interval, 
                futs=False, 
                mark=False,
                index=False,       
                forward=False, 
                batch_size=SPOT_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,          
                dir_=spot_candles_dir,     
                startTimes_dict=startTimes_dict,  
                logger=logger,
                backfill=None)

        if args.backward=='dne' or args.backward=='all': 
            candle_fill_wrapper(
                symbols=symbols_dne, 
                # dbs=dbs_dne, 
                interval=candle_interval, 
                futs=False, 
                mark=False,
                index=False,       
                # backfill=3,
                forward=False, 
                batch_size=SPOT_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,     
                dir_=spot_candles_dir,   
                startTimes_dict=startTimes_dict,         
                logger=logger,
                backfill=None)









################################

    if args.usd_futs_candles: 
        if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

        db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange)

        symbols_exist, startTimes_dict, symbols_dne  = prepare_for_candle_fetch(
            dir_=usd_futs_candles_dir, 
            symbols=usd_futs_symbols_of_interest, 
            candle_interval=candle_interval, 
            check_existence=args.check_existence,
            futs=True, 
            mark=False,
            index=False,                  
            logger=logger,
            db_args_dict=db_args_dict)     

        if args.forward: 
            candle_fill_wrapper(
                symbols=symbols_exist, 
                # dbs=dbs_exist, 
                interval=candle_interval, 
                futs=True, 
                mark=False,
                index=False,    
                forward=True, 
                batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,
                dir_=usd_futs_candles_dir, 
                startTimes_dict=startTimes_dict,
                logger=logger,
                backfill=None)      

        if args.backward=='exists' or args.backward=='exist' or args.backward=='all': 
            candle_fill_wrapper(
                symbols=symbols_exist, 
                # dbs=dbs_exist, 
                interval=candle_interval, 
                futs=True, 
                mark=False,
                index=False,   
                forward=False, 
                batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,          
                dir_=usd_futs_candles_dir,  
                startTimes_dict=startTimes_dict,     
                logger=logger,
                backfill=None)

        if args.backward=='dne' or args.backward=='all': 
            candle_fill_wrapper(
                symbols=symbols_dne, 
                # dbs=dbs_dne, 
                interval=candle_interval, 
                futs=True, 
                mark=False,
                index=False,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
                # backfill=3,
                forward=False, 
                batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,     
                dir_=usd_futs_candles_dir,   
                startTimes_dict=startTimes_dict,         
                logger=logger,
                backfill=None)

######################
    ##### MARK

    if args.usd_futs_mark: 
        if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

        db_args_dict=dict(TYPE='usd_futs_mark', EXCHANGE=exchange)

        symbols_exist, startTimes_dict, symbols_dne  = prepare_for_candle_fetch(
            dir_=usd_futs_mark_candles_dir, 
            symbols=usd_futs_symbols_of_interest, 
            candle_interval=candle_interval, 
            check_existence=args.check_existence,
            futs=True, 
            mark=True,
            index=False,                  
            logger=logger,
            db_args_dict=db_args_dict)     

        if args.forward: 
            candle_fill_wrapper(
                symbols=symbols_exist, 
                # dbs=dbs_exist, 
                interval=candle_interval, 
                futs=True, 
                mark=True,
                index=False,    
                forward=True, 
                batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,
                dir_=usd_futs_mark_candles_dir, 
                startTimes_dict=startTimes_dict,
                logger=logger,
                backfill=None)      

        if args.backward=='exists' or args.backward=='exist' or args.backward=='all': 
            candle_fill_wrapper(
                symbols=symbols_exist, 
                # dbs=dbs_exist, 
                interval=candle_interval, 
                futs=True, 
                mark=True,
                index=False,   
                forward=False, 
                batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,          
                dir_=usd_futs_mark_candles_dir,  
                startTimes_dict=startTimes_dict,     
                logger=logger,
                backfill=None)

        if args.backward=='dne' or args.backward=='all': 
            candle_fill_wrapper(
                symbols=symbols_dne, 
                # dbs=dbs_dne, 
                interval=candle_interval, 
                futs=True, 
                mark=True,
                index=False,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
                # backfill=3,
                forward=False, 
                batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,     
                dir_=usd_futs_mark_candles_dir,   
                startTimes_dict=startTimes_dict,         
                logger=logger,
                backfill=None)



####################################
##### INDEX

    if args.usd_futs_index: 
        if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

        db_args_dict=dict(TYPE='usd_futs_index', EXCHANGE=exchange)

        symbols_exist, startTimes_dict, symbols_dne  = prepare_for_candle_fetch(
            dir_=usd_futs_index_candles_dir, 
            symbols=usd_futs_symbols_of_interest, 
            candle_interval=candle_interval, 
            check_existence=args.check_existence,
            futs=True, 
            mark=False,
            index=False,                  
            logger=logger,
            db_args_dict=db_args_dict)     

        if args.forward: 
            candle_fill_wrapper(
                symbols=symbols_exist, 
                # dbs=dbs_exist, 
                interval=candle_interval, 
                futs=True, 
                mark=False,
                index=False,    
                forward=True, 
                batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,
                dir_=usd_futs_index_candles_dir, 
                startTimes_dict=startTimes_dict,
                logger=logger,
                backfill=None)      

        if args.backward=='exists' or args.backward=='exist' or args.backward=='all': 
            candle_fill_wrapper(
                symbols=symbols_exist, 
                # dbs=dbs_exist, 
                interval=candle_interval, 
                futs=True, 
                mark=False,
                index=False,   
                forward=False, 
                batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,          
                dir_=usd_futs_index_candles_dir,  
                startTimes_dict=startTimes_dict,     
                logger=logger,
                backfill=None)

        if args.backward=='dne' or args.backward=='all': 
            candle_fill_wrapper(
                symbols=symbols_dne, 
                # dbs=dbs_dne, 
                interval=candle_interval, 
                futs=True, 
                mark=False,
                index=False,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
                # backfill=3,
                forward=False, 
                batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
                db_args_dict=db_args_dict,     
                dir_=usd_futs_index_candles_dir,   
                startTimes_dict=startTimes_dict,         
                logger=logger,
                backfill=None)











    # if args.usd_futs_candles: 
    #     if custom_symbols is not None: usd_futs_symbols_of_interest=custom_symbols

    #     symbols_exist,dbs_exist,symbols_dne,dbs_dne = prepare_for_candle_fetch(
    #         dir_=usd_futs_candles_dir, 
    #         symbols=usd_futs_symbols_of_interest, 
    #         candle_interval=candle_interval, 
    #         check_existence=args.check_existence,
    #         logger=logger,
    #         db_args_dict=dict(TYPE='usd_futs', EXCHANGE=exchange))
    
    #     if args.forward: 
    #         candle_fill_wrapper(
    #             symbols=symbols_exist, 
    #             dbs=dbs_exist, 
    #             interval=candle_interval, 
    #             futs=True, 
    #             forward=True, 
    #             batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
    #             logger=logger)

    #     if args.backward=='exists' or args.backward=='exist' or args.backward=='all': 
    #         candle_fill_wrapper(
    #             symbols=symbols_exist, 
    #             dbs=dbs_exist, 
    #             interval=candle_interval, 
    #             futs=True, 
    #             forward=False, 
    #             batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
    #             logger=logger)

    #     if args.backward=='dne' or args.backward=='all': 
    #         candle_fill_wrapper(
    #             symbols=symbols_dne, 
    #             dbs=dbs_dne, 
    #             interval=candle_interval, 
    #             futs=True, 
    #             # backfill=3,
    #             forward=False, 
    #             batch_size=FUTS_CANDLE_RATE_LIMIT/4, 
    #             logger=logger)
