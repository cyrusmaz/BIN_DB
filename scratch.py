candle_db(DB_DIRECTORY=db_dir, DB_NAME=db_name, INTERVAL=candle_interval, SYMBOL=s, **db_args_dict)   

s='SUNBUSD'
dir_='/mnt/nvme1n1/DB/BINANCE/CANDLES/SPOT/'
candle_interval='5m'
db_name=f'{s}_{candle_interval}_candle.db'
db_dir = f'{dir_}{candle_interval}/'   
db_args_dict={'TYPE': 'spot', 'EXCHANGE': 'binance'}


# d=candle_db(
#     dir_='/mnt/nvme1n1/DB/BINANCE/CANDLES/SPOT/',
#     symbol='SUNBUSD',
#     candle_interval='5m', 
#     db_args_dict={'TYPE': 'spot', 'EXCHANGE': 'binance'}
#     )
/mnt/nvme1n1/DB/BINANCE/CANDLES/SPOT/5m/KAVABUSD_5m_candle.db

s='KAVABUSD'

db=candle_db(DB_DIRECTORY='/mnt/nvme1n1/DB/BINANCE/CANDLES/SPOT/5m/', DB_NAME='KAVABUSD_5m_candle.db', INTERVAL=candle_interval, SYMBOL=s, **db_args_dict)   
db.get_last()


db.get_last()







symbols_exist,dbs_exist,symbols_dne,dbs_dne = prepare_for_candle_fetch(
    dir_=spot_candles_dir, 
    symbols=spot_symbols_of_interest, 
    candle_interval=candle_interval, 
    check_existence=True,
    logger=logger,
    db_args_dict=dict(TYPE='spot', EXCHANGE=exchange))     










