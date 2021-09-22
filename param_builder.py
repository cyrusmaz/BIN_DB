import json

db_parameters = dict(
    EXCHANGE             = 'binance',
    EXCHANG_TYPES        = ['usd_futs', 'coin_futs', 'spot'],
    DB_DIRECTORY         = '/mnt/nvme1n1/DB/BINANCE/',
    DB_DIRECTORY_CANDLES = '/mnt/nvme1n1/DB/BINANCE/CANDLES/',
    DB_DIRECTORY_OI      = '/mnt/nvme1n1/DB/BINANCE/OI/',
    DB_DIRECTORY_FUNDING = '/mnt/nvme1n1/DB/BINANCE/FUNDING/',
    DB_DIRECTORY_SYMBOLS = '/mnt/nvme1n1/DB/BINANCE/SYMBOLS/',
    # UPDATE_INTERVAL      = 5, 
    CANDLE_INTERVAL      = '5m',
    OI_INTERVAL          = '5m',
    SPOT_CANDLE_QUOTES   = ['USDT', 'BTC', 'ETH', ], 
    LOG_DIRECTORY        = '/mnt/nvme1n1/DB/BINANCE/LOGS/',

    SPOT_CANDLE_LIMIT=1000,
    SPOT_CANDLE_RATE_LIMIT=1200,

    FUTS_CANDLE_LIMIT=499,
    FUTS_CANDLE_RATE_LIMIT=2400/2,

    FUTS_OI_LIMIT=500,
    FUTS_OI_RATE_LIMIT=2400,

    FUTS_FUNDING_LIMIT=1000,
    FUTS_FUNDING_RATE_LIMIT=2400,

)

with open('/mnt/nvme1n1/DB/BINANCE/params.json','w') as f: 
    json.dump(db_parameters,f, indent=3)

# with open('/mnt/nvme1n1/DB/BINANCE/params.json','r') as f: 
#     s= json.load(f)







# db_parameters = dict(

#     EXCHANGE             = 'binance',
#     EXCHANG_TYPES        = ['usd_futs', 'coin_futs', 'spot'],
#     DB_DIRECTORY         = '/home/cm/Documents/PY_DEV/DB/BINANCE/',
#     DB_DIRECTORY_CANDLES = '/home/cm/Documents/PY_DEV/DB/BINANCE/CANDLES/',
#     DB_DIRECTORY_OI      = '/home/cm/Documents/PY_DEV/DB/BINANCE/OI/',
#     DB_DIRECTORY_FUNDING = '/home/cm/Documents/PY_DEV/DB/BINANCE/FUNDING/',
#     DB_DIRECTORY_SYMBOLS = '/home/cm/Documents/PY_DEV/DB/BINANCE/SYMBOLS/',
#     UPDATE_INTERVAL      = 5, 
#     CANDLE_INTERVAL      = '5m',
#     OI_INTERVAL          = '5m',
#     SPOT_CANDLE_QUOTES   = ['USDT', 'BTC', 'ETH', ], 
#     LOG_DIRECTORY        = '/home/cm/Documents/PY_DEV/DB/BINANCE/LOGS/',

#     SPOT_CANDLE_LIMIT=1000,
#     SPOT_CANDLE_RATE_LIMIT=1200,

#     FUTS_CANDLE_LIMIT=499,
#     FUTS_CANDLE_RATE_LIMIT=2400/2,

#     FUTS_OI_LIMIT=500,
#     FUTS_OI_RATE_LIMIT=2400,

#     FUTS_FUNDING_LIMIT=1000,
#     FUTS_FUNDING_RATE_LIMIT=2400,

# )



# with open('/mnt/nvme1n1/DB/BINANCE/params_old.json','w') as f: 
#     json.dump(db_parameters,f, indent=3)