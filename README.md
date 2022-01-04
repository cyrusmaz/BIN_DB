# Abstract 

BIN_DB is a commandline application that asynchronously pulls data from Binance and populates a local SQLite database for each dataset. It accomodates spot, USD-M futures, COIN-M futures, as well as open interest, funding, and others. It is capable of both backfilling and forwardfilling, and it always respects the Binance rate limits imposed by Binance. Furthermore, it comes equipped with a DB_reader class which streamlines accessing the data in the local SQLite databases. 

---------------------------------------

# Important Files:

### *params.json* 
This file contains all the relevant paramaters: 
*DB_DIRECTORY*: the directory where all the databases will be stored. important rate limit parameters as well as default candle/OI resolutions,
*CANDLE_INTERVAL/OI_INTERVAL*: lists of the default candle/OI resolutions, as well as
relevant rate limit parameters. 
Note: do not change the rate limit parameters unless you know what you're doing.

### *main3.py* 
This is the commandline interface for the user to get the desired data from the exchange and populate the local databases. 

---------------------------------------

# Basic Usage:
### Pulling Data: 
> python main3.py --help 

to get an overview of all the possible commandline arguments. 

> python main3.py --get_all

to get all available data for instruments that are currently trading. This includes: 
1. candles (spot, USD-M, COIN-M)
2. index candles (USD-M, COIN-M)
3. mark candles (USD-M, COIN-M)
4. open interest (USD-M, COIN-M)
5. funding rates (USD-M, COIN-M)

### Accessing Data: 

> import sys
> 
> sys.path.insert(1, '/path/to/BIN_DB') 
>
> from DB_reader import DB_reader
> 
> param_path='/path/to/BIN_DB/params.json'
> 
> dbr = DB_reader(param_path=param_path)

to instantiate the DB_reader class. Here, */path/to/BIN_DB/params.json* is the path to the same *params.json* mentioned above under **Important Files**.

Then, you may use any of the class methods for the desired objective. The methods have self-explanatory names. In the DB_reader class, the methods whose names end with an underscore are generally for internal use by the class, while those methods without the trailing underscore in their name are intended for the end user. 
