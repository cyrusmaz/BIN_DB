import aiohttp, asyncio, json, requests
from async_fns import get_futs_stats

def generic_req(url):
    response = requests.get(url)
    response = json.loads(response.text)
    return response


url='https://binance.com/fapi/v1/exchangeInfo'
finfo = generic_req(url)



####################################
async def get_oi(symbol):
    oi_endpoint = "https://fapi.binance.com/fapi/v1/openInterest?symbol={}"
    url = oi_endpoint.format(symbol)
    print(url)
    output = {}
    async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
        async with session.get(url) as resp:
            resp = await resp.json(loads=json.loads)
            try:
                oi = resp["openInterest"]
                output[symbol]=float(oi)
            except: 
                print(resp)
                print(symbol)
                # output[symbol]=float(-69)
    return output

async def get_ois(symbols):
    result = await asyncio.gather(*[get_oi(symbol) for symbol in symbols])
    output = {}
    for d in result:
        output.update(d)
    return output



finfod = {x['symbol']:x for x in finfo['symbols']}
finfod['SCUSDT']

perps = [x['symbol'] for x in  finfo['symbols'] if x['contractType']=='PERPETUAL' and x['status']=='TRADING' ]




oi_dict = asyncio.run(get_ois(perps))
d= oi_dict
sorted(d.items(), key=lambda x: x[1])




data = asyncio.run(
    get_futs_stats(**dict(symbols=perps, stat='oi', 
    period='1d', limit=10,
    usdf=True, coinf=False, 
    endTimes=None, logger=None,
    coinf_details=NotImplemented)))