from datetime import datetime, timezone, timedelta

import sys

# sys.argv is a list of arguments: [script_name, arg1, arg2, ...]
if len(sys.argv) < 3:
    print("Usage: python *.py <LimitPrice Like 0.97¢ > <Shares Like 100 ($)>")
    sys.exit(1)
#.py 0.97 100
LimitPrice = float(sys.argv[1])
Shares = float(sys.argv[2])

print("LimitPrice:", LimitPrice)
print("Shares:", Shares)


# LimitPrice = 0.97
# Shares = 100.0

# print(LimitPrice)
# print(Shares)

def get_rounded_timestamps(interval_minutes=15):
    # current UTC time (timezone-aware)
    now_utc = datetime.now(timezone.utc)
    
    # round down to nearest interval (e.g., 15 minutes)
    rounded_minute = (now_utc.minute // interval_minutes) * interval_minutes
    rounded_utc = now_utc.replace(minute=rounded_minute, second=0, microsecond=0)
    
    # convert UTC to Eastern Time (UTC-5)
    ET_OFFSET = timedelta(hours=-5)
    rounded_et = rounded_utc.astimezone(timezone(ET_OFFSET))
    
    # timestamps
    utc_unix = int(rounded_utc.timestamp())
    et_unix = int(rounded_et.timestamp())
    
    return {
        "utc_time": rounded_utc.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "utc_unix": utc_unix,
        "et_time": rounded_et.strftime("%Y-%m-%d %I:%M:%S %p %Z"),
        "et_unix": et_unix
    }

# Example use:
times = get_rounded_timestamps()
# print(times["utc_time"])
# print(times["utc_unix"])
# print(times["et_time"])
# print(times["et_unix"])

print("Curent Market BTC 15min:", f"https://polymarket.com/event/btc-updown-15m-{times["utc_unix"]}")

import requests

times = get_rounded_timestamps()
url = f"https://gamma-api.polymarket.com/markets/slug/btc-updown-15m-{times["utc_unix"]}"

response = requests.get(url)

market = response.json()

keep_keys = ['id', 'question', 'slug', 'volume', 'clobTokenIds']

market_filtered = {k: market[k] for k in keep_keys if k in market}
market_filtered['clobTokenIds']

#market

import json

raw = market_filtered["clobTokenIds"]   # this is a string

clob_list = json.loads(raw)             # now it's a real Python list

UP_clobTokenId = clob_list[0]
DOWN_clobTokenId = clob_list[1]
print(f"Event {market_filtered['question']}")
# print(f"UP_clobTokenId {UP_clobTokenId}")
# print(f"DOWN_clobTokenId {DOWN_clobTokenId}")

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BookParams

client = ClobClient("https://clob.polymarket.com")  # read-only

#mid = client.get_midpoint(token_id)
price_UP = client.get_price(UP_clobTokenId, side="BUY")
price_DOWN = client.get_price(DOWN_clobTokenId, side="BUY")
spread = client.get_spread(UP_clobTokenId)
#book = client.get_order_book(token_id)
#books = client.get_order_books([BookParams(token_id=token_id)])
#print(mid, price, book.market, len(books))
# print(price_UP)
# print(price_DOWN)

up_down_market = float(price_UP['price']) - float(price_DOWN['price'])
up_down_token_id = ""
if up_down_market <= 0:
    print(f"Market is BUY Down {price_DOWN['price']}")
    up_down_token_id = DOWN_clobTokenId
else:
    print(f"Market is BUY UP {price_UP['price']}")
    up_down_token_id = UP_clobTokenId


from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

host: str = "https://clob.polymarket.com"
key: str = "0x4e7b1d413b107fefd8375851491fde12a286b84702f226e7751529a32f96a857" #This is your Private Key. Export from https://reveal.magic.link/polymarket or from your Web3 Extension
chain_id: int = 137 #No need to adjust this
POLYMARKET_PROXY_ADDRESS: str = '0xFCE914cC0b98696F7B390ed98f5ab15fc408d291' #This is the address listed below your profile picture when using the Polymarket site.

### Initialization of a client using a Polymarket Proxy associated with an Email/Magic account. If you login with your email use this example.
client = ClobClient(host, key=key, chain_id=chain_id, signature_type=1, funder=POLYMARKET_PROXY_ADDRESS)

client.set_api_creds(client.create_or_derive_api_creds()) 


order_args = OrderArgs(
    price=LimitPrice,
    size=Shares,
    side=BUY,
    token_id=up_down_token_id ,
)
signed_order = client.create_order(order_args)

resp_post_order = client.post_order(signed_order, OrderType.GTC)
print(resp_post_order)
