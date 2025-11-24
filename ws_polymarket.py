from websocket import WebSocketApp
import json
import time
import threading
from datetime import datetime, timezone, timedelta
import requests
import json

MARKET_CHANNEL = "market"
USER_CHANNEL = "user"



class WebSocketOrderBook:
    def __init__(self, channel_type, url, data, auth, message_callback, verbose):
        self.channel_type = channel_type
        self.url = url
        self.data = data
        self.auth = auth
        self.message_callback = message_callback
        self.verbose = verbose
        furl = url + "/ws/" + channel_type
        self.ws = WebSocketApp(
            furl,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        self.orderbooks = {}

    def on_message(self, ws, message):
        print(message)
        pass

    def on_error(self, ws, error):
        print("Error: ", error)
        exit(1)

    def on_close(self, ws, close_status_code, close_msg):
        print("closing")
        exit(0)

    def on_open(self, ws):
        if self.channel_type == MARKET_CHANNEL:
            ws.send(json.dumps({"assets_ids": self.data, "type": MARKET_CHANNEL}))
        elif self.channel_type == USER_CHANNEL and self.auth:
            ws.send(
                json.dumps(
                    {"markets": self.data, "type": USER_CHANNEL, "auth": self.auth}
                )
            )
        else:
            exit(1)

        thr = threading.Thread(target=self.ping, args=(ws,))
        thr.start()

    def ping(self, ws):
        while True:
            ws.send("PING")
            time.sleep(10)

    def run(self):
        self.ws.run_forever()

    
if __name__ == "__main__":

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

    def get_first_clob_id():

        times = get_rounded_timestamps()
        
        url = f"https://gamma-api.polymarket.com/events/slug/btc-updown-15m-{times['utc_unix']}"
        
        response = requests.get(url)
        
        event = response.json()
        
        keep_keys = ['id', 'title', 'slug', 'markets']
        
        event_filtered = {k: event[k] for k in keep_keys if k in event}
        
        markets = event_filtered['markets'][0]
        clobTokenIds = markets['clobTokenIds']
        first_clob_id = json.loads(clobTokenIds)[0]
        return first_clob_id
        #return clobTokenIds

        
    url = "wss://ws-subscriptions-clob.polymarket.com"
    #Complete these by exporting them from your initialized client. 
    api_key = "019a5744-cda4-7124-b090-5faf15429d33"
    api_secret = "XWbHqxGrJJBBo_1fECI0kwEdhNUwRQoaD96DX-S7HQs="
    api_passphrase = "21cc503b60964bcdf961235d7f49718d6550ffc9b12f1cd5fba85b2c0ba04032"

    # asset_ids = [
    #     "81359512599058474907927709206590202450188231854048001263723068230144326925685",
    # ]

    asset_id = get_first_clob_id()
    print(asset_id)
    asset_ids = [
        asset_id,
    ]
    condition_ids = [] # no really need to filter by this one

    auth = {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}

    market_connection = WebSocketOrderBook(
        MARKET_CHANNEL, url, asset_ids, auth, None, True
    )
    user_connection = WebSocketOrderBook(
        USER_CHANNEL, url, condition_ids, auth, None, True
    )

    market_connection.run()
    # user_connection.run()