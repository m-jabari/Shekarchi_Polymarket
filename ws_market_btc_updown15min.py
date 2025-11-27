# from websocket import WebSocketApp
# import json
# import time
# import threading
# from datetime import datetime, timezone
# import requests
# import pyarrow as pa
# import pyarrow.parquet as pq
# import os
# import pytz

# MARKET_CHANNEL = "market"
# USER_CHANNEL = "user"
# NY_TZ = pytz.timezone("America/New_York")


# class WebSocketOrderBook:
#     def __init__(self, channel_type, url, data, auth, message_callback, verbose):
#         self.channel_type = channel_type
#         self.url = url
#         self.data = data
#         self.auth = auth
#         self.message_callback = message_callback
#         self.verbose = verbose

#         furl = url + "/ws/" + channel_type
#         self.ws = WebSocketApp(
#             furl,
#             on_message=self.on_message,
#             on_error=self.on_error,
#             on_close=self.on_close,
#             on_open=self.on_open,
#         )

#         # Thread-safe message buffer
#         self.buffer = []
#         self.lock = threading.Lock()

#         # Output parquet path (ET accurate with DST)
#         et_time = datetime.now(timezone.utc).astimezone(NY_TZ)
#         ts = et_time.strftime("%Y%m%d_%H%M")
#         self.outfile = f"polymarket_BTC_UpDown_15min_{ts}.parquet"

#         # Start background writer thread
#         writer_thread = threading.Thread(target=self._flush_loop, daemon=True)
#         writer_thread.start()
    
    
#     def _flush_loop(self):
#         """Periodically flush buffered messages to a single valid Parquet file."""
#         schema = pa.schema([("timestamp", pa.string()), ("raw", pa.string())])
#         writer = None

#         try:
#             while True:
#                 time.sleep(2)
#                 with self.lock:
#                     if not self.buffer:
#                         continue
#                     batch = self.buffer
#                     self.buffer = []

#                 table = pa.Table.from_pylist(batch, schema=schema)

#                 # Create the writer once
#                 if writer is None:
#                     writer = pq.ParquetWriter(self.outfile, schema)

#                 writer.write_table(table)

#                 if self.verbose:
#                     print(f"[{datetime.now(timezone.utc).isoformat()}] Flushed {len(batch)} rows → {self.outfile}")

#         finally:
#             # Ensure footer is written even if you Ctrl+C
#             if writer is not None:
#                 writer.close()
#                 print(f"✅ Closed ParquetWriter: {self.outfile}")        

#     def dump_message(self, message):
#         """
#         Collect messages in memory and flush to Parquet in append mode.
#         Real append — no overwrite.
#         """
#         try:
#             et_time = datetime.now(timezone.utc).astimezone(NY_TZ)
#             ts = et_time.strftime("%Y%m%d_%H%M")
#             self.outfile = f"polymarket_BTC_UpDown_15min_{ts}.parquet"
    
#             # Initialize structures
#             if not hasattr(self, "_buffer"):
#                 self._buffer = []
#                 self._buffer_lock = threading.Lock()
    
#             # Append to in-memory buffer
#             with self._buffer_lock:
#                 self._buffer.append({
#                     "timestamp": datetime.now(timezone.utc).isoformat(),
#                     "raw": message
#                 })
    
#             # Flush every 100 messages (tune as needed)
#             if len(self._buffer) >= 50:
#                 self._flush_to_parquet()
    
#         except Exception as e:
#             print(f"[Error dumping message] {e}")
    
    
#     def _flush_to_parquet(self):
#         """
#         Internal helper to flush the buffer to disk, appending to existing parquet.
#         """
#         try:
#             with self._buffer_lock:
#                 if not self._buffer:
#                     return
#                 data = self._buffer
#                 self._buffer = []
    
#             table = pa.Table.from_pylist(data)
    
#             if not os.path.exists(self.outfile):
#                 pq.write_table(table, self.outfile, compression="snappy")
#             else:
#                 # True append using pandas bridge (safe incremental append)
#                 import pandas as pd
#                 from fastparquet import write as fp_write, ParquetFile
    
#                 df = pd.DataFrame(data)
#                 fp_write(self.outfile, df, compression="SNAPPY", append=True)
    
#         except Exception as e:
#             print(f"[Error flushing parquet] {e}")



#     def on_message(self, ws, message):
#         try:
#             data = json.loads(message)
#             self.dump_message(message)
#         except:
#             print("Invalid JSON:", message)
#             return
    
#         # CASE 1 — It's a list, skip or handle differently
#         if isinstance(data, list):
#             # Optional: print for debugging
#             # print("List message:", data)
#             return
    
#         # CASE 2 — It's a dict but not a price_change event
#         if "event_type" not in data:
#             return
    
#         if data["event_type"] != "price_change":
#             return
            

#         # CASE 3 — Valid price_changes message
#         for p in data.get("price_changes", []):
#             if p.get("side") != "BUY":
#                 continue
#             # if p.get("side") == UP_ID:
#             #     print(f"UP best_ask: {best_ask}")
#             # elif p.get("side") == DOWN_ID:
#             #     print(f"DOWN best_ask: {best_ask}")
#             asset_id = p.get("asset_id")
#             side = p.get("side")
#             price = p.get("price")
#             size = p.get("size")
#             best_bid = p.get("best_bid")
#             best_ask = p.get("best_ask")
#             timestamp = str(p.get('ts'))
#             #print(f"side: {side} | price: {price} | size: {size} | best_bid: {best_bid} | best_ask: {best_ask} | {timestamp}")
#             print(f"best_ask: {best_ask} | {asset_id}")



            
#         # # CASE 3 — Valid price_changes message
#         # for p in data.get("tick_size_change", []):
    
#         #     print(f"market: {msg.get('market')}")# | old_tick_size: {msg['tick_size']['old']} | new_tick_size: {msg['tick_size']['new']} | {str(msg.get('ts')}")



#     def on_error(self, ws, error):
#         print("Error:", error)
#         exit(1)

#     def on_close(self, ws, close_status_code, close_msg):
#         print("Connection closed")
#         if writer is not None:
#             writer.close()
#         exit(0)

#     def on_open(self, ws):
#         """Subscribe to channel."""
#         if self.channel_type == MARKET_CHANNEL:
#             ws.send(json.dumps({"assets_ids": self.data, "type": MARKET_CHANNEL}))
#         elif self.channel_type == USER_CHANNEL and self.auth:
#             ws.send(json.dumps({"markets": self.data, "type": USER_CHANNEL, "auth": self.auth}))
#         else:
#             print("Invalid channel or missing auth")
#             exit(1)

#         # Start keepalive ping
#         thr = threading.Thread(target=self.ping, args=(ws,), daemon=True)
#         thr.start()

#     def ping(self, ws):
#         while True:
#             try:
#                 ws.send("PING")
#                 time.sleep(10)
#             except Exception as e:
#                 print("Ping failed:", e)
#                 break

#     def run(self):
#         self.ws.run_forever()
    
#     # def close_writer(self):
#     #     """Ensure the writer is properly closed on shutdown."""
#     #     if hasattr(self, "_writer"):
#     #         with self._parquet_lock:
#     #             self._writer.close()
#     def close_writer(self):
#         self._flush_to_parquet()

# def get_rounded_timestamps(interval_minutes=15):
#     now_utc = datetime.now(timezone.utc)
#     rounded_minute = (now_utc.minute // interval_minutes) * interval_minutes
#     rounded_utc = now_utc.replace(minute=rounded_minute, second=0, microsecond=0)
#     rounded_et = rounded_utc.astimezone(NY_TZ)
#     utc_unix = int(rounded_utc.timestamp())
#     et_unix = int(rounded_et.timestamp())
#     return {
#         "utc_time": rounded_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
#         "utc_unix": utc_unix,
#         "et_time": rounded_et.strftime("%Y-%m-%d %I:%M:%S %p %Z"),
#         "et_unix": et_unix,
#     }


# def get_first_clob_id():
#     """Fetch latest event and return first CLOB token id."""
#     times = get_rounded_timestamps()
#     url = f"https://gamma-api.polymarket.com/events/slug/btc-updown-15m-{times['utc_unix']}"
#     resp = requests.get(url, timeout=5)
#     resp.raise_for_status()
#     event = resp.json()
#     markets = event.get("markets", [])
#     if not markets:
#         raise RuntimeError("No markets in event response")
#     clob_token_ids = markets[0].get("clobTokenIds")
#     if isinstance(clob_token_ids, str):
#         first = json.loads(clob_token_ids)[0]
#     elif isinstance(clob_token_ids, list):
#         first = clob_token_ids[0]
#     else:
#         raise RuntimeError("Unexpected clobTokenIds type")
#     return first


# if __name__ == "__main__":
#     url = "wss://ws-subscriptions-clob.polymarket.com"
#     api_key = "019a5744-cda4-7124-b090-5faf15429d33"
#     api_secret = "XWbHqxGrJJBBo_1fECI0kwEdhNUwRQoaD96DX-S7HQs="
#     api_passphrase = "21cc503b60964bcdf961235d7f49718d6550ffc9b12f1cd5fba85b2c0ba04032"

#     asset_id = get_first_clob_id()
#     print("Connecting asset:", asset_id)
#     asset_ids = [asset_id]
#     condition_ids = []

#     auth = {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}

#     market_connection = WebSocketOrderBook(
#         MARKET_CHANNEL, url, asset_ids, auth, None, True
#     )

#     market_connection.run()















# from websocket import WebSocketApp
# import json
# import time
# import threading
# import json
# import time
# from datetime import datetime, timezone
# import requests
# import os
# import pytz


# MARKET_CHANNEL = "market"
# USER_CHANNEL = "user"
# NY_TZ = pytz.timezone("America/New_York")


# class WebSocketOrderBook:
#     def __init__(self, channel_type, url, data, auth, message_callback, verbose):
#         self.channel_type = channel_type
#         self.url = url
#         self.data = data
#         self.auth = auth
#         self.message_callback = message_callback
#         self.verbose = verbose
#         furl = url + "/ws/" + channel_type
#         self.ws = WebSocketApp(
#             furl,
#             on_message=self.on_message,
#             on_error=self.on_error,
#             on_close=self.on_close,
#             on_open=self.on_open,
#         )
#         self.orderbooks = {}

#     def on_message(self, ws, message):
#         print(message)
#         pass

#     def on_error(self, ws, error):
#         print("Error: ", error)
#         exit(1)

#     def on_close(self, ws, close_status_code, close_msg):
#         print("closing")
#         exit(0)

#     def on_open(self, ws):
#         if self.channel_type == MARKET_CHANNEL:
#             ws.send(json.dumps({"assets_ids": self.data, "type": MARKET_CHANNEL}))
#         else:
#             exit(1)

#         thr = threading.Thread(target=self.ping, args=(ws,))
#         thr.start()

#     def ping(self, ws):
#         while True:
#             ws.send("PING")
#             time.sleep(10)

#     def run(self):
#         self.ws.run_forever()




# def get_rounded_timestamps(interval_minutes=15):
#     now_utc = datetime.now(timezone.utc)
#     rounded_minute = (now_utc.minute // interval_minutes) * interval_minutes
#     rounded_utc = now_utc.replace(minute=rounded_minute, second=0, microsecond=0)
#     rounded_et = rounded_utc.astimezone(NY_TZ)
#     utc_unix = int(rounded_utc.timestamp())
#     et_unix = int(rounded_et.timestamp())
#     return {
#         "utc_time": rounded_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
#         "utc_unix": utc_unix,
#         "et_time": rounded_et.strftime("%Y-%m-%d %I:%M:%S %p %Z"),
#         "et_unix": et_unix,
#     }


# def get_market():
#     """Fetch latest event and return Market CLOB info."""
#     times = get_rounded_timestamps()
#     url = f"https://gamma-api.polymarket.com/events/slug/btc-updown-15m-{times['utc_unix']}"
#     resp = requests.get(url, timeout=5)
#     resp.raise_for_status()
#     var_event = resp.json()
#     var_markets = var_event.get("markets", [])
#     var_markets = var_markets[0]
#     var_question = var_markets.get("question","")
#     var_conditionId = var_markets.get("conditionId","")
#     var_clob_token_ids = var_markets.get("clobTokenIds",[])
#     var_clob_token_ids = clob_ids = json.loads(var_clob_token_ids)
#     var_UP_clobTokenId = var_clob_token_ids[0]
#     var_Down_clobTokenId = var_clob_token_ids[1]
    
#     return {
#         "UP_clobTokenId": var_UP_clobTokenId,
#         "Down_clobTokenId": var_Down_clobTokenId,
#         "question": var_question,
#         "conditionId": var_conditionId
#     }



# if __name__ == "__main__":
#     url = "wss://ws-subscriptions-clob.polymarket.com"
#     api_key = "019a5744-cda4-7124-b090-5faf15429d33"
#     api_secret = "XWbHqxGrJJBBo_1fECI0kwEdhNUwRQoaD96DX-S7HQs="
#     api_passphrase = "21cc503b60964bcdf961235d7f49718d6550ffc9b12f1cd5fba85b2c0ba04032"


#     market = get_market()

#     asset_ids = [market['UP_clobTokenId'], market['Down_clobTokenId']]
#     condition_ids = [market['conditionId']]
    
#     auth = {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}

#     market_connection = WebSocketOrderBook(
#         MARKET_CHANNEL, url, asset_ids, auth, None, True
#     )

#     market_connection.run()
























from websocket import WebSocketApp
import json
import time
import threading
from datetime import datetime, timezone
import requests
import pytz


MARKET_CHANNEL = "market"
USER_CHANNEL = "user"
NY_TZ = pytz.timezone("America/New_York")


class WebSocketOrderBook:
    def __init__(self, channel_type, url, data, auth, message_callback=None, verbose=False):
        self.channel_type = channel_type
        self.url = url
        self.data = data
        self.auth = auth
        self.message_callback = message_callback
        self.verbose = verbose

        ws_url = f"{url}/ws/{channel_type}"
        self.ws = WebSocketApp(
            ws_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

    # -------------------------------
    # MESSAGE HANDLER
    # -------------------------------
    def on_message(self, ws, message):
        """Pass WS message to the callback."""
        if self.verbose:
            print("RAW WS MESSAGE:", message)

        try:
            parsed = json.loads(message)
        except:
            parsed = message

        if self.message_callback:
            self.message_callback(parsed)

    # -------------------------------
    # ERROR HANDLER
    # -------------------------------
    def on_error(self, ws, error):
        print("Error:", error)

    # -------------------------------
    # CLOSE HANDLER
    # -------------------------------
    def on_close(self, ws, code, msg):
        print("WebSocket closed")

    # -------------------------------
    # OPEN HANDLER
    # -------------------------------
    def on_open(self, ws):
        if self.channel_type == MARKET_CHANNEL:
            sub_msg = {"assets_ids": self.data, "type": MARKET_CHANNEL}
            ws.send(json.dumps(sub_msg))
        else:
            print("Unsupported channel")
            return

        # Start PING thread
        threading.Thread(target=self._ping, args=(ws,), daemon=True).start()

    # -------------------------------
    # PING LOOP
    # -------------------------------
    def _ping(self, ws):
        while True:
            try:
                ws.send("PING")
                time.sleep(10)
            except:
                break

    # -------------------------------
    # START WS
    # -------------------------------
    def run(self):
        self.ws.run_forever()

def get_rounded_timestamps(interval_minutes=15):
    now_utc = datetime.now(timezone.utc)
    rounded_minute = (now_utc.minute // interval_minutes) * interval_minutes
    rounded_utc = now_utc.replace(minute=rounded_minute, second=0, microsecond=0)
    rounded_et = rounded_utc.astimezone(NY_TZ)

    return {
        "utc_unix": int(rounded_utc.timestamp()),
        "et_unix": int(rounded_et.timestamp()),
    }


def get_market():
    ts = get_rounded_timestamps()
    url = f"https://gamma-api.polymarket.com/events/slug/btc-updown-15m-{ts['utc_unix']}"

    resp = requests.get(url, timeout=5)
    resp.raise_for_status()

    event = resp.json()
    markets = event["markets"][0]

    clob_ids = json.loads(markets["clobTokenIds"])

    return {
        "UP_clobTokenId": clob_ids[0],
        "Down_clobTokenId": clob_ids[1],
        "question": markets.get("question"),
        "conditionId": markets.get("conditionId"),
    }

if __name__ == "__main__":

    market = get_market()
    asset_ids = [market["UP_clobTokenId"], market["Down_clobTokenId"]]

    url = "wss://ws-subscriptions-clob.polymarket.com"


    def handle_orderbook(msg):
        bids = msg.get("bids", [])
        asks = msg.get("asks", [])

        # Helpers to clean numbers
        def to_float(val):
            try:
                return float(val)
            except:
                return None

        best_bid = bids[0] if bids else None
        best_ask = asks[0] if asks else None

        print("\n📘 ORDER BOOK UPDATE")
        #print(f"Market:    {msg.get('market')}")
        #print(f"Asset ID:  {msg.get('asset_id')}")
        print(f"Timestamp: {msg.get('timestamp')}")

        if best_bid:
            print(f"Best Bid:  price={to_float(best_bid['price'])}, size={to_float(best_bid['size'])}")
        else:
            print("Best Bid:  None")

        if best_ask:
            print(f"Best Ask:  price={to_float(best_ask['price'])}, size={to_float(best_ask['size'])}")
        else:
            print("Best Ask:  None")

        print(f"Depth:     {len(bids)} bids, {len(asks)} asks")
    # def handle_price_change(msg):
    #     def to_float(v):
    #         try:
    #             return float(v)
    #         except:
    #             return None

    #     print("\n⚡ PRICE CHANGE EVENT")
    #     #print(f"Market:    {msg.get('market')}")
    #     print(f"Timestamp: {msg.get('timestamp')}")

    #     changes = msg.get("price_changes", [])

    #     for ch in changes:
    #         print("\n  ▸ Asset:", ch.get("asset_id"),
    #         f"Side: ", {ch.get("side")},
    #         f"Price: "     , {to_float(ch.get("price"))},
    #         f"Size: "      , {to_float(ch.get("size"))},
    #         f"Best Bid: "  , {to_float(ch.get("best_bid"))},
    #         f"Best Ask: "  , {to_float(ch.get("best_ask"))},
    #         )#End print
    #         #print("    Hash:       ", ch.get("hash"))
    def handle_price_change(msg, market=market):

        def to_float(v):
            try:
                return float(v)
            except:
                return None

        up_id = market["UP_clobTokenId"]
        down_id = market["Down_clobTokenId"]

        print("\n⚡ PRICE CHANGE EVENT")
        print(f"Market:    {msg.get('market')}")
        print(f"Timestamp: {msg.get('timestamp')}")

        changes = msg.get("price_changes", [])

        for ch in changes:

            asset_id = ch.get("asset_id")

            # Label asset as UP or DOWN
            if asset_id == up_id:
                asset_label = "UP"
            elif asset_id == down_id:
                asset_label = "DOWN"
            else:
                asset_label = asset_id  # fallback

            print(
                f"\n  ▸ Asset: {asset_label}\n"
                f"    Side:       {ch.get('side')}\n"
                f"    Price:      {to_float(ch.get('price'))}\n"
                f"    Size:       {to_float(ch.get('size'))}\n"
                f"    Best Bid:   {to_float(ch.get('best_bid'))}\n"
                f"    Best Ask:   {to_float(ch.get('best_ask'))}"
            )
        
            
            
    def handle_last_trade(msg):

        def to_float(v):
            try:
                return float(v)
            except:
                return None

        print("\n🔥 LAST TRADE PRICE ",
            #f"Market:    {msg.get('market')}",
            #f"Asset ID:  {msg.get('asset_id')}",
            f"Side:      {msg.get('side')}",
            f"Price:     {to_float(msg.get('price'))}",
            f"Size:      {to_float(msg.get('size'))}",
            f"Timestamp: {msg.get('timestamp')}",
            f"Fee bps:   {msg.get('fee_rate_bps')}",
            )#End print
        
    def handle_message(msg):
        if not isinstance(msg, dict):
            print("Unknown message:", msg)
            return

        event_type = msg.get("event_type")

        if event_type == "book" :
            handle_orderbook(msg)

        elif event_type == "price_change" :
            handle_price_change(msg)

        elif event_type == "last_trade_price" :
            handle_last_trade(msg)

        else:
            print("⚠ Unknown event_type:", event_type)
            print(msg)


    market_connection = WebSocketOrderBook(
        MARKET_CHANNEL,
        url,
        asset_ids,
        auth=None,
        message_callback=handle_message,
        verbose=False,
    )

    market_connection.run()
