from websocket import WebSocketApp
import json
import time
import threading
from datetime import datetime, timezone
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import os
import pytz

MARKET_CHANNEL = "market"
USER_CHANNEL = "user"
NY_TZ = pytz.timezone("America/New_York")


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

        # Thread-safe message buffer
        self.buffer = []
        self.lock = threading.Lock()

        # Output parquet path (ET accurate with DST)
        et_time = datetime.now(timezone.utc).astimezone(NY_TZ)
        ts = et_time.strftime("%Y%m%d_%H%M")
        self.outfile = f"polymarket_BTC_UpDown_15min_{ts}.parquet"

        # Start background writer thread
        writer_thread = threading.Thread(target=self._flush_loop, daemon=True)
        writer_thread.start()
    
    
    def _flush_loop(self):
        """Periodically flush buffered messages to a single valid Parquet file."""
        schema = pa.schema([("timestamp", pa.string()), ("raw", pa.string())])
        writer = None

        try:
            while True:
                time.sleep(2)
                with self.lock:
                    if not self.buffer:
                        continue
                    batch = self.buffer
                    self.buffer = []

                table = pa.Table.from_pylist(batch, schema=schema)

                # Create the writer once
                if writer is None:
                    writer = pq.ParquetWriter(self.outfile, schema)

                writer.write_table(table)

                if self.verbose:
                    print(f"[{datetime.now(timezone.utc).isoformat()}] Flushed {len(batch)} rows → {self.outfile}")

        finally:
            # Ensure footer is written even if you Ctrl+C
            if writer is not None:
                writer.close()
                print(f"✅ Closed ParquetWriter: {self.outfile}")        

    def dump_message(self, message):
        """
        Collect messages in memory and flush to Parquet in append mode.
        Real append — no overwrite.
        """
        try:
            et_time = datetime.now(timezone.utc).astimezone(NY_TZ)
            ts = et_time.strftime("%Y%m%d_%H%M")
            self.outfile = f"polymarket_BTC_UpDown_15min_{ts}.parquet"
    
            # Initialize structures
            if not hasattr(self, "_buffer"):
                self._buffer = []
                self._buffer_lock = threading.Lock()
    
            # Append to in-memory buffer
            with self._buffer_lock:
                self._buffer.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "raw": message
                })
    
            # Flush every 100 messages (tune as needed)
            if len(self._buffer) >= 50:
                self._flush_to_parquet()
    
        except Exception as e:
            print(f"[Error dumping message] {e}")
    
    
    def _flush_to_parquet(self):
        """
        Internal helper to flush the buffer to disk, appending to existing parquet.
        """
        try:
            with self._buffer_lock:
                if not self._buffer:
                    return
                data = self._buffer
                self._buffer = []
    
            table = pa.Table.from_pylist(data)
    
            if not os.path.exists(self.outfile):
                pq.write_table(table, self.outfile, compression="snappy")
            else:
                # True append using pandas bridge (safe incremental append)
                import pandas as pd
                from fastparquet import write as fp_write, ParquetFile
    
                df = pd.DataFrame(data)
                fp_write(self.outfile, df, compression="SNAPPY", append=True)
    
        except Exception as e:
            print(f"[Error flushing parquet] {e}")



    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            self.dump_message(message)
        except:
            print("Invalid JSON:", message)
            return
    
        # CASE 1 — It's a list, skip or handle differently
        if isinstance(data, list):
            # Optional: print for debugging
            # print("List message:", data)
            return
    
        # CASE 2 — It's a dict but not a price_change event
        if "event_type" not in data:
            return
    
        if data["event_type"] != "price_change":
            return
            

        # CASE 3 — Valid price_changes message
        for p in data.get("price_changes", []):
            if p.get("side") != "BUY":
                continue
            # if p.get("side") == UP_ID:
            #     print(f"UP best_ask: {best_ask}")
            # elif p.get("side") == DOWN_ID:
            #     print(f"DOWN best_ask: {best_ask}")
            asset_id = p.get("asset_id")
            side = p.get("side")
            price = p.get("price")
            size = p.get("size")
            best_bid = p.get("best_bid")
            best_ask = p.get("best_ask")
            timestamp = str(p.get('ts'))
            #print(f"side: {side} | price: {price} | size: {size} | best_bid: {best_bid} | best_ask: {best_ask} | {timestamp}")
            print(f"best_ask: {best_ask} | {asset_id}")



            
        # # CASE 3 — Valid price_changes message
        # for p in data.get("tick_size_change", []):
    
        #     print(f"market: {msg.get('market')}")# | old_tick_size: {msg['tick_size']['old']} | new_tick_size: {msg['tick_size']['new']} | {str(msg.get('ts')}")



    def on_error(self, ws, error):
        print("Error:", error)
        exit(1)

    def on_close(self, ws, close_status_code, close_msg):
        print("Connection closed")
        if writer is not None:
            writer.close()
        exit(0)

    def on_open(self, ws):
        """Subscribe to channel."""
        if self.channel_type == MARKET_CHANNEL:
            ws.send(json.dumps({"assets_ids": self.data, "type": MARKET_CHANNEL}))
        elif self.channel_type == USER_CHANNEL and self.auth:
            ws.send(json.dumps({"markets": self.data, "type": USER_CHANNEL, "auth": self.auth}))
        else:
            print("Invalid channel or missing auth")
            exit(1)

        # Start keepalive ping
        thr = threading.Thread(target=self.ping, args=(ws,), daemon=True)
        thr.start()

    def ping(self, ws):
        while True:
            try:
                ws.send("PING")
                time.sleep(10)
            except Exception as e:
                print("Ping failed:", e)
                break

    def run(self):
        self.ws.run_forever()
    
    # def close_writer(self):
    #     """Ensure the writer is properly closed on shutdown."""
    #     if hasattr(self, "_writer"):
    #         with self._parquet_lock:
    #             self._writer.close()
    def close_writer(self):
        self._flush_to_parquet()

def get_rounded_timestamps(interval_minutes=15):
    now_utc = datetime.now(timezone.utc)
    rounded_minute = (now_utc.minute // interval_minutes) * interval_minutes
    rounded_utc = now_utc.replace(minute=rounded_minute, second=0, microsecond=0)
    rounded_et = rounded_utc.astimezone(NY_TZ)
    utc_unix = int(rounded_utc.timestamp())
    et_unix = int(rounded_et.timestamp())
    return {
        "utc_time": rounded_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "utc_unix": utc_unix,
        "et_time": rounded_et.strftime("%Y-%m-%d %I:%M:%S %p %Z"),
        "et_unix": et_unix,
    }


def get_first_clob_id():
    """Fetch latest event and return first CLOB token id."""
    times = get_rounded_timestamps()
    url = f"https://gamma-api.polymarket.com/events/slug/btc-updown-15m-{times['utc_unix']}"
    resp = requests.get(url, timeout=5)
    resp.raise_for_status()
    event = resp.json()
    markets = event.get("markets", [])
    if not markets:
        raise RuntimeError("No markets in event response")
    clob_token_ids = markets[0].get("clobTokenIds")
    if isinstance(clob_token_ids, str):
        first = json.loads(clob_token_ids)[0]
    elif isinstance(clob_token_ids, list):
        first = clob_token_ids[0]
    else:
        raise RuntimeError("Unexpected clobTokenIds type")
    return first


if __name__ == "__main__":
    url = "wss://ws-subscriptions-clob.polymarket.com"
    api_key = "019a5744-cda4-7124-b090-5faf15429d33"
    api_secret = "XWbHqxGrJJBBo_1fECI0kwEdhNUwRQoaD96DX-S7HQs="
    api_passphrase = "21cc503b60964bcdf961235d7f49718d6550ffc9b12f1cd5fba85b2c0ba04032"

    asset_id = get_first_clob_id()
    print("Connecting asset:", asset_id)
    asset_ids = [asset_id]
    condition_ids = []

    auth = {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}

    market_connection = WebSocketOrderBook(
        MARKET_CHANNEL, url, asset_ids, auth, None, True
    )

    market_connection.run()
