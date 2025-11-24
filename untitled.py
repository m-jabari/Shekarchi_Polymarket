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

    # def _flush_loop(self):
    #     """Periodically flush buffered messages to Parquet file."""
    #     schema = pa.schema([("timestamp", pa.string()), ("raw", pa.string())])

    #     while True:
    #         time.sleep(2)  # flush interval (seconds)
    #         with self.lock:
    #             if not self.buffer:
    #                 continue
    #             batch = self.buffer
    #             self.buffer = []

    #         # Convert to Arrow Table
    #         table = pa.Table.from_pylist(batch, schema=schema)

    #         # Append to Parquet file
    #         if os.path.exists(self.outfile):
    #             pq.write_to_dataset(table, root_path=self.outfile + "_dataset")
    #         else:
    #             pq.write_table(table, self.outfile)

    #         if self.verbose:
    #             print(f"[{datetime.now(timezone.utc).isoformat()}] Flushed {len(batch)} rows to {self.outfile}")
    def _flush_loop(self):
        """Periodically flush buffered messages to a single Parquet file."""
        schema = pa.schema([("timestamp", pa.string()), ("raw", pa.string())])
        writer = None
    
        while True:
            time.sleep(2)
            with self.lock:
                if not self.buffer:
                    continue
                batch = self.buffer
                self.buffer = []
    
            table = pa.Table.from_pylist(batch, schema=schema)
    
            if writer is None:
                writer = pq.ParquetWriter(self.outfile, schema)
    
            writer.write_table(table)
    
            if self.verbose:
                print(f"[{datetime.now(timezone.utc).isoformat()}] Flushed {len(batch)} rows → {self.outfile}")

    def on_message(self, ws, message):
        """Handle incoming message and buffer it."""
        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "raw": message,
        }
        with self.lock:
            self.buffer.append(data)
        if self.verbose:
            #print(f"Captured message at {data['timestamp']}")
            print(message)
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
