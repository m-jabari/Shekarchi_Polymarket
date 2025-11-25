## WORKING CODE for 2:30 Duration
from websocket import WebSocketApp
import json
import time
import threading
import sys
import pytz
import requests
import os
from datetime import datetime, timezone
from collections import deque
import pyarrow as pa
import pyarrow.parquet as pq

# === Constants ===
MARKET_CHANNEL = "market"
USER_CHANNEL = "user"
NY_TZ = pytz.timezone("America/New_York")

# === Globals for writer ===
buffer = deque()
lock = threading.Lock()
writer = [None]
current_outfile = [None]
current_asset_id = [None]
running = True

# === Parquet Writer Utilities ===
def get_outfile_name(asset_id):
    et_time = datetime.now(timezone.utc).astimezone(NY_TZ)
    ts = et_time.strftime("%Y-%m-%d_%H%M")
    return f"polymarket_BTC_UpDown_15min_{ts}_{asset_id}.parquet"

def start_writer(asset_id):
    """Initialize a new ParquetWriter when asset_id changes."""
    outfile = get_outfile_name(asset_id)
    schema = pa.schema([
        ("timestamp", pa.string()),
        ("raw", pa.string())
    ])
    writer[0] = pq.ParquetWriter(outfile, schema)
    current_outfile[0] = outfile
    current_asset_id[0] = asset_id
    print(f"[Writer] Started new file: {outfile}")

def rotate_file_if_needed(new_asset_id):
    """Rotate Parquet file when asset_id changes."""
    with lock:
        if current_asset_id[0] != new_asset_id:
            if writer[0]:
                writer[0].close()
                print(f"[Writer] Closed file for asset {current_asset_id[0]}")
            start_writer(new_asset_id)
            buffer.clear()

def dump_to_parquet():
    """Continuously flush buffered messages into Parquet every few seconds."""
    while running:
        time.sleep(2)
        with lock:
            if not buffer or writer[0] is None:
                continue
            batch = list(buffer)
            buffer.clear()
        try:
            table = pa.Table.from_pylist(batch)
            writer[0].write_table(table)
        except Exception as e:
            print(f"[Writer Error] {e}")

def stop_writer():
    """Gracefully stop the writer thread."""
    global running
    running = False
    with lock:
        if writer[0]:
            writer[0].close()
            print(f"[Writer] Closed on shutdown")

def append_to_buffer(message):
    """Fast in-memory buffer append (non-blocking)."""
    ts = datetime.now(timezone.utc).isoformat()
    with lock:
        buffer.append({"timestamp": ts, "raw": message})

# === WebSocket Callbacks ===
def on_message(ws, message):
    append_to_buffer(message)
    print(message)

def on_error(ws, error):
    print("Error:", error)
    os._exit(1)

def on_close(ws, close_status_code, close_msg):
    print("Closing connection.")
    os._exit(0)

def ping(ws):
    while True:
        try:
            ws.send("PING")
            time.sleep(10)
        except Exception:
            break

def on_open(ws, channel_type, data, auth):
    if channel_type == MARKET_CHANNEL:
        payload = {"assets_ids": data, "type": MARKET_CHANNEL}
    elif channel_type == USER_CHANNEL and auth:
        payload = {"markets": data, "type": USER_CHANNEL, "auth": auth}
    else:
        print("Invalid channel configuration.")
        os._exit(1)
    ws.send(json.dumps(payload))
    threading.Thread(target=ping, args=(ws,), daemon=True).start()

def run_websocket(channel_type, url, data, auth, verbose=True):
    furl = f"{url}/ws/{channel_type}"
    rotate_file_if_needed(data[0])  # rotate if new asset
    ws = WebSocketApp(
        furl,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=lambda ws: on_open(ws, channel_type, data, auth),
    )
    ws.run_forever()

# === Asset Monitoring ===
def get_rounded_timestamps(interval_minutes=15):
    now_utc = datetime.now(timezone.utc)
    rounded_minute = (now_utc.minute // interval_minutes) * interval_minutes
    rounded_utc = now_utc.replace(minute=rounded_minute, second=0, microsecond=0)
    rounded_et = rounded_utc.astimezone(NY_TZ)
    utc_unix = int(rounded_utc.timestamp())
    et_unix = int(rounded_et.timestamp())
    return {"utc_unix": utc_unix, "et_unix": et_unix}

def get_first_clob_id():
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

def monitor_asset_id(current_id, url, auth):
    """Watch for asset changes and restart WebSocket when needed."""
    while True:
        try:
            new_id = get_first_clob_id()
            if new_id != current_id[0]:
                print(f"[Monitor] Asset changed: {current_id[0]} → {new_id}")
                current_id[0] = new_id
                rotate_file_if_needed(new_id)
                threading.Thread(
                    target=run_websocket,
                    args=(MARKET_CHANNEL, url, [new_id], auth, True),
                    daemon=True,
                ).start()
            time.sleep(1)
        except Exception as e:
            print("[Monitor Error]", e)
            time.sleep(3)

# === Main Execution ===
if __name__ == "__main__":
    url = "wss://ws-subscriptions-clob.polymarket.com"

    auth = {
        "apiKey": "019a5744-cda4-7124-b090-5faf15429d33",
        "secret": "XWbHqxGrJJBBo_1fECI0kwEdhNUwRQoaD96DX-S7HQs=",
        "passphrase": "21cc503b60964bcdf961235d7f49718d6550ffc9b12f1cd5fba85b2c0ba04032",
    }

    first_id = get_first_clob_id()
    print("[Init] Initial asset:", first_id)
    asset_id_ref = [first_id]

    start_writer(first_id)
    threading.Thread(target=dump_to_parquet, daemon=True).start()
    threading.Thread(target=monitor_asset_id, args=(asset_id_ref, url, auth), daemon=True).start()

    run_websocket(MARKET_CHANNEL, url, [first_id], auth, verbose=True)




    




# from websocket import WebSocketApp
# import json
# import time
# from datetime import datetime, timezone
# import threading
# import sys
# import pytz
# import requests
# import os

# MARKET_CHANNEL = "market"
# USER_CHANNEL = "user"
# NY_TZ = pytz.timezone("America/New_York")

# def on_message(ws, message):
#     print(message)

# def on_error(ws, error):
#     print("Error:", error)
#     os._exit(1)

# def on_close(ws, close_status_code, close_msg):
#     print("Closing connection.")
#     os._exit(0)

# def ping(ws):
#     while True:
#         try:
#             ws.send("PING")
#             time.sleep(10)
#         except Exception:
#             break

# def on_open(ws, channel_type, data, auth):
#     if channel_type == MARKET_CHANNEL:
#         payload = {"assets_ids": data, "type": MARKET_CHANNEL}
#     elif channel_type == USER_CHANNEL and auth:
#         payload = {"markets": data, "type": USER_CHANNEL, "auth": auth}
#     else:
#         print("Invalid channel configuration.")
#         os._exit(1)

#     ws.send(json.dumps(payload))
#     threading.Thread(target=ping, args=(ws,), daemon=True).start()

# def run_websocket(channel_type, url, data, auth, verbose=True):
#     furl = f"{url}/ws/{channel_type}"
#     ws = WebSocketApp(
#         furl,
#         on_message=on_message,
#         on_error=on_error,
#         on_close=on_close,
#         on_open=lambda ws: on_open(ws, channel_type, data, auth),
#     )
#     ws.run_forever()

# def get_rounded_timestamps(interval_minutes=15):
#     now_utc = datetime.now(timezone.utc)
#     rounded_minute = (now_utc.minute // interval_minutes) * interval_minutes
#     rounded_utc = now_utc.replace(minute=rounded_minute, second=0, microsecond=0)
#     rounded_et = rounded_utc.astimezone(NY_TZ)
#     utc_unix = int(rounded_utc.timestamp())
#     et_unix = int(rounded_et.timestamp())
#     return {
#         "utc_unix": utc_unix,
#         "et_unix": et_unix,
#     }

# def get_first_clob_id():
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

# def monitor_asset_id(current_id, url, auth):
#     while True:
#         try:
#             new_id = get_first_clob_id()
#             if new_id != current_id[0]:
#                 print(f"Asset changed: {current_id[0]} → {new_id}")
#                 current_id[0] = new_id
#                 threading.Thread(
#                     target=run_websocket,
#                     args=(MARKET_CHANNEL, url, [new_id], auth, True),
#                     daemon=True,
#                 ).start()
#             time.sleep(1)
#         except Exception as e:
#             print("Monitor error:", e)
#             time.sleep(3)

# if __name__ == "__main__":
#     url = "wss://ws-subscriptions-clob.polymarket.com"

#     auth = {
#         "apiKey": "019a5744-cda4-7124-b090-5faf15429d33",
#         "secret": "XWbHqxGrJJBBo_1fECI0kwEdhNUwRQoaD96DX-S7HQs=",
#         "passphrase": "21cc503b60964bcdf961235d7f49718d6550ffc9b12f1cd5fba85b2c0ba04032",
#     }

#     first_id = get_first_clob_id()
#     print("Initial asset:", first_id)
#     asset_id_ref = [first_id]

#     threading.Thread(target=monitor_asset_id, args=(asset_id_ref, url, auth), daemon=True).start()
#     run_websocket(MARKET_CHANNEL, url, [first_id], auth, verbose=True)
