import os
import time
import threading
import pandas as pd
from datetime import datetime, timedelta, UTC
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market

quotes_buffer = []
buffer_lock = threading.Lock()

# --- Polygon client ---
client = WebSocketClient(
    api_key="FvmC9qDPdIhehYoiJeX6DPoZTlEswpKV",
    feed=Feed.RealTime,
    market=Market.Crypto
)
client.subscribe("XQ.*")

# --- Handler ---
def handle_msg(msgs):
    local_buf = []
    for m in msgs:
        if m.event_type == "XQ":
            local_buf.append({
                "pair": m.pair,
                "bid_price": m.bid_price,
                "bid_size": m.bid_size,
                "ask_price": m.ask_price,
                "ask_size": m.ask_size,
                "timestamp": m.timestamp,
                "exchange_id": m.exchange_id,
                "received_timestamp": m.received_timestamp
            })
    with buffer_lock:
        quotes_buffer.extend(local_buf)

# --- WebSocket thread ---
def websocket_thread():
    try:
        client.run(handle_msg)
    except Exception as e:
        print(f"⚠️ WebSocket thread error: {e}")

# --- Stop function ---
def stop_websocket():
    try:
        client.close()
    except Exception as e:
        print(f"⚠️ Error closing WebSocket: {e}")

# --- Capture one minute ---
def capture_minute(start_time):
    global quotes_buffer
    quotes_buffer = []
    end_time = start_time + timedelta(minutes=1)

    t = threading.Thread(target=websocket_thread, daemon=True)
    t.start()

    print(f"🚀 Capturing {start_time.strftime('%H:%M:%S')} → {end_time.strftime('%H:%M:%S')} UTC")
    time.sleep((end_time - datetime.now(UTC)).total_seconds())

    stop_websocket()
    t.join(timeout=10)

    with buffer_lock:
        safe_copy = list(quotes_buffer)

    if not safe_copy:
        print("⚠️ No data captured.")
        return

    df = pd.DataFrame.from_records(safe_copy)

    # --- New path structure ---
    date_str = start_time.strftime("%Y-%m-%d")
    save_dir = os.path.join("data", "quote", date_str)
    os.makedirs(save_dir, exist_ok=True)

    filename = f"crypto_quotes_{start_time.strftime('%Y-%m-%d_%H-%M')}.parquet"
    filepath = os.path.join(save_dir, filename)

    df.to_parquet(filepath, index=False)
    print(f"💾 Saved {len(df)} records → {filepath}")

# --- Continuous loop ---
def run_continuous():
    now = datetime.now(UTC)
    next_start = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    wait = (next_start - now).total_seconds()
    print(f"⏳ Waiting {wait:.1f}s until {next_start.strftime('%H:%M:%S')} UTC...")
    time.sleep(wait)

    while True:
        capture_minute(next_start)
        next_start += timedelta(minutes=1)
        delay = (next_start - datetime.now(UTC)).total_seconds()
        if delay > 0:
            print(f"⏳ Waiting {delay:.1f}s until {next_start.strftime('%H:%M:%S')} UTC...")
            time.sleep(delay)

if __name__ == "__main__":
    run_continuous()
