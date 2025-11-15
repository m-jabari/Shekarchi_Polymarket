from websocket import WebSocketApp
import json
import time
import threading
from datetime import datetime, timezone
import pytz

NY_TZ = pytz.timezone("America/New_York")


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


def format_diff(ms_diff):
    minutes = ms_diff // 60000
    seconds = (ms_diff % 60000) // 1000
    millis = ms_diff % 1000
    return f"{minutes:02d}:{seconds:02d}:{millis:03d}"


class WebSocketOrderBook:
    WINDOW_MS = 15 * 60 * 1000   # 15 minutes

    def __init__(self, url, auth=None, message_callback=None, verbose=False):
        self.url = url
        self.auth = auth
        self.message_callback = message_callback
        self.verbose = verbose
        self.ts_meta = None

        self.ws = WebSocketApp(
            url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

    # ------------------------------
    def on_message(self, ws, message):
        self.handle_message(message)

    def on_error(self, ws, error):
        print("Error:", error)
        exit(1)

    def on_close(self, ws, code, msg):
        print("Connection closed")
        exit(0)

    def on_open(self, ws):
        payload = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": "{\"symbol\":\"btc/usd\"}"
                }
            ]
        }

        ws.send(json.dumps(payload))
        print("Connected & subscribed.")

        threading.Thread(target=self.ping, args=(ws,), daemon=True).start()

    # ------------------------------
    def ping(self, ws):
        while True:
            try:
                ws.send("PING")
            except:
                print("Ping failed.")
                break
            time.sleep(10)

    # ------------------------------
    def ts_to_human(self, ts_ms):
        utc_dt = datetime.utcfromtimestamp(ts_ms / 1000).replace(tzinfo=pytz.utc)
        et_dt = utc_dt.astimezone(NY_TZ)
        return et_dt.strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------
    def handle_message(self, msg):
        try:
            data = json.loads(msg)
        except:
            print("Invalid JSON")
            return

        self.ts_meta = get_rounded_timestamps()

        topic = data.get("topic", "unknown")
        payload = data.get("payload", {})
        symbol = payload.get("symbol", "btc/usd")

        base_unix_ms = self.ts_meta["et_unix"] * 1000
        end_ms = base_unix_ms + self.WINDOW_MS   # 15-minute window end

        # Case 1: array of items
        if "data" in payload:
            for item in payload["data"]:
                ts_ms = item.get("timestamp")
                val = float(item.get("value"))
                val_fmt = f"{val:.3f}"

                # Countdown (not count up)
                diff_ms = end_ms - ts_ms
                if diff_ms < 0:
                    diff_ms = 0

                diff_fmt = format_diff(diff_ms)

                print(f"{topic} {symbol} | {val_fmt} | "
                      f"{self.ts_to_human(base_unix_ms)} | {self.ts_to_human(ts_ms)} | {diff_fmt}")

        # Case 2: single update
        elif "value" in payload and "timestamp" in payload:
            ts_ms = payload["timestamp"]
            val = float(payload["value"])
            val_fmt = f"{val:.3f}"

            diff_ms = end_ms - ts_ms
            if diff_ms < 0:
                diff_ms = 0

            diff_fmt = format_diff(diff_ms)

            print(f"{topic} {symbol} | {val_fmt} | "
                  f"{self.ts_to_human(base_unix_ms)} | {self.ts_to_human(ts_ms)} | {diff_fmt}")

    # ------------------------------
    def run(self):
        self.ws.run_forever()


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    url = "wss://ws-live-data.polymarket.com"

    ws_client = WebSocketOrderBook(url)
    ws_client.run()
