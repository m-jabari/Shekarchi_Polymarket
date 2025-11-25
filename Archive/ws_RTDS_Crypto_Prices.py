from websocket import WebSocketApp
import json
import time
import threading
from datetime import datetime, timezone
import pytz

NY_TZ = pytz.timezone("America/New_York")


class WebSocketOrderBook:
    WINDOW_MS = 15 * 60 * 1000   # 15 minutes

    def __init__(self, url="wss://ws-live-data.polymarket.com", auth=None, message_callback=None, verbose=False):
        self.url = url
        self.auth = auth
        self.message_callback = message_callback
        self.verbose = verbose
        self.ts_meta = None

        # Persistent base value — FIXED
        self.val_base = None
        self.current_window_start = None

        self.ws = WebSocketApp(
            url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

    # -----------------------------
    # Time helpers
    # -----------------------------
    def get_rounded_timestamps(self, interval_minutes=15):
        """
        Round the current UTC time down to the nearest interval (default 15 minutes)
        and return both UTC and ET timestamps and unix values.
        """
        now_utc = datetime.now(timezone.utc)
        rounded_minute = (now_utc.minute // interval_minutes) * interval_minutes
        rounded_utc = now_utc.replace(minute=rounded_minute, second=0, microsecond=0)
        rounded_et = rounded_utc.astimezone(NY_TZ)

        return {
            "utc_time": rounded_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "utc_unix": int(rounded_utc.timestamp()),
            "et_time": rounded_et.strftime("%Y-%m-%d %I:%M:%S %p %Z"),
            "et_unix": int(rounded_et.timestamp()),
        }

    def ts_to_short(self, ts_ms):
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(NY_TZ)
        return dt.strftime("%b%d %H:%M")

    def ts_to_time(self, ts_ms):
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(NY_TZ)
        return dt.strftime("%H:%M:%S")

    def format_diff_short(self, ms_diff):
        sec = ms_diff // 1000
        return f"{sec // 60:02d}:{sec % 60:02d}"

    # -----------------------------
    # WebSocket Handlers
    # -----------------------------
    def on_message(self, ws, message):
        self.handle_message(message)

    def on_error(self, ws, error):
        print("Error:", error)

    def on_close(self, ws, code, msg):
        print("Connection closed")

    def on_open(self, ws):
        sub = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": "{\"symbol\":\"btc/usd\"}"
                }
            ]
        }
        ws.send(json.dumps(sub))
        print("Connected & subscribed.")

        threading.Thread(target=self.ping, args=(ws,), daemon=True).start()

    def ping(self, ws):
        while True:
            try:
                ws.send("PING")
            except:
                break
            time.sleep(10)

    # -----------------------------
    # Core logic with FIXED base value
    # -----------------------------
    def handle_message(self, msg):
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            return

        payload = data.get("payload", {})
        if "value" not in payload or "timestamp" not in payload:
            return

        symbol = payload.get("symbol", "btc/usd")

        # use class method instead of global function
        self.ts_meta = self.get_rounded_timestamps()

        # Compute new 15m window start (ET)
        window_start_ms = self.ts_meta["et_unix"] * 1000
        ts_ms = payload["timestamp"]
        value = float(payload["value"])
        value_fmt = f"{value:.3f}"

        # If window changed → reset base
        if self.current_window_start != window_start_ms:
            self.current_window_start = window_start_ms
            self.val_base = value  # STORE BASE HERE

        # Calculate delta
        delta_value = round(value - self.val_base, 3)

        # Remaining time
        end_ms = window_start_ms + self.WINDOW_MS
        diff_ms = max(end_ms - ts_ms, 0)

        arrow = "↑" if delta_value > 0 else "↓" if delta_value < 0 else "→"

        # -----------------------------
        # Build return values for the callback
        # -----------------------------
        result = {
            "symbol": symbol,
            "window_start_short": self.ts_to_short(window_start_ms),
            "ts_time": self.ts_to_time(ts_ms),
            "diff_short": self.format_diff_short(diff_ms),
            "val_base_fmt": f"{self.val_base:.3f}",
            "value_fmt": value_fmt,
            "arrow": arrow,
            "delta_value": delta_value,
        }

        # If user provided a callback, send values out
        if self.message_callback is not None:
            self.message_callback(result)
        else:
            # default behavior if no callback is passed
            print(
                f"{result['symbol']} | "
                f"{result['window_start_short']} {result['ts_time']} | "
                f"{result['diff_short']} | {result['val_base_fmt']} | "
                f"{result['value_fmt']} | {result['arrow']} | {result['delta_value']}"
            )

    def run(self):
        self.ws.run_forever()



if __name__ == "__main__":

    def print_order_update(data):
        symbol = data["symbol"]
        window_start_short = data["window_start_short"]
        ts_time = data["ts_time"]
        diff_short = data["diff_short"]
        val_base_fmt = data["val_base_fmt"]
        value_fmt = data["value_fmt"]
        arrow = data["arrow"]
        delta_value = data["delta_value"]

        print(
            f"{symbol} | {window_start_short} {ts_time} | "
            f"{diff_short} | {val_base_fmt} | "
            f"{value_fmt} | {arrow} | {delta_value}"
        )

    ws_client = WebSocketOrderBook(message_callback=print_order_update)
    ws_client.run()


# vscode ➜ /workspaces/ubuntu $  cd /workspaces/ubuntu ; /usr/bin/env /usr/local/python/current/bin/python /home/vscode/.vscode-server/extensions/ms-python.debugpy-2025.16.0/bundled/libs/debugpy/adapter/../../debugpy/launcher 47075 -- /workspaces/ubuntu/Shekarchi_Polymarket/ws_RTDS_Crypto_Prices.py 
# Connected & subscribed.