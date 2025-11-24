#!/usr/bin/env python3
import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from queue import Queue
import threading
import websockets

# -----------------------------
# Fast JSON loader (orjson fallback)
# -----------------------------
try:
    import orjson as json_lib

    def fast_json_loads(s):
        return json_lib.loads(s)
except ImportError:
    import json as json_lib

    def fast_json_loads(s):
        return json_lib.loads(s)


NY_TZ = ZoneInfo("America/New_York")


class UltraFastOrderBookWS:
    """
    Ultra-fast websocket client for Polymarket Chainlink BTC/USD price feed.
    - Minimal work in hot path
    - Async websocket
    - Background consumer for prints / downstream processing
    """

    WINDOW_MS = 15 * 60 * 1000  # 15 minutes

    def __init__(
        self,
        url: str = "wss://ws-live-data.polymarket.com",
        symbol: str = "btc/usd",
        interval_minutes: int = 15,
        message_callback=None,
    ):
        self.url = url
        self.symbol = symbol
        self.interval_minutes = interval_minutes

        self.message_callback = message_callback

        # State for current 15m window
        self.current_window_start_ms = None
        self.val_base = None

        # Timezone
        self.tz_et = NY_TZ

        # Output queue so WS loop never blocks
        self.out_queue: Queue = Queue()

        # Start consumer thread
        threading.Thread(target=self._consumer_loop, daemon=True).start()

    # -----------------------------
    # Background consumer
    # -----------------------------
    def _consumer_loop(self):
        while True:
            item = self.out_queue.get()
            if self.message_callback:
                self.message_callback(item)
            else:
                print(
                    f"{item['symbol']} | "
                    f"{item['ts']} | "
                    f"{item['remain']} | "
                    f"{item['base']:.3f} → {item['value']:.3f} "
                    f"{item['arrow']} Δ {item['delta']:.3f}"
                )

    # -----------------------------
    # Helpers
    # -----------------------------
    def _format_mmss(self, ms_diff: int) -> str:
        sec = ms_diff // 1000
        return f"{sec // 60:02d}:{sec % 60:02d}"

    # -----------------------------
    # Core message processing
    # -----------------------------
    def _process_payload(self, payload: dict):
        ts_ms = payload["timestamp"]
        value = float(payload["value"])

        # Compute ET time
        dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        dt_et = dt_utc.astimezone(self.tz_et)

        # Rounded window
        rounded_minute = (dt_et.minute // self.interval_minutes) * self.interval_minutes
        start_et = dt_et.replace(minute=rounded_minute, second=0, microsecond=0)
        window_start_ms = int(start_et.timestamp() * 1000)

        # Human readable: "Nov30 11:30"
        window_start_human = start_et.strftime("%b%d %H:%M")

        # New window
        if self.current_window_start_ms != window_start_ms or self.val_base is None:
            self.current_window_start_ms = window_start_ms
            self.val_base = value

        delta = value - self.val_base

        # Remaining time
        end_ms = window_start_ms + self.WINDOW_MS
        remain_ms = max(end_ms - ts_ms, 0)

        arrow = "↑" if delta > 0 else "↓" if delta < 0 else "→"

        result = {
            "symbol": payload.get("symbol", self.symbol),
            "ts": f"{dt_et.hour:02d}:{dt_et.minute:02d}:{dt_et.second:02d}",
            "remain": self._format_mmss(remain_ms),
            "value": value,
            "base": self.val_base,
            "delta": round(delta, 3),
            "arrow": arrow,
            "window_start_ms": window_start_ms,
            "window_start_human": window_start_human,
        }

        self.out_queue.put(result)

    # -----------------------------
    # WebSocket subscribe
    # -----------------------------
    async def _subscribe(self, ws):
        sub_msg = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": '{"symbol":"btc/usd"}',
                }
            ],
        }
        await ws.send(
            json_lib.dumps(sub_msg).decode()
            if hasattr(json_lib, "dumps")
            else json_lib.dumps(sub_msg)
        )

    # -----------------------------
    # WebSocket main loop
    # -----------------------------
    async def _run_forever(self):
        while True:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=10,
                    ping_timeout=5,
                    max_queue=None,
                ) as ws:
                    await self._subscribe(ws)

                    async for msg in ws:
                        try:
                            data = fast_json_loads(msg)
                        except Exception:
                            continue

                        payload = data.get("payload")
                        if not payload:
                            continue

                        if "value" not in payload or "timestamp" not in payload:
                            continue

                        self._process_payload(payload)

            except Exception as e:
                print(f"[WS ERROR] {e!r}")
                await asyncio.sleep(1.0)

    def run(self):
        asyncio.run(self._run_forever())


def pretty_print(msg: dict):
    print(
        f"{msg['symbol']} | {msg['window_start_human']} | "
        f"{msg['ts']} | {msg['remain']} | "
        f"{msg['base']:.3f} | {msg['value']:.3f} | "
        f"{msg['arrow']} | {msg['delta']:.3f}"
    )


if __name__ == "__main__":
    client = UltraFastOrderBookWS(message_callback=pretty_print)
    client.run()