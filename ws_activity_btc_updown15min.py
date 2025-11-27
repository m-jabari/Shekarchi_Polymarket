#!/usr/bin/env python3
import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from queue import Queue
import threading
import websockets
import pytz

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

NY_TZ = pytz.timezone("America/New_York")

class UltraFastOrderBookWS:


    def __init__(
        self,
        url: str = "wss://ws-live-data.polymarket.com",
        interval_minutes: int = 15,
        message_callback=None,

    ):
        self.url = url
        #self.interval_minutes = interval_minutes
        self.ts = self.get_rounded_timestamps()
        self.message_callback = message_callback

        self.out_queue: Queue = Queue()

        threading.Thread(target=self._consumer_loop, daemon=True).start()

    def get_rounded_timestamps(self,interval_minutes=15):
        now_utc = datetime.now(timezone.utc)
        rounded_minute = (now_utc.minute // interval_minutes) * interval_minutes
        rounded_utc = now_utc.replace(minute=rounded_minute, second=0, microsecond=0)
        rounded_et = rounded_utc.astimezone(NY_TZ)

        return {
            "utc_unix": int(rounded_utc.timestamp()),
            "et_unix": int(rounded_et.timestamp()),
        }

        
    def _consumer_loop(self):
        while True:
            item = self.out_queue.get()
            if self.message_callback:
                self.message_callback(item)
            else:
                print(item)

    async def _subscribe(self, ws):
        
        sub_msg = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "activity",
                    "type": "orders_matched",
                    "filters": "{\"event_slug\":\"btc-updown-15m-"f"{self.ts['utc_unix']}""\"}"
                }
                # ,{
                #     "topic": "crypto_prices_chainlink",
                #     "type": "update",
                #     "filters": "{\"symbol\":\"btc/usd\"}"
                # }
            ]
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

                        topic = data.get("topic")
                        payload = data.get("payload")
                        if not payload:
                            continue

                        # ------------------------------
                        # Route messages by topic
                        # ------------------------------

                        # # 1. BTC Price Feed
                        # if topic == "crypto_prices_chainlink":
                        #     if "value" in payload and "timestamp" in payload:
                        #         self._process_price(payload)
                        #     continue

                        # 2. Activity Feed (orders matched)
                        if topic == "activity":
                            self.out_queue.put({
                                "topic": "activity",
                                "payload": payload
                            })
                            continue

            except Exception as e:
                print(f"[WS ERROR] {e!r}")
                await asyncio.sleep(1.0)

    def run(self):
        asyncio.run(self._run_forever())


# -----------------------------
# Pretty print callback
# -----------------------------
# def pretty_print(msg: dict):
#     print(">>>", msg)
def pretty_print(msg: dict):
    # if msg.get("topic") == "crypto_prices_chainlink":
    #     p = msg.get("payload", {})
    #     print(p)
    if msg.get("topic") == "activity":
        #return  # ignore anything else

        p = msg.get("payload", {})
        #print(p)
        # Extract clean fields
        asset        = p.get("asset")
        condition_id = p.get("conditionId")
        slug         = p.get("slug") or p.get("eventSlug")
        outcome      = p.get("outcome")
        side         = p.get("side")
        username     = p.get("name")
        pseudonym    = p.get("pseudonym")
        size         = p.get("size")
        price        = p.get("price")
        timestamp    = p.get("timestamp")
        proxy_wallet = p.get("proxyWallet")

        # Print in aligned table style
        print(
            f"\n"
            f" topic         | activity\n"
            f" asset         | {asset}\n"
            f" condition_id  | {condition_id}\n"
            f" slug          | {slug}\n"
            f" outcome       | {outcome}\n"
            f" side          | {side}\n"
            f" username      | {username}\n"
            f" pseudonym     | {pseudonym}\n"
            f" size          | {size}\n"
            f" price         | {price}\n"
            f" tradeValue    | {size * price}\n"
            f" timestamp     | {timestamp}\n"
            f" proxy_wallet  | {proxy_wallet}\n"
        )



if __name__ == "__main__":
    client = UltraFastOrderBookWS(message_callback=pretty_print)
    client.run()