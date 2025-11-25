from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List

# Initialize WebSocket client
client = WebSocketClient(
    api_key="WexyVbDEKTlHO_yCVtN6XnxY8C9SxXjU",
    feed=Feed.RealTime,
    market=Market.Crypto
)

# Subscribe to two different channels for the same ticker:
# XQ = Quotes (bid/ask), XT = Trades (executed trades)
client.subscribe("XQ.BTC-USD")   # Quote updates
client.subscribe("XT.BTC-USD")   # Trade updates

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        # Print clean, separated output for clarity
        if hasattr(m, "ev"):
            event_type = m.ev
        else:
            event_type = "Unknown"

        print("=" * 60)
        print(f"📡 EVENT TYPE: {event_type}")
        print(m)
        print("=" * 60)

# Run the WebSocket client and handle messages
client.run(handle_msg)
