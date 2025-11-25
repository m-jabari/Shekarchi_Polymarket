from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List

client = WebSocketClient(
	api_key="WexyVbDEKTlHO_yCVtN6XnxY8C9SxXjU", 
	feed=Feed.RealTime,
	market=Market.Crypto
	)

# aggregates (per minute)
client.subscribe("XA.*")
# client.subscribe("XA.*")  # all crypto pair
# client.subscribe("XA.BTC-USD")
# client.subscribe("XA.BTC-EUR")
# client.subscribe("XA.ETH-USD")

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)
