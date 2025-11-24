from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List

client = WebSocketClient(
	api_key="WexyVbDEKTlHO_yCVtN6XnxY8C9SxXjU",
	feed=Feed.RealTime,
	market=Market.Crypto
	)

# aggregates (per second)
client.subscribe("XAS.*")
# client.subscribe("XAS.*") # all crypto pair
# client.subscribe("XAS.BTC-USD")
# client.subscribe("XAS.BTC-EUR")
# client.subscribe("XAS.ETH-USD")

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)
