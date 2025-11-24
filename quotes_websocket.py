from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List

client = WebSocketClient(
	api_key="WexyVbDEKTlHO_yCVtN6XnxY8C9SxXjU",
	feed=Feed.RealTime,
	market=Market.Crypto
	)

# quotes
#client.subscribe("XQ.*")
# client.subscribe("XQ.*") # all crypto pair
client.subscribe("XQ.BTC-USD")
client.subscribe("XT.BTC-USD")
# client.subscribe("XQ.BTC-EUR")
# client.subscribe("XQ.ETH-USD")


def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)