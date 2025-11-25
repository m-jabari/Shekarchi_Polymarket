from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List

client = WebSocketClient(
	api_key="WexyVbDEKTlHO_yCVtN6XnxY8C9SxXjU",
	feed=Feed.Business,
	market=Market.Crypto
	)

# FMV
client.subscribe("FMV.*")
# client.subscribe("FMV.*")  # all crypto pair
# client.subscribe("FMV.BTC-EUR")
# client.subscribe("FMV.ETH-USD")

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

client.run(handle_msg)
