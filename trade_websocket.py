from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List

client = WebSocketClient(
	api_key="jRphamPSnUm1vH5y109I_kNkgGnUIRTV",
	feed=Feed.RealTime,
	market=Market.Crypto
	)

# trades
# client.subscribe("XT.*")
# client.subscribe("XT.*") # all crypto pair
client.subscribe("XT.BTC-USD")
# client.subscribe("XT.BTC-EUR")
# client.subscribe("XT.ETH-USD")


def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)
