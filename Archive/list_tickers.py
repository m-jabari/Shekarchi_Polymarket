from polygon import RESTClient

client = RESTClient("WexyVbDEKTlHO_yCVtN6XnxY8C9SxXjU")

tickers = []
for t in client.list_tickers(
	market="stocks",
	active="true",
	order="asc",
	limit="1000",
	sort="ticker",
	):
    tickers.append(t)

print(tickers)
