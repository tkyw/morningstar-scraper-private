# import pandas as pd
# from IPython.display import display

# df = pd.read_csv("Malaysia Fund Universe.csv", index_col=0)
# display(df.sort_index())
import pandas as pd
import requests

# df1 = pd.read_excel("Malaysia Fund Universe (Updated).xlsx", index_col=0)
# df2 = pd.read_csv("Fund Alpha.csv", index_col=0)
# df = df1.merge(df2).set_index("Name", drop=True)
# df.to_excel("Malaysia Fund Universe (Updated) Alpha.xlsx")
ticker  = "F00001443D"
end_date = "20240113"
start_date =  "19000101"
link = f"https://quotespeed.morningstar.com/ra/snChartData?instid=MSSAL&sdkver=2.56.0&productType=quikr&cdt=2&country=MYS&ed={end_date}&f=d&fields=HS793&hasPreviousClose=true&ipoDates=20110519&pids=0P0001ICDS&sd={start_date}&tickers={ticker}&qs_wsid=744C2A4741043A926710025C14AF39DC&_=1713269251320"
res = requests.get(link)
print(res.json())
