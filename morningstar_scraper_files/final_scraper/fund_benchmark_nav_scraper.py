import requests
import pandas as pd
import numpy as np
import concurrent.futures
from playwright.sync_api import sync_playwright
from time import sleep

def trigger_page(df_row):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(f"https://my.morningstar.com/my/report/fund/performance.aspx?t={df_row['ticker']}&fundservcode=&lang=en-MY")
        page.get_by_role("button", name="I’m an individual investor").click()
        page.locator("sal-components-chart-iframe div").nth(3).click()
        page.get_by_placeholder("Add Comparison").click()
        sleep(1)
        page.keyboard.type(df_row["Benchmark"])
        page.get_by_placeholder("Add Comparison").click()
        page.get_by_role("cell", name=df_row["Benchmark"], exact=True).click()
        page.pause()
        browser.close()

# if __name__ == "__main__":
#     df = pd.read_csv("data/output.csv", index_col=0)
#     print(df)
#     trigger_page(df.iloc[0])


# categoryid = MYCA000022
# indexid = F00000T609

headers = {
    'authority': 'quotespeed.morningstar.com',
    'accept': '*/*',
    'accept-language': 'en-GB,en;q=0.9',
    'origin': 'https://my.morningstar.com',
    'referer': 'https://my.morningstar.com/my/report/fund/performance.aspx?t=0P0001ICDR&fundservcode=&lang=en-MY',
    'sec-ch-ua': '"Not)A;Brand";v="24", "Chromium";v="116"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
}

params = {
    'instid': 'MSSAL',
    'sdkver': '2.56.0',
    'productType': 'quikr',
    'cdt': '7',
    'ed': '20240530',
    'f': 'd',
    'hasPreviousClose': 'true',
    'ipoDates': '20141222',
    'pids': '0P00013EL3',
    'sd': '1900101',
    'tickers': '28.01.F00000T609',
    'qs_wsid': '0A2DFF0D60934C12DD9B38E26535388B',
    '_': '1717499858682',
}

if __name__ == "__main__":
    res = requests.get('https://quotespeed.morningstar.com/ra/uniqueChartData', params=params, headers=headers)

    df  = pd.json_normalize(res.json()["Data"][0]["DailyData"]["LastPrice"])[["Date", "Last"]].set_index("Date", drop=True).astype(float)
    print(df.pct_change())
