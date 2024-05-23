from sys import prefix
import requests
import pandas as pd
import numpy as np
from pprint import pprint as pp
from morningstar_scraper import get_res_from_screener
from xlwings import view, load
import concurrent.futures
import os.path
import sys
from time import sleep
from playwright.sync_api import sync_playwright

def track_exposure_links(response):
    url = response.url
    if url.startswith("https://www.us-api.morningstar.com"):
        try:
            headers = response.all_headers()
            autho = headers["authorization"]
            authos.append(autho)
        except Exception as e:
            print(f"Error accessing headers: {e}")

def trigger_page(page):
    page.locator("div.sal--wrapper--nav > ul > li").nth(5).click()
    sleep(3)

def process_links(playwright, ticker):
    url = f"https://my.morningstar.com/my/report/fund/portfolio.aspx?t={ticker}&fundservcode=&lang=en-MY"
    chromium = playwright.chromium
    browser = chromium.launch(headless=False)
    context = browser.new_context(user_agent=user_agent)
    page = context.new_page()
    page.on("request", track_exposure_links)  # get the authorization
    page.goto(url)
    trigger_page(page)
    page.close()
    context.close()
    browser.close()

def get_exposure_links(ticker):
    with sync_playwright() as playwright:
        process_links(playwright, ticker)
    return authos

def authentication_scraper():
    global authos
    ticker = "F00001443C"  # 0P00016LLS, F00001443C, F00001443D
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    authos = []  # Global variable to store authorization headers
    exposure_links = get_exposure_links(ticker)
    autho = list(set(exposure_links))[0]
    with open("autho.txt", "w") as wf:
        wf.write(autho)
    return autho

def main():
    global fund_df
    for i in range(page, max_page + 1)[:]:
        res = get_res_from_screener(i, page_size, user_agent)
        print(f"Request to page {i} was successfull")
        info = res['rows']
        info_df = pd.json_normalize(info)
        try:
            info_df = info_df.loc[:, ["SecId", "Name", "PriceCurrency", "CategoryName", "StarRatingM255"]]
        except:
            info_df = info_df.loc[:, ["SecId", "Name", "PriceCurrency", "CategoryName"]]
            info_df.loc[:, "StarRatingM255"] = np.nan
        fund_df = pd.concat([fund_df, info_df], axis=0)

def get_fund_data(ticker: str):
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'authorization': authentication,
    }

    params = {
        'secExchangeList': '',
        'limitAge': '',
        'currency': '',
        'hideYTD': 'false',
        'refresh': 'true',
        'languageId': 'en',
        'locale': 'en',
        'clientId': 'MDC_intl',
        'benchmarkId': 'mstarorcat',
        'component': 'sal-components-mip-growth-10k',
        'version': '3.60.0',
    }

    response = requests.get(
        f'https://www.us-api.morningstar.com/sal/sal-service/fund/performance/v3/{ticker}',
        params=params,
        headers=headers,
    )
    try:
        data = response.json()["graphData"]
    except:
        print("graphData problem")
        return
    return data

def clean_data(ticker):
    global start, end, freq
    global fund_df
    print(f"Now scraping {df[ticker]}...")
    data = get_fund_data(ticker)
    try:
        category = pd.json_normalize(data["category"]).set_index("date").rename(columns={"value": "Benchmark"})
    except KeyError:
        print(f"Failed to scrape {df[ticker]}")
        with open("error.txt", 'a') as wf:
            wf.write(f"{df[ticker]}\n")
        return
    fund = pd.json_normalize(data["fund"]).set_index("date").rename(columns={"value": "Fund"})
    temp_df = pd.concat([fund, category], axis=1)
    temp_df.index = pd.to_datetime(temp_df.index)
    temp_df = temp_df.sort_index().rename(dict(zip(temp_df.columns, [df[ticker], category_name[ticker] ])), axis=1)
    fund_df = pd.concat([fund_df, temp_df], axis=1)
    return fund_df


if __name__ == "__main__":
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    page = 1
    max_page = 40
    page_size = 50

    freqs = {
        "D": 252,
        "M": 12,
        "Q": 252/4,
    }
    risk_free_rate = 0.03
    w1 = 1; w2 = 0
    temp_df = pd.read_excel("/Users/tkyw/Library/Mobile Documents/com~apple~CloudDocs/Documents/work/work Doc/Due Diligence/Fund Reviews/Morningstar Screener.xlsx")
    df = temp_df.set_index("SecId")["Name"].to_dict()
    category_name = temp_df.set_index("SecId")["CategoryName"].to_dict()
    fund_df = pd.DataFrame()
    destination = "testing.csv"
    if os.path.exists(destination):
        scraped_fund = pd.read_csv(destination, index_col=[0], encoding="ISO-8859-1")
    else:
        scraped_fund = pd.DataFrame()
    with open("error.txt") as rf:
        error_data = [item.strip() for item in rf.readlines()]
    scraped_fund_names = scraped_fund.columns
    authentication = authentication_scraper()
    with concurrent.futures.ThreadPoolExecutor(5) as executor:
        for ticker in list(df.keys())[:]:
            if  (df[ticker] not in scraped_fund_names) and (df[ticker] not in error_data):
                # print(clean_data(ticker))
                executor.submit(clean_data, ticker)

    fund_df = pd.concat([fund_df, scraped_fund], axis=1)
    fund_df.to_clipboard(excel=True)
    fund_df.to_csv(destination)
