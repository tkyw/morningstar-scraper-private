import requests
import pandas as pd
import numpy as np
from pprint import pprint as pp
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

    response = requests.get(  ## send request to the hidden api links containing the specific fund detail
        f'https://www.us-api.morningstar.com/sal/sal-service/fund/performance/v3/{ticker}',
        params=params,
        headers=headers,
    )
    try:
        data = response.json()["indexName"] ## conver the response to json format, selecting the key containing the nav of both fund and benchmark
    except:
        print("graphData problem")
        return
    return data

def clean_data(ticker):
    global start, end, freq
    global fund_df
    print(f"Now scraping {df[ticker]}...")
    data = get_fund_data(ticker)
    temp_df = pd.DataFrame({df[ticker] : [data, ticker]}, index=["Benchmark", "ticker"])
    fund_df = pd.concat([fund_df, temp_df], axis=1)
    return fund_df


if __name__ == "__main__":
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    temp_df = pd.read_csv("data/screeners.csv", index_col=0)
    df = temp_df.set_index("SecId", drop=True)["Name"].to_dict()
    category_name = temp_df.set_index("SecId")["CategoryName"].to_dict()
    fund_df = pd.DataFrame() ## a dataframe which have the function of fetching funds' details.
    destination = "data/output.csv"
    if os.path.exists(destination):
        scraped_fund = pd.read_csv(destination, index_col=[0], encoding="ISO-8859-1")
    else:
        scraped_fund = pd.DataFrame()  ## create a file to store all funds scraped
    scraped_fund_names = scraped_fund.columns
    authentication = authentication_scraper() ## scrape the bearer token to bypass the authentication validator
    # with concurrent.futures.ThreadPoolExecutor(5) as executor: ## use 5 threads to allow 5 multithreading scraping process to be executing concurrently.
    for ticker in list(df.keys())[:20]:
        if  (df[ticker] not in scraped_fund_names): ## only send request to the fund that has not in the scraped fund's list and error data list
            # executor.submit(clean_data, ticker)
            clean_data(ticker)
    # fund_df = pd.concat([fund_df, scraped_fund], axis=1)  ## merge the previously scraped fund and new scraped funds
    fund_df.T.to_clipboard(excel=True)
    fund_df.T.to_csv(destination) ## ultimately, store the funds nav into a file caled output.csv, consider changing this based on user preferen
