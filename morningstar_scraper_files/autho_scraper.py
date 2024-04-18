from concurrent.futures.process import _sendback_result
from os import sendfile
import requests
from pprint import pprint as pp
import pandas as pd
from IPython.display import display
from morningstar_scraper import get_res_from_screener
from playwright.sync_api import sync_playwright, Playwright
from time import sleep

def track_exposure_links(response, browser):
    global authos
    url = response.url
    if url.startswith("https://www.us-api.morningstar.com"):
        headers = response.all_headers()
        try:
            autho = headers["authorization"]
            print(autho)
            authos.append(autho)
        except:
            pass


def trigger_page(page):
    page.locator("div.sal--wrapper--nav > ul > li").nth(5).click()
    sleep(5)
    # page.get_by_role("button", name="I’m an individual investor").click()
    # page.get_by_role("button", name="Country").click()

def process_links(playwright: Playwright, ticker):
    global authos
    url = f"https://my.morningstar.com/my/report/fund/portfolio.aspx?t={ticker}&fundservcode=&lang=en-MY"
    chromium = playwright.chromium
    browser = chromium.launch(headless=False)
    context = browser.new_context(user_agent=user_agent)
    page = context.new_page()
    authos = []
    page.on("request", lambda response: track_exposure_links(response, browser))
    page.goto(url)
    trigger_page(page)
    browser.close()
    return authos

def get_exposure_links(ticker):
    with sync_playwright() as p:
        exposure_links = process_links(p, ticker)
    return exposure_links

def main():
    autho = list(set(get_exposure_links(ticker)))[0]
    with open("autho.txt", "w") as wf:
        wf.write(autho)


if __name__ == "__main__":
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    page = 1
    max_page = 40
    page_size = 50
    ticker="F00001443C" # 0P00016LLS, F00001443C, F00001443D
    main()
