import requests
from pprint import pprint as pp
import pandas as pd
from IPython.display import display
from utils import export_to_excel, get_res_from_screener, read_autho
import autho_scraper
from playwright.sync_api import sync_playwright, Playwright
import concurrent.futures
from exposure_scraper_utils import ExposureScraper
from time import sleep
import numpy as np
import os.path
import re

def combined_data(exposures, fund):
    global df
    try:
        if (name := fund["Name"]) in scraped_fund.columns or (name := fund["Name"]) in df.columns:
            print("Skipping " + name)
            return
        countries_exposures = exposures.countries_exposure_scraper().reset_index(drop=True).T
        countries_exposures.columns = [fund["Name"]]
        df = df.merge(countries_exposures, left_index=True, right_index=True, how="outer")
        display(df)
    except Exception as e:
        print(e)
        export_to_excel(scraped_fund, df, file, sheet_name)
def scrape_funds(page, max_page, page_size, user_agent, scraped_fund, file, sheet_name):
    global df
    try:
        for i in range(page, max_page + 1)[:]:
            res = get_res_from_screener(i, page_size, user_agent)
            print(f"Request to page {i} was successfull")
            info = res['rows']
            info_df = pd.json_normalize(info)
            info_df = info_df.loc[:, ["SecId", "Name"]]
            autho = read_autho()
            with concurrent.futures.ThreadPoolExecutor(2) as executor:
                for idx in range(info_df.shape[0])[:]:
                    fund = info_df.iloc[idx,:]
                    exposures = ExposureScraper(fund['SecId'], autho, user_agent)
                    combined_df = executor.submit(combined_data, exposures, fund)
        export_to_excel(scraped_fund, df, file, sheet_name)
    except KeyboardInterrupt:
        export_to_excel(scraped_fund, df, file, sheet_name)


if __name__ == "__main__":
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    page = 1
    max_page = 1
    page_size = 50
    file = "countries_exposures.xlsx"
    sheet_name = "countries_exposures"
    if os.path.exists(file):
        scraped_fund = pd.read_excel(file, index_col=0, sheet_name=sheet_name)
    else:
        scraped_fund = pd.DataFrame()
    df = pd.DataFrame()
    autho_scraper.main()
    scrape_funds(page, max_page, page_size, user_agent, scraped_fund, file, sheet_name)
