from pandas.io.formats.style_render import Subset
import requests
from pprint import pprint as pp
import pandas as pd
from requests.utils import select_proxy
from IPython.display import display
from morningstar_scraper import get_res_from_screener
from playwright.sync_api import sync_playwright, Playwright
from time import sleep
import numpy as np
import concurrent.futures
import os.path
import re

class ExposureScraper:
    def __init__(self, ticker, autho):
        self.ticker = ticker
        self.autho = autho
        self.user_agent = user_agent
        self.headers = {
            'authorization': self.autho,
            'user-agent': self.user_agent,
        }

    def send_request(self, link, params, headers=None):
        print("Sending Request to " + link)
        return requests.get(link, params=params, headers=self.headers) if headers is None else requests.get(link, params=params, headers=headers)

    def sector_exposure_scraper(self):
        params = {
            'languageId': 'en',
            'locale': 'en',
            'clientId': 'MDC_intl',
            'benchmarkId': 'mstarorcat',
            'component': 'sal-components-mip-sector-exposure',
            'version': '3.60.0',
        }
        link = f"https://www.us-api.morningstar.com/sal/sal-service/fund/portfolio/v2/sector/{self.ticker}/data"
        res = self.send_request(link, params)
        equity_sector_exp = res.json()["EQUITY"]['fundPortfolio']  #AHAM Absolute Return III
        fixedincome_sector_exp = res.json()["FIXEDINCOME"]['fundPortfolio']
        del equity_sector_exp['portfolioDate']
        del fixedincome_sector_exp['portfolioDate']
        equity_sector_exp = pd.DataFrame(equity_sector_exp, index=["Exposure"])
        fixedincome_sector_exp = pd.DataFrame(fixedincome_sector_exp, index=["Exposure"])
        return pd.concat([equity_sector_exp, fixedincome_sector_exp], axis=1)

    def countries_exposure_scraper(self):
        params = {
            'languageId': 'en',
            'locale': 'en',
            'clientId': 'MDC_intl',
            'benchmarkId': 'mstarorcat',
            'component': 'sal-components-mip-country-exposure',
            'version': '3.60.0',
        }

        link = f'https://www.us-api.morningstar.com/sal/sal-service/fund/portfolio/regionalSectorIncludeCountries/{self.ticker}/data'
        res = self.send_request(link, params)
        countries_exp = res.json()["fundPortfolio"]["countries"]
        df = pd.DataFrame(countries_exp)
        if len(df.columns) == 1:
            df["Exposure"] = np.nan
        return df.set_index("name").T

    def top_holdings_scraper(self, fund):
        params = {
            'premiumNum': '100',
            'freeNum': '25',
            'languageId': 'en',
            'locale': 'en',
            'clientId': 'MDC_intl',
            'benchmarkId': 'mstarorcat',
            'component': 'sal-components-mip-holdings',
            'version': '3.60.0',
        }

        link = f'https://www.us-api.morningstar.com/sal/sal-service/fund/portfolio/holding/v2/{self.ticker}/data'
        res = self.send_request(link, params)
        print("Fund Name = " + fund)
        data = res.json()
        holding_types = ["equityHoldingPage", "otherHoldingPage", "boldHoldingPage"]
        top_holdings = []
        for htype in holding_types:
            top_holdings.extend(data[htype]["holdingList"])
        if top_holdings != []:
            holdings = pd.DataFrame(top_holdings)[['securityName',"weighting"]].set_index("securityName").T
            holdings.rename(lambda fund_name: re.sub(r"\d+\.?\d*%", "", fund_name), axis=1, inplace=True)
            return holdings
        else:
            return

    def liquidity_exposure_scraper(self):
        headers = {
            'authorization': self.autho,
            'user-agent': self.user_agent,
        }

        params = {
            'languageId': 'en',
            'locale': 'en',
            'clientId': 'MDC_intl',
            'benchmarkId': 'mstarorcat',
            'component': 'sal-components-mip-factor-profile',
            'version': '3.60.0',
        }

        link = f'https://www.us-api.morningstar.com/sal/sal-service/fund/factorProfile/{self.ticker}/data'
        res = self.send_request(link, params, headers)
        try:
            liquidity = pd.DataFrame(res.json()["factors"]["liquidity"])[["categoryAvg", "percentile"]].drop_duplicates()
        except:
            liquidity = pd.DataFrame([np.nan] * 2, index=["categoryAvg", "percentile"]).T
        return liquidity

def export_to_excel(scraped_fund, df, file, sheet_name):
    combined_df = scraped_fund.merge(df, left_index=True, right_index=True, how="outer")
    combined_df.to_excel(file, sheet_name=sheet_name)

def read_autho():
    with open("autho.txt", 'r') as rf:
        return rf.readlines()[0]

def combined_data(exposures, fund):
    global df
    try:
        if (name := fund["Name"]) in scraped_fund.columns or (name := fund["Name"]) in df.columns:
            print("Skipping " + name)
            return
        # sector_exposures = exposures.sector_exposure_scraper().reset_index(drop=True).T
        # sector_exposures.columns = [fund["Name"]]
        # countries_exposures = exposures.countries_exposure_scraper().reset_index(drop=True).T
        # countries_exposures.columns = [fund["Name"]]
        top_holdings = exposures.top_holdings_scraper(fund["Name"])
        if top_holdings is not None:
            top_holdings = top_holdings.reset_index(drop=True).T.reset_index()
            top_holdings.columns = ["securityName", fund["Name"]]
            top_holdings = top_holdings.drop_duplicates(subset="securityName", keep='first').set_index("securityName", drop=True)
            top_holdings.rename(str.strip, axis=0, inplace=True)
        else:
            return
        # liquidity = exposures.liquidity_exposure_scraper().reset_index(drop=True).T
        # liquidity.columns = [fund["Name"]]
        # combined_df = pd.concat([sector_exposures, countries_exposures, top_holdings, liquidity], axis=1).T
        # combined_df.columns = [fund["Name"]]
        #
        # print("this is combined")
        # display(combined_df)
        # print("this is df")
        # display(df)
        df = df.merge(top_holdings, left_index=True, right_index=True, how="outer")
        # df = df.merge(combined_df, left_index=True, right_index=True, how="outer").drop_duplicates()
        display(df)
    except Exception as e:
        print(e)
        export_to_excel(scraped_fund, df, file, sheet_name)

def main():
    global df
    df = pd.DataFrame()
    try:
        for i in range(page, max_page + 1)[:]:
            res = get_res_from_screener(i, page_size, user_agent)
            print(f"Request to page {i} was successfull")
            info = res['rows']
            info_df = pd.json_normalize(info)
            info_df = info_df.loc[:, ["SecId", "Name"]]
            autho = read_autho()
            with concurrent.futures.ThreadPoolExecutor(5) as executor:
                for idx in range(info_df.shape[0])[:]:
                    fund = info_df.iloc[idx,:]
                    exposures = ExposureScraper(fund['SecId'], autho)
                    # combined_data(exposures, fund)
                    combined_df = executor.submit(combined_data, exposures, fund)
        export_to_excel(scraped_fund, df, file, sheet_name)
    except KeyboardInterrupt:
        export_to_excel(scraped_fund, df, file, sheet_name)

if __name__ == "__main__":
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    page = 1
    max_page = 40
    page_size = 50
    file = "top holdings.xlsx"
    sheet_name = "top holdings"
    if os.path.exists(file):
        scraped_fund = pd.read_excel(file, index_col=0, sheet_name=sheet_name)
    else:
        scraped_fund = pd.DataFrame()
    main()
