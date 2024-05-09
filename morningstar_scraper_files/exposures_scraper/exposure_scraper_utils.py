import pandas as pd
import requests
import re
import numpy as np

class ExposureScraper:
    def __init__(self, ticker, autho, user_agent):
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
