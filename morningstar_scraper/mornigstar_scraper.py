import pandas as pd
from pandas.core.arrays.timedeltas import periods_per_second
from pandas.io.formats.format import Final
import requests
import pprint
import numpy as np
from IPython.display import display
from playwright.sync_api import sync_playwright, Playwright
import re
import concurrent.futures
import os.path


class Financial_metrics_calculation:

    YTD_RISK_FREE_RATE =0.0067
    ONE_Y_RISK_FREE_RATE = 0.028
    THREE_Y_RISK_FREE_RATE = 0.0734
    FIVE_Y_RISK_FREE_RATE = 0.1276
    TEN_Y_RISK_FREE_RATE = 0.3228

    RISK_FREE_RATES = pd.DataFrame([YTD_RISK_FREE_RATE, ONE_Y_RISK_FREE_RATE, THREE_Y_RISK_FREE_RATE, FIVE_Y_RISK_FREE_RATE, TEN_Y_RISK_FREE_RATE], index=["YTD Risk Free Rate", "1Y Risk Free Rate", "3Y Risk Free Rate", "5Y Risk Free Rate", "10Y Risk Free Rate"]).T

    def __init__(self, df):
        self.df = df
        self.month_end_data = self.df.resample("M").last()
        self.years = self.df.index.year.unique()

    def compute_returns(self, return_df):
        return (return_df.iloc[-1, :] / return_df.iloc[0, :]) - 1

    def annual_return(self):
        return_df = pd.DataFrame()
        for year in self.years:
            return_df[f"{year} Return"] = self.compute_returns(self.df.loc[str(year), :])
        return return_df

    def fund_returns(self):
        return_df = pd.DataFrame(columns=["YTD Return", "1Y Return", "3Y Return", "5Y Return", "10Y Return"])
        return_df["YTD Return"] = self.compute_returns(self.df.loc["2024", :])
        years = [1, 3, 5, 10]
        for year in years:
            end_date = self.df.index[-1].date()
            start_date = end_date - pd.DateOffset(years=year)
            earliest_date = self.df.index[0].date()
            if start_date.date() >= earliest_date:
                period_df = self.df.loc[str( start_date ): str(end_date)]
                return_df[f"{year}Y Return"] = self.compute_returns(period_df)
            else:
                return_df[f"{year}Y Return"] = np.nan
        return return_df

    def compute_std(self, period_df):
        return_df = period_df.pct_change()
        return return_df.std(ddof=1)

    def annual_std(self):
        std_df = pd.DataFrame()
        for year in self.years:
            std_df[f"{year} Std"] = self.compute_std(self.df.loc[str(year), :])
        return std_df

    def fund_std(self):
        std_df = pd.DataFrame(columns=["YTD Std", "1Y Std", "3Y Std", "5Y Std", "10Y Std"])
        std_df["YTD Std"] = self.compute_std(self.df.loc["2024", :])
        years = [1, 3, 5, 10]
        for year in years:
            end_date = self.df.index[-1].date()
            start_date = end_date - pd.DateOffset(years=year)
            earliest_date = self.df.index[0].date()
            if start_date.date() >= earliest_date:
                period_df = self.df.loc[str( start_date ): str(end_date)]
                std_df[f"{year}Y Std"] = self.compute_std(period_df)
            else:
                std_df[f"{year}Y Std"] = np.nan
        return std_df

    def annualized_return(self, return_df):
        target_return = return_df.iloc[:5, :]
        temp_df = pd.concat([target_return, pd.Series([self.df.index[-1].month/12, 1, 3, 5, 10], index=target_return.index)], axis=1)
        annualized_df = (1 + temp_df.iloc[:, 0]) ** (1/temp_df.iloc[:, -1]) - 1
        return_df.iloc[:5, :] = annualized_df.to_frame()
        return return_df

    def annuazlied_std(self, std_df):
        return std_df * np.sqrt(252)

    def fund_sharpe(self):
        fund_returns = self.annualized_return( pd.concat([self.fund_returns(), self.annual_return()], axis=1).T)
        fund_stds = self.annuazlied_std( pd.concat([self.fund_std(), self.annual_std()], axis=1).T )
        fund_risk_free_rates = pd.DataFrame(np.full_like(fund_returns, Financial_metrics_calculation.ONE_Y_RISK_FREE_RATE), index=fund_returns.index)
        sharpe_df = pd.concat([fund_returns.reset_index(drop=True), fund_risk_free_rates.reset_index(drop=True), fund_stds.reset_index(drop=True)], axis=1, ignore_index=True)
        sharpe_df = sharpe_df.rename(columns = dict(zip(sharpe_df.columns, ["Returns", "Risk Free Rate", "Std"])))
        sharpe_df.index = ["YTD Sharpe", "1Y Sharpe", "3Y Sharpe", "5Y Sharpe", "10Y Sharpe"] + [f"{year} Sharpe" for year in self.years]
        sharpe_ratio_df = ((sharpe_df['Returns'] - sharpe_df['Risk Free Rate']) / sharpe_df['Std']).to_frame()
        sharpe_ratio_df.columns = ["Last"]
        return sharpe_ratio_df

    def max_drawdown(self):
        max_nav = self.df.cummax()
        diff_nav_max = self.df - max_nav
        drawdown = diff_nav_max / max_nav
        max_drawdown = drawdown.min()
        # print(max_drawdown)
        return max_drawdown

def get_res_from_screener(page, page_size):
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'dnt': '1',
        'origin': 'https://my.morningstar.com',
        'referer': 'https://my.morningstar.com/my/screener/fund.aspx',
        'sec-ch-ua': '"Chromium";v="123", "Not:A-Brand";v="8"',
        'sec-ch-ua-mobile': '?0',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': user_agent,
    }
    params = {
        'page': str(page),
        'pageSize': str(page_size),
        'sortOrder': 'LegalName asc',
        'outputType': 'json',
        'version': '1',
        'languageId': 'en-MY',
        'currencyId': 'MYD',
        'universeIds': 'FOMYS$$ALL',
        'securityDataPoints': 'SecId|Name|PriceCurrency|TenforeId|LegalName|ClosePrice|Yield_M12|CategoryName|Medalist_RatingNumber|StarRatingM255|SustainabilityRank|GBRReturnD1|GBRReturnW1|GBRReturnM1|GBRReturnM3|GBRReturnM6|GBRReturnM0|GBRReturnM12|GBRReturnM36|GBRReturnM60|GBRReturnM120|MaxFrontEndLoad|OngoingCharge|ManagerTenure|MaxDeferredLoad|InitialPurchase|FundTNAV|EquityStyleBox|BondStyleBox|AverageMarketCapital|AverageCreditQualityCode|EffectiveDuration|MorningstarRiskM255|AlphaM36|BetaM36|R2M36|StandardDeviationM36|SharpeM36|TrackRecordExtension',
        'filters': '',
        'term': '',
        'subUniverseId': '',
    }

    response = requests.get(
        'https://tools.morningstar.co.uk/api/rest.svc/v0gfft3bk4/security/screener',
        params=params,
        headers=headers,
    )
    return response.json()

def get_fund_data(res):
    fund_data = res["Data"][0]['DailyData']
    keys = list(fund_data.keys())[0]
    fund_nav =pd.json_normalize(fund_data[keys]).loc[:, ["Date", "Last"]]
    fund_nav.index = pd.to_datetime(fund_nav.Date)
    fund_nav.drop("Date", axis=1, inplace=True)
    return fund_nav

def data_cleaning(df):
    return df.astype(float)

def get_res_data(response):
    global results
    try:
        url = response.url
        if "snChartData?" in url:
            results.append(response.url)
    except:
        pass


def trigger_page(page, i):
    if i == True:
        page.get_by_role("button", name="I’m an individual investor").click()
    page.locator("sal-components-chart-iframe div").nth(3).click()
    page.get_by_role("button", name="MAX").click()


def get_api_link(playwright: Playwright, ticker):
    global results
    print(ticker)
    chromium = playwright.chromium
    browser = chromium.launch(headless=False)
    page = browser.new_page()
    # Subscribe to "request" and "response" events.
    results = []
    page.on("response", lambda response: get_res_data(response))
    page.goto(f"https://my.morningstar.com/my/report/fund/performance.aspx?t={ticker}&fundservcode=&lang=en-MY")
    page_loaded = False
    pop_up = True
    while page_loaded == False:
        try:
            trigger_page(page, pop_up)
            page_loaded = True
        except Exception as e:
            print(e)
            print("Failed to trigger page before timeout")
            print("Reloading...")
            page.reload()
            pop_up = False
    return results[0]
    browser.close()

def automate_api(ticker):
    with sync_playwright() as p:
        api_link = get_api_link(p, ticker)
        return api_link

def get_nav(link):
    headers = {
        'user-agent': user_agent
    }

    response = requests.get(link, headers=headers).json()
    return response


def skip_scraped_fund(scraped_fund, fund_name):
    list_of_scraped_funds = scraped_fund.columns
    return True if fund_name in list_of_scraped_funds else False

def export_to_csv(scraped_fund, df, file):
    combined_df = pd.concat([scraped_fund, df], axis=1)
    combined_df.to_csv(file)

def export_error_fund(fund_name):
    with open("error_fund.txt", "a") as wf:
        wf.write(fund_name + "\n")

def create_link(ticker, start_date, end_date, api_link):
    patterns = [r"(?<=ed=)\d+", r"(?<=sd=)\d+", "(?<=tickers=)\w+"]
    for pattern, item in zip(patterns, [end_date, start_date, ticker]):
        api_link = re.sub(pattern, item, api_link)
    return api_link

def compute_fund_info(scraped_fund , fund, start_date, end_date, api_link):
    global df
    try:
        print("Now Requesting to {}".format(fund['SecId']))
        if skip_scraped_fund(scraped_fund, fund["Name"]) == True:
            print(f"{fund['Name']} has been scraped")
            print("Skipping...")
            return
        fund_res = get_nav(create_link(fund['SecId'], start_date, end_date, api_link))
        temp_df = data_cleaning(get_fund_data(fund_res))
        # display(temp_df)
        temp_df.to_csv("testing.csv")
        # ===================================
        financial_metrics = Financial_metrics_calculation(temp_df)
        returns = pd.concat([financial_metrics.fund_returns(), financial_metrics.annual_return()], axis=1)
        sharpe_ratios = financial_metrics.fund_sharpe().T
        max_drawdown = financial_metrics.max_drawdown()
        combined_df = pd.concat([returns, sharpe_ratios], axis=1)
        combined_df[["Max Drawdown", "PriceCurrency", "StarRatingM255", "CategoryName" ]] = np.hstack([max_drawdown,fund.loc[["PriceCurrency", "StarRatingM255", "CategoryName"]].values])
        combined_df = combined_df.T; combined_df.columns = [fund['Name']]
        df = pd.concat([df, combined_df], axis=1)
        pprint.pprint(df)
        return False
    except Exception as e:
        export_error_fund(fund["Name"])
        print("Exception Trigger")
        print(e)
        return True

def test_api_link_state(sample_fund):
    if not os.path.exists(api_link_file):
        with open(api_link_file, 'w') as wf:
            pass
    with open(api_link_file, 'r') as rf:
        api_link = rf.readlines()
    state = compute_fund_info(scraped_fund, sample_fund, start_date, end_date, api_link[0]) if api_link != [] else "No API Link Found"
    return state, api_link[0]

def main(scraped_fund: pd.DataFrame, file: str, start_date, end_date):
    global df
    try:
        for i in range(page, max_page + 1)[:]:
            res = get_res_from_screener(i, page_size)
            print(f"Request to page {i} was successfull")
            info = res['rows']
            info_df = pd.json_normalize(info)
            try:
                info_df = info_df.loc[:, ["SecId", "Name", "PriceCurrency", "CategoryName", "StarRatingM255"]]
            except:
                info_df = info_df.loc[:, ["SecId", "Name", "PriceCurrency", "CategoryName"]]
                info_df.loc[:, "StarRatingM255"] = np.nan

            sample_fund = info_df.iloc[0]
            api_expired, api_link = test_api_link_state(sample_fund)
            if api_expired == True or api_expired == "No API Link Found": # automate scraping of API link
                error_text = "API Link Expired" if api_expired == True else api_expired
                print(error_text)
                print("Getting new API Link...")
                try:
                    api_link = automate_api(sample_fund["SecId"])
                except:
                    print("Encounter an error in getting the api link")
                finally:
                    print("Successfully get new API Link!")
                with open(api_link_file, "w") as wf:
                    wf.write(api_link)

            with concurrent.futures.ThreadPoolExecutor(4) as executor:
                for idx in range(info_df.shape[0])[:]:
                    fund = info_df.iloc[idx,:]
                    results = executor.submit(compute_fund_info, scraped_fund, fund, start_date, end_date, api_link)

        export_to_csv(scraped_fund, df, file)
    except KeyboardInterrupt:
        export_to_csv(scraped_fund, df, file)


if __name__ == "__main__":
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    page = 1
    max_page = 40
    page_size = 50

    df = pd.DataFrame()

    start_date = "19000101"
    end_date = "20240329"
    file = "Malaysia Fund Universe (Automated).csv"
    api_link_file ="api_link.txt"
    if os.path.exists(file):
        scraped_fund = pd.read_csv(file, index_col=0)
    else:
        scraped_fund = pd.DataFrame()
    main(scraped_fund, file, start_date, end_date)
