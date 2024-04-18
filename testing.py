import requests
from pprint import pprint as pp
import pandas as pd
from IPython.display import display


if __name__ == "__main__":
    url = f"https://my.morningstar.com/my/report/fund/portfolio.aspx?t={}&fundservcode=&lang=en-MY"

# "https://www.us-api.morningstar.com/sal/sal-service/fund/portfolio/regionalSectorIncludeCountries/F00000W8R0/data?languageId=en&locale=en&clientId=MDC_intl&benchmarkId=mstarorcat&component=sal-components-mip-country-exposure&version=3.60.0"

# holdings = pd.DataFrame(response.json()["otherHoldingPage"]["holdingList"])
# holdings = holdings[['securityName',"weighting"]]




# countries_exp = response.json()["fundPortfolio"]["countries"]  # countries regionalSectorIncludeCountries
# df = pd.DataFrame(countries_exp)
# display(df)

# region_exp = response.json()["fundPortfolio"]  # no real time  regionalSector
# del region_exp["masterPortfolioId"]
# del region_exp["portfolioDate"]

# sector_exp = response.json()["EQUITY"]['fundPortfolio']  # real time
# del sector_exp['portfolioDate']

# pp(response.json()["factors"]["liquidity"])  # liquidity  real time
# countries and region https://www.us-api.morningstar.com/sal/sal-service/fund/portfolio/regionalSectorIncludeCountries/F0000182RP/data?languageId=en&locale=en&clientId=MDC_intl&benchmarkId=mstarorcat&component=sal-components-mip-country-exposure&version=3.60.0
# sector https://www.us-api.morningstar.com/sal/sal-service/fund/portfolio/v2/sector/F0000182RP/data?languageId=en&locale=en&clientId=MDC_intl&benchmarkId=mstarorcat&component=sal-components-mip-sector-exposure&version=3.60.0
# top holdings https://www.us-api.morningstar.com/sal/sal-service/fund/portfolio/holding/v2/F0000182RP/data?premiumNum=100&freeNum=25&languageId=en&locale=en&clientId=MDC_intl&benchmarkId=mstarorcat&component=sal-components-mip-holdings&version=3.60.0
