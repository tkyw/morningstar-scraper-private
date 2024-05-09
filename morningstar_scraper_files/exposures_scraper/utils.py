import pandas as pd
import requests



def export_to_excel(scraped_fund, df, file, sheet_name):
    combined_df = scraped_fund.merge(df, left_index=True, right_index=True, how="outer")
    combined_df.to_excel(file, sheet_name=sheet_name)

def read_autho():
    with open("autho.txt", 'r') as rf:
        return rf.readlines()[0]

def get_res_from_screener(page, page_size, user_agent):
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
