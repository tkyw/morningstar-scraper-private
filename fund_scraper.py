import requests
import pandas as pd 
def getSafe(obj, keywords):
    item = obj.get(keywords)
    try:
        item = item['value']
    except:
        return None
    return item

cookies = {
    'msession': 'c9c27212-0410-4265-87d4-1d182e05df5d.7b05c09df3e20c2072401c21b3a1fc9d0d132e40d9b1ceb427452e18f2a3719d',
    'optimizelyEndUserId': 'oeu1709987255202r0.5727933400783585',
    'OptanonConsent': 'isGpcEnabled=0&datestamp=Mon+Mar+11+2024+09%3A37%3A42+GMT%2B0800+(Malaysia+Time)&version=202306.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=MY%3B10',
    'OptanonAlertBoxClosed': '2024-03-11T01:37:42.550Z',
    'mbuddy': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXSyIsImtpZCI6IjE2NjYwMjExMjkifQ.eyJzZXNzaW9uIjoiYzljMjcyMTItMDQxMC00MjY1LTg3ZDQtMWQxODJlMDVkZjVkLjdiMDVjMDlkZjNlMjBjMjA3MjQwMWMyMWIzYTFmYzlkMGQxMzJlNDBkOWIxY2ViNDI3NDUyZTE4ZjJhMzcxOWQiLCJzdWIiOiI0OTdDQkE3MC1GQkIzLTQ4NjktQTEwQS1DOTA2MzMwQkVEMkIiLCJhdHRyaWJ1dGVzIjp7fSwic2NvcGVzIjpbXSwiaWF0IjoxNzEwMjIwNDA3LCJpc3MiOiJodHRwczovL2ludmVzdG9yLm1vcm5pbmdzdGFyLmNvbSIsImV4cCI6MTcxMDIyMDcwN30.FlFvC23Zx0MG0H-HeigPWZ01rI18zsVBX0apJ0cmmM15gwqXiKccUckVm3c07gn6Ev-stgdtkirkjYoYC6SOXoIhUcFSh02qIqn69BKhSeq9S9v56NW074U6iwCyqbtw6NU98OrbSRJJMoEjs_i1eoXUxuh2a9FreLVt0oDAsjp2kP4rKc8-8nQ5tSJrhrTIMwykr6uKQ8uvEBPVsn2oulOfgBXbHbTM_VOEx7vl7wtgXzmlaO7c59UADhqOz95vUfJXYE9bvFwldi-y3G13Coy-XzXAPs37xk4LUtWLFFIQ1v3gX01W2w2So-xY8TCfyCx8k3b9cEuTo735PxJKvg',
    'mstar': 'V6O4P5N7MPP4061218P9M1PL1O8K5OLL9L1111K0611POO1P156M03NPL45NP905L5P510N4L0559N0PKP594LKO713N43867217596N59KM77ONO5NO30L85O2545N9NOA34FEA7BB03ADC1C663AF6D01713D547',
}

headers = {
    'authority': 'www.morningstar.com',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    # 'cookie': 'msession=c9c27212-0410-4265-87d4-1d182e05df5d.7b05c09df3e20c2072401c21b3a1fc9d0d132e40d9b1ceb427452e18f2a3719d; optimizelyEndUserId=oeu1709987255202r0.5727933400783585; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Mar+11+2024+09%3A37%3A42+GMT%2B0800+(Malaysia+Time)&version=202306.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=MY%3B10; OptanonAlertBoxClosed=2024-03-11T01:37:42.550Z; mbuddy=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXSyIsImtpZCI6IjE2NjYwMjExMjkifQ.eyJzZXNzaW9uIjoiYzljMjcyMTItMDQxMC00MjY1LTg3ZDQtMWQxODJlMDVkZjVkLjdiMDVjMDlkZjNlMjBjMjA3MjQwMWMyMWIzYTFmYzlkMGQxMzJlNDBkOWIxY2ViNDI3NDUyZTE4ZjJhMzcxOWQiLCJzdWIiOiI0OTdDQkE3MC1GQkIzLTQ4NjktQTEwQS1DOTA2MzMwQkVEMkIiLCJhdHRyaWJ1dGVzIjp7fSwic2NvcGVzIjpbXSwiaWF0IjoxNzEwMjIwNDA3LCJpc3MiOiJodHRwczovL2ludmVzdG9yLm1vcm5pbmdzdGFyLmNvbSIsImV4cCI6MTcxMDIyMDcwN30.FlFvC23Zx0MG0H-HeigPWZ01rI18zsVBX0apJ0cmmM15gwqXiKccUckVm3c07gn6Ev-stgdtkirkjYoYC6SOXoIhUcFSh02qIqn69BKhSeq9S9v56NW074U6iwCyqbtw6NU98OrbSRJJMoEjs_i1eoXUxuh2a9FreLVt0oDAsjp2kP4rKc8-8nQ5tSJrhrTIMwykr6uKQ8uvEBPVsn2oulOfgBXbHbTM_VOEx7vl7wtgXzmlaO7c59UADhqOz95vUfJXYE9bvFwldi-y3G13Coy-XzXAPs37xk4LUtWLFFIQ1v3gX01W2w2So-xY8TCfyCx8k3b9cEuTo735PxJKvg; mstar=V6O4P5N7MPP4061218P9M1PL1O8K5OLL9L1111K0611POO1P156M03NPL45NP905L5P510N4L0559N0PKP594LKO713N43867217596N59KM77ONO5NO30L85O2545N9NOA34FEA7BB03ADC1C663AF6D01713D547',
    'dnt': '1',
    'referer': 'https://www.morningstar.com/us-equity-funds?page=9',
    'sec-ch-ua': '"Not(A:Brand";v="24", "Chromium";v="122"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
}

page = 1
limit = 50
pages = 27
data = []

def get_funds():
    for page in range(1, pages+1):
        print("Now scraping page {}".format(page))
        response = requests.get(
            f'https://www.morningstar.com/api/v2/navigation-list/us-equity-funds?sort=fundSize:desc&page={page}&limit={limit}',
            cookies=cookies,
            headers=headers,
        )
        for fund in response.json()['results']:
            meta = fund['meta']
            fields = fund['fields']
            info = {
                "secID": meta['securityID'],
                "fund_name": meta['ticker'],
                "expense_ratio": getSafe(fields, 'adjustedExpenseRatio'),
                "AUM": getSafe(fields, 'fundSize'),
                "initial_investment": getSafe(fields, 'minimumInitialInvestment'),
                "category": getSafe(fields,'morningstarCategory'),
                "return_rank_3y":  getSafe(fields,'returnRankCategory[3y]'),
                "return_rank_5y": getSafe(fields, 'returnRankCategory[5y]'),
                "return_rank_10y": getSafe(fields, 'returnRankCategory[10y]'),        
            }
            data.append(info)
    df = pd.DataFrame(data)
    return df