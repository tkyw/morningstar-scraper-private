import requests
import pandas as pd

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1EY3hOemRHTnpGRFJrSTRPRGswTmtaRU1FSkdOekl5TXpORFJrUTROemd6TWtOR016bEdOdyJ9.eyJodHRwczovL21vcm5pbmdzdGFyLmNvbS9tc3Rhcl9pZCI6Ijc2NjU2NkFELTkxMjEtNDJDMS05RjM2LTkwREM1RkNENUUxQyIsImh0dHBzOi8vbW9ybmluZ3N0YXIuY29tL3Bhc3N3b3JkQ2hhbmdlUmVxdWlyZWQiOmZhbHNlLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9lbWFpbCI6Imo2M2s4OTVuanBhOG1ubXE0cDJqbXN0dGNoOTBzYWhiQG1hYXMtbXN0YXIuY29tIiwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vcm9sZSI6WyJFQy5TZXJ2aWNlLkNvbmZpZ3VyYXRpb24iLCJFQy5TZXJ2aWNlLkhvc3RpbmciLCJFQ1VTLkFQSS5BdXRvY29tcGxldGUiLCJFQ1VTLkFQSS5TY3JlZW5lciIsIkVDVVMuQVBJLlNlY3VyaXRpZXMiLCJQQUFQSVYxLlhyYXkiLCJWZWxvVUkuQWxsb3dBY2Nlc3MiXSwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vY29tcGFueV9pZCI6IjI4MmNjODY1LTM1MDUtNGUyMC04ZDI0LTg0YTQzOGIyMTkyNCIsImh0dHBzOi8vbW9ybmluZ3N0YXIuY29tL2ludGVybmFsX2NvbXBhbnlfaWQiOiJDbGllbnQwIiwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vZGF0YV9yb2xlIjpbIkVDVVMuRGF0YS5VUy5PcGVuRW5kRnVuZHMiLCJRUy5NYXJrZXRzIiwiUVMuUHVsbHFzIiwiU0FMLlNlcnZpY2UiXSwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vbGVnYWN5X2NvbXBhbnlfaWQiOiIyNGJmMGE4NS0zMjcxLTRiMWItYWIxZS0wZTlmZDE4ODE4YmQiLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS91aW1fcm9sZXMiOiJFQU1TLE1VX01FTUJFUl8xXzEiLCJpc3MiOiJodHRwczovL2xvZ2luLXByb2QubW9ybmluZ3N0YXIuY29tLyIsInN1YiI6ImF1dGgwfDc2NjU2NkFELTkxMjEtNDJDMS05RjM2LTkwREM1RkNENUUxQyIsImF1ZCI6WyJodHRwczovL2F1dGgwLWF3c3Byb2QubW9ybmluZ3N0YXIuY29tL21hYXMiLCJodHRwczovL3VpbS1wcm9kLm1vcm5pbmdzdGFyLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE3MTMzNDk2OTAsImV4cCI6MTcxMzM1MzI5MCwic2NvcGUiOiJvcGVuaWQiLCJndHkiOiJwYXNzd29yZCIsImF6cCI6ImlRa1d4b2FwSjlQeGw4Y0daTHlhWFpzYlhWNzlnNjRtIn0.xzbzSy4JD5FR7NcE3w7m4ew9mXVvylNobwSmM0MnqW-AQWaNSAVziUvKM74jWUNS94Eo5c3ZQfscmB0S4nbY1MvG095x_YKhDhH-C6klyYQj3L3QD4BVlGr8PPprdHqd4uPlcYpLml4QysmQD7EhxGWw7jRV_uXeS8-XqXp6maYSHihQ2jn8qChWlVnOFUWCdEsyqtdxqDW3hYtGB2JYwGAxStJE6DdTqLCjT2iByd-Pv7gCTgq3vMj2oiBxTxZdgiSvIpeWp0fh7NALE4KwngP_efCmkjdD38YwmgPV-eVBHRkGe_yzAznzSbJsrNia0swmXjCg-g-cVfKr2sTTpA',
    'credentials': 'omit',
    'dnt': '1',
    'origin': 'https://my.morningstar.com',
    'referer': 'https://my.morningstar.com/my/report/fund/portfolio.aspx?t=0P00016LLS&fundservcode=&lang=en-MY',
    'sec-ch-ua': '"Chromium";v="123", "Not:A-Brand";v="8"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'x-api-realtime-e': 'eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.jidbnhWUJvWMvD2omPxv9L_-iiW424c89SMUexsXDrY3VE6CfIU1g6cRPZ-DToeSbtFEvMPV4DoNlOtzUJ63Ryja23rtjlMsrpV-nLpUUpjcZp7ZL0YjGQNbsq1a-vAwf7GBOk6lnsOWycXB0mKaHMXfHpgwAsRcfGK1QpIb27U.da0qHJBnRmc0_EOq.1fXioE66EIItzsggPK3b4HNypNp1Ltva84HWRNmxwBPsUo5kvUXaYDuFjLHT2K39RsoMqZzERuQMfrP8fYoqTfkBmg-xD5sQbqHYeKgvuqMdOlkRVx4y16ft1RiliknyWNGxTd_5KXZrkDadGR7gHQyC775iNEBi0bI9F_JXu3t7_8uBMHNOYlIeRHm2Dqmz17ukL_zoGOX74_KiaYkBL5RamA.7wz-dbBZR15KCDRanQrHPg',
    'x-api-requestid': 'f54acede-ef9f-93ef-57b6-c6537ec6dfe3',
    'x-sal-contenttype': 'e7FDDltrTy+tA2HnLovvGL0LFMwT+KkEptGju5wXVTU=',
}

params = {
    'languageId': 'en',
    'locale': 'en',
    'clientId': 'MDC_intl',
    'benchmarkId': 'mstarorcat',
    'component': 'sal-components-mip-factor-profile',
    'version': '3.60.0',
}

response = requests.get(
    'https://www.us-api.morningstar.com/sal/sal-service/fund/factorProfile/F00000W8R0/data',
    params=params,
    headers=headers,
)

print(pd.DataFrame(response.json()["factors"]["liquidity"])[["categoryAvg", "percentile"]].drop_duplicates())
