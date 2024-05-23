from sys import prefix
import requests
import pandas as pd
import numpy as np
from pprint import pprint as pp
from morningstar_scraper import get_res_from_screener
from xlwings import view, load
import concurrent.futures
import os.path

def main():
    global fund_df
    for i in range(page, max_page + 1)[:]:
        res = get_res_from_screener(i, page_size, user_agent)
        print(f"Request to page {i} was successfull")
        info = res['rows']
        info_df = pd.json_normalize(info)
        try:
            info_df = info_df.loc[:, ["SecId", "Name", "PriceCurrency", "CategoryName", "StarRatingM255"]]
        except:
            info_df = info_df.loc[:, ["SecId", "Name", "PriceCurrency", "CategoryName"]]
            info_df.loc[:, "StarRatingM255"] = np.nan
        fund_df = pd.concat([fund_df, info_df], axis=0)

def cumulative(df):
    df_rets = df.pct_change() + 1
    return (df_rets).prod() - 1

def calendar_year_return(df, latest_date, last_date, first_year, w1=1, w2=0, is_cumulative=False):
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, list(reversed(range(start, end+1))))))
    df_new["YTD"] = ((df.pct_change().loc[latest_date.split("-")[0]].dropna(axis=0)) + 1).prod() - 1
    for year in [1, 3, 5, 7, 10]:
        print(pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*year)-1))
        periodic_return = df.pct_change().loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*year)-3):]
        for idx in periodic_return.columns:
            print(periodic_return.loc[:, idx].shape[0], year * 12)
            if periodic_return.loc[:, idx].shape[0] == (year * 12):
                df_new.loc[idx, f"{year} Year"] = ((periodic_return.loc[:, idx].dropna(axis=0)) + 1).prod() -1
            else:
                df_new.loc[idx, f"{year} Year"] = np.nan
    inception_df = df.resample("M").last()
    target_index = str(df.index[0].date())
    inception_df.loc[target_index, :]  = df.loc[target_index, :]
    inception_df = inception_df.sort_index()
    df_new["Since Inception"] = ((inception_df.pct_change()) + 1).prod() -1
    for year in list(reversed(range(start, end+1))):
        print(df.pct_change().loc[str(year)]  + 1)
        df_new[str(year)] = (df.pct_change().loc[str(year)]  + 1).prod() -1
    print(df_new)
    return df_new

def rolling_return(df,w1=1,w2=0, start_date=None):
    start_date = start_date if start_date is not None else df.index[0]
    df = df.pct_change(periods=freq).loc[start_date:, :].dropna(axis=0)
    if w1 != 1:
        df["Benchmark"] = (df['Benchmark1'] * w1)  + (df['Benchmark2'] * w2)
        df = df.iloc[:, [0,-1]]
    df = df.resample("2BQ").last().sort_index(ascending=False).T
    return df


def semi_deviation(df, w1=1, w2=0):
    if w1 != 1:
        df.loc[:, 'Benchmark1'] = df.loc[:, 'Benchmark1'] * w1
        df.loc[:, 'Benchmark2'] = df.loc[:, 'Benchmark2'] * w2
        df.loc[:, 'Benchmark'] = df.loc[:, 'Benchmark1'] + df.loc[:, 'Benchmark2']
        df = df.iloc[:, [0,-1]]
    msq = (df[df < 0] ** 2).sum()
    # print(df)
    sd = (msq / (df.shape[0] - 1)) ** 0.5
    return sd



def downside_risk(df, latest_date, last_date, first_year, w1=1, w2=0):
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, list(reversed(range(start,end+1))))))
    df_new["YTD"] = semi_deviation((rets(df,w1,w2).loc[latest_date.split("-")[0]].dropna(axis=0)), w1=w1,w2=w2) * np.sqrt(12)
    for year in [1,3,5,7,10]:
        periodic_return = rets(df,w1,w2).loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*year)-1):]
        if periodic_return.shape[0] == year * 12:
            df_new[f"{year} Year"] = semi_deviation((periodic_return.dropna(axis=0)), w1=w1,w2=w2) * np.sqrt(12)
        else:
            df_new[f"{year} Year"] = [np.nan]
    df_new["Since Inception"] = semi_deviation((rets(df,w1,w2).loc[first_year:]), w1=w1,w2=w2) * np.sqrt(12)

    for year in list(reversed(range(start,end+1))):
        try:
            df_new[str(year)] = semi_deviation(rets(df,w1,w2).loc[str(year)], w1=w1,w2=w2) * np.sqrt(12)
        except:
            print(df_new[str(year)])

    return df_new

def drawdown(df, latest_date, last_date, first_year, w1=1, w2=0):
    dfs = [
        df.loc[latest_date.split("-")[0]],
        df.loc[pd.to_datetime(last_date):],
        df.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*3)-1):],
        df.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*5)-1):],
        df.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*7)-1):],
        df.loc[pd.to_datetime(latest_date) -  pd.DateOffset(months=((12)*10)-1):],
        df.loc[first_year:]
            ]
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"])

    for df_temp, col in zip(dfs, df_new):
        if (col == "YTD" or col == "Since Inception") or (df_temp.shape[0] == int(col.split(" ")[0]) * 12):
            max_price = df_temp.dropna(axis=0).cummax()
            max_drawdown = ((df_temp.dropna(axis=0) - max_price) / max_price)
            if w1 != 1:
                max_drawdown['Benchmark'] = max_drawdown['Benchmark1'] * w1 + max_drawdown['Benchmark2'] * w2
                max_drawdown = max_drawdown.iloc[:, [0,-1]]
            max_drawdown_val = max_drawdown.min()
            df_new[col] = max_drawdown_val
        else:
            df_new[col] = [np.nan]
    return df_new
def drawdown_duration(df, latest_date, last_date, first_year, w1=1, w2=0):
    dfs = [
        df.loc[latest_date.split("-")[0]],
        df.loc[pd.to_datetime(last_date):],
        df.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*3)-1):],
        df.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*5)-1):],
        df.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*7)-1):],
        df.loc[pd.to_datetime(latest_date) -  pd.DateOffset(months=((12)*10)-1):],
        df.loc[first_year:]]
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"])

    for df_temp, col in zip(dfs, df_new):
        if (col == "YTD" or col == "Since Inception") or (df_temp.shape[0] == int(col.split(" ")[0]) * 12):
            max_price = df_temp.dropna(axis=0).cummax()
            max_drawdown = ((df_temp.dropna(axis=0) - max_price) / max_price)
            if w1 != 1:
                max_drawdown['Benchmark'] = max_drawdown['Benchmark1'] * w1 + max_drawdown['Benchmark2'] * w2
                max_drawdown = max_drawdown.iloc[:, [0,-1]]
            max_drawdown_date = max_drawdown.idxmin()
            returns1 = max_drawdown.loc[max_drawdown_date.iloc[0]:, "Fund"]
            returns2 = max_drawdown.loc[max_drawdown_date.iloc[1]:, "Benchmark"]
            returns = [returns1, returns2]

            dates = []
            for ret in returns:
                try:
                    date = ret.loc[ret >= 0].dropna(axis=0).index[0]
                except IndexError:
                    date = None
                dates.append(date)
            dates = pd.Series(dates, index=['Fund', 'Benchmark'])
            try:
                drawdown_dn =(dates - max_drawdown_date)
            except:
                drawdown_dn = pd.Series(dates, index=['Fund', 'Benchmark'])
            df_new[col] = drawdown_dn
        else:
            df_new[col] = [np.nan]
    return df_new

def conditional_rets(df, freq):
    if freq == "DM":
        return df.pct_change()
    else:
        nw_df = df.resample(freq).last()
        nw_df.loc[df.index[0], :] = df.loc[df.index[0], :]
        nw_df = nw_df.sort_index().pct_change()
        return nw_df


def rets(df, w1=1,w2=0, freq="M"):
    if w1 != 1:
        df = conditional_rets(df, freq)
        df['Benchmark'] = df['Benchmark1'] * w1 + df['Benchmark2'] * w2
        df = df.iloc[:, [0,-1]]
        return df
    else:
        return conditional_rets(df, freq)

def annalised_std(df, df_rets, latest_date, last_date, first_year ,w1, w2):
    years = list(reversed(range(start,end+1)))
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, years)))
    dfs = [
    df_rets.loc[latest_date.split("-")[0]],
    df_rets.loc[pd.to_datetime(last_date):],
    df_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*3)-1):],
    df_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*5)-1):],
    df_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*7)-1):],
    df_rets.loc[pd.to_datetime(latest_date) -  pd.DateOffset(months=((12)*10)-1):],
    df_rets.loc[first_year:]
            ]
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, years)))
    for df, col in zip(dfs, df_new.columns[:8]):
        if (col == "YTD" or col == "Since Inception") or (df.shape[0] == int(col.split(" ")[0]) * 12):
            df_new[col] = df.std() * np.sqrt(12)
        else:
            df_new[col] = [np.nan]
    df_new.loc[:, list(map(str,years))] =  (df_rets.resample("Y").agg(np.std) * np.sqrt(12)).sort_index(ascending=False).values.T
    return df_new

def beta(df, latest_date, last_date, first_year, w1=1, w2=0):
    periodic_rets = rets(df, w1, w2, freq="DM")
    dfs = [
        periodic_rets.loc[latest_date.split("-")[0]],
        periodic_rets.loc[pd.to_datetime(last_date):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*3)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*5)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*7)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) -  pd.DateOffset(months=((12)*10)-1):],
        periodic_rets.loc[first_year:]
            ]
    years = list(reversed(range(start,end+1)))
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, years)))
    for df_rets, col in zip(dfs, df_new.columns[:8]):
        if (col == "YTD" or col == "Since Inception") or (df_rets.shape[0] == int(col.split(" ")[0]) * 12):
            df_rets_nona = df_rets.dropna(axis=0)
            try:
                # print(col)
                beta, intercept = np.polyfit(y=df_rets_nona['Fund'], x=df_rets_nona['Benchmark'], deg=1)
            except:
                # print(col)
                ...
            df_new[col] = [beta]
        else:
            df_new[col] = [np.nan]

    for year in years:
        df_active = rets(df, w1,w2, freq="DM").loc[str(year)].dropna(axis=0)
        try:
            beta, intercept = np.polyfit(y=df_active['Fund'], x=df_active['Benchmark'], deg=1)
        except:
            beta = [np.nan]
        df_new[str(year)] = [beta]

    return df_new
def tracking_error(df, latest_date, last_date, first_year, w1=1, w2=0):
    periodic_rets = rets(df, w1, w2)
    dfs = [
        periodic_rets.loc[latest_date.split("-")[0]],
        periodic_rets.loc[pd.to_datetime(last_date):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*3)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*5)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*7)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) -  pd.DateOffset(months=((12)*10)-1):],
        periodic_rets.loc[first_year:]
            ]
    years = list(reversed(range(start,end+1)))
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, years)))
    for df_rets, col in zip(dfs, df_new.columns[:8]):
        if (col == "YTD" or col == "Since Inception") or (df_rets.shape[0] == int(col.split(" ")[0]) * 12):
            df_active = df_rets.dropna(axis=0)
            try:
                df_active['alpha'] = df_active['Fund'] - df_active['Benchmark']
                tracking_error = df_active['alpha'].std()
            except:
                print(col)
            df_new[col] = [tracking_error * np.sqrt(12)]
        else:
            df_new[col] = [np.nan]
    for year in years:
        df_active = rets(df, w1, w2).loc[str(year)].dropna(axis=0)
        try:
            df_active['alpha'] = df_active['Fund'] - df_active['Benchmark']
            tracking_error = df_active['alpha'].std()
            az_error = tracking_error * np.sqrt(12)
        except:
            az_error = np.nan
        df_new[str(year)] = [az_error]
    return df_new

def sharpe_ratio(df, latest_date, last_date, first_year, w1=1, w2=0):
    periodic_rets = rets(df, w1, w2)
    dfs = [
        periodic_rets.loc[latest_date.split("-")[0]],
        periodic_rets.loc[pd.to_datetime(last_date):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*3)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*5)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*7)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) -  pd.DateOffset(months=((12)*10)-1):],
        periodic_rets.loc[first_year:]
            ]
    years = list(reversed(range(start,end+1)))
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, years)))
    for df_rets, col in zip(dfs, df_new.columns[:7]):
        if (col == "YTD" or col == "Since Inception") or (df_rets.shape[0] == int(col.split(" ")[0]) * 12):
            df_active = df_rets.dropna(axis=0)
            try:
                df_sharpe = df_active - (risk_free_rate/12)
                annualized_sharpe_ratio = (df_sharpe.mean() / df_sharpe.std()) * np.sqrt(12)
            except:
                print(col)
            df_new[col] = annualized_sharpe_ratio
        else:
            df_new[col] = [np.nan]

    for year in years:
        df_active = rets(df, w1, w2).loc[str(year)].dropna(axis=0)
        try:
            df_sharpe = df_active - (risk_free_rate/12)
            annualized_sharpe_ratio = (df_sharpe.mean() / df_sharpe.std()) * np.sqrt(12)
        except:
            annualized_sharpe_ratio = np.nan
        df_new[str(year)] = annualized_sharpe_ratio
    return df_new

def treynor_ratio(df, latest_date, last_date, first_year, w1=1, w2=0):
    periodic_rets = rets(df, w1, w2)
    dfs = [
        periodic_rets.loc[latest_date.split("-")[0]],
        periodic_rets.loc[pd.to_datetime(last_date):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*3)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*5)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*7)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) -  pd.DateOffset(months=((12)*10)-1):],
        periodic_rets.loc[first_year:]
            ]
    years = list(reversed(range(start,end+1)))
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, years)))
    for df_rets, col in zip(dfs, df_new.columns[:8]):
        if (col == "YTD" or col == "Since Inception") or (df_rets.shape[0] == int(col.split(" ")[0]) * 12):
            df_active = df_rets.dropna(axis=0)
            df_beta_rets = rets(df.dropna(axis=0), w1, w2, freq="DM").dropna(axis=0)
            beta, intercept = np.polyfit(x=df_beta_rets['Fund'], y=df_beta_rets['Benchmark'], deg=1)
            annualized_treynor = (((df_active.mean()  *12) - risk_free_rate) / beta)
            df_new[col] = annualized_treynor
        else:
            df_new[col] = [np.nan]
    for year in years:
        df_active = rets(df, w1, w2).loc[str(year)].dropna(axis=0)
        df_beta_rets = rets(df, w1, w2, freq="DM").loc[str(year)].dropna(axis=0)
        try:
            beta, intercept = np.polyfit(x=df_beta_rets['Fund'], y=df_beta_rets['Benchmark'], deg=1)
            annualized_treynor = (((df_active.mean()  *12) - risk_free_rate) / beta)
        except:
            annualized_treynor = [np.nan]
        df_new[str(year)] = annualized_treynor
    return df_new

def information_ratio(df, latest_date, last_date, first_year, w1=1, w2=0):
    periodic_rets = rets(df, w1, w2)
    dfs = [
        periodic_rets.loc[latest_date.split("-")[0]],
        periodic_rets.loc[pd.to_datetime(last_date):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*3)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*5)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*7)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) -  pd.DateOffset(months=((12)*10)-1):],
        periodic_rets.loc[first_year:]
            ]
    years = list(reversed(range(start,end+1)))
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, years)))
    for df_rets, col in zip(dfs, df_new.columns[:8]):
        if (col == "YTD" or col == "Since Inception") or (df_rets.shape[0] == int(col.split(" ")[0]) * 12):
            df_active = df_rets.dropna(axis=0)
            df_active['alpha'] = df_active['Fund'] - df_active['Benchmark']
            tracking_error = df_active['alpha'].std()
            # print(df_active)
            information_ratio = (df_active['alpha'].mean() / tracking_error) * np.sqrt(12)
            df_new[col] = [information_ratio]
        else:
            df_new[col] = [np.nan]

    for year in years:
        df_active = rets(df, w1, w2).loc[str(year)].dropna(axis=0)
        try:
            df_active['alpha'] = df_active['Fund'] - df_active['Benchmark']
            tracking_error = df_active['alpha'].std()
            information_ratio = (df_active['alpha'].mean() / tracking_error) * np.sqrt(12)
        except:
            information_ratio = np.nan
        df_new[str(year)] = [information_ratio]
    return df_new

def geomean(df):
    df += 1
    return (df.prod() ** (1 / df.shape[0])) - 1

def capture_ratio(df, latest_date, last_date, first_year, w1=1, w2=0):
    dfs = [
        df.loc[latest_date.split("-")[0]],
        df.loc[pd.to_datetime(last_date):],
        df.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*3)-1):],
        df.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*5)-1):],
        df.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*7)-1):],
        df.loc[pd.to_datetime(latest_date) -  pd.DateOffset(months=((12)*10)-1):],
        df.loc[first_year:]
            ]
    years = list(reversed(range(start,end+1)))
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, years)))
    for df, col in zip(dfs, df_new.columns[:8]):
        if (col == "YTD" or col == "Since Inception") or (df.shape[0] == int(col.split(" ")[0]) * 12):
            df_active = rets(df.dropna(axis=0).resample("D").last(), w1, w2).dropna(axis=0)

            # modification
            upside_fund_geo = geomean(df_active.query("Fund > 0"))
            upside_bench_geo = geomean(df_active.query("Benchmark > 0"))

            upside_capture = upside_fund_geo['Fund'] / upside_bench_geo['Benchmark']
            try:
                downside_fund_geo = geomean(df_active.query("Fund <= 0"))
                downside_bench_geo = geomean(df_active.query("Benchmark <= 0"))
                downside_capture = downside_fund_geo['Fund'] / downside_bench_geo['Benchmark']

                capture_ratio = upside_capture / downside_capture
                infos = pd.Series([upside_capture, downside_capture, capture_ratio])
                df_new[col] = infos
            except ZeroDivisionError:
                downside_capture = np.nan
                capture_ratio = upside_capture
                df_new[col] = pd.Series([upside_capture, downside_capture, capture_ratio])
        else:
            df_new[col] = pd.Series([np.nan] * 3)

    for year in years:
        df_active = rets(df.loc[str(year)].dropna(axis=0).resample("D").last(), w1, w2).dropna(axis=0)
        if df_active.shape[0] != 0:
            try:
                upside = df_active.query("Benchmark > 0")
                upside_geo = geomean(upside)
                upside_capture = upside_geo['Fund'] / upside_geo['Benchmark']
                # print(upside_capture)
                downside = df_active.query("Benchmark <= 0")
                downside_geo = geomean(downside)
                downside_capture = downside_geo['Fund'] / downside_geo['Benchmark']
                # print(downside_capture)
                capture_ratio = upside_capture / downside_capture
            except ZeroDivisionError:
                upside_capture = upside_capture
                downside_capture = np.nan
                capture_ratio = upside_capture
        else:
            upside_capture = np.nan
            downside_capture = np.nan
            capture_ratio = np.nan
        df_new[str(year)] = [upside_capture, downside_capture, capture_ratio]
    return df_new

def batting_average(df, latest_date, last_date, first_year, w1=1, w2=0):
    periodic_rets = rets(df, w1, w2)
    dfs = [
        periodic_rets.loc[latest_date.split("-")[0]],
        periodic_rets.loc[pd.to_datetime(last_date):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*3)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*5)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) - pd.DateOffset(months=((12)*7)-1):],
        periodic_rets.loc[pd.to_datetime(latest_date) -  pd.DateOffset(months=((12)*10)-1):],
        periodic_rets.loc[first_year:]
            ]
    years = list(reversed(range(start,end+1)))
    df_new = pd.DataFrame(columns=['YTD', "1 Year", "3 Year", "5 Year", "7 Year", "10 Year", "Since Inception"] + list(map(str, years)))
    for df_rets, col in zip(dfs, df_new.columns[:8]):
        if (col == "YTD" or col == "Since Inception") or (df_rets.shape[0] == int(col.split(" ")[0]) * 12):
            df_active = df_rets.dropna(axis=0)
            df_active['alpha'] = df_active['Fund'] - df_active['Benchmark']
            batting_average = df_active.query("alpha > 0").shape[0] / df_active.shape[0]
            df_new[col] = [batting_average]
        else:
            df_new[col] = [np.nan]
    for year in years:
        df_active = rets(df, w1, w2).loc[str(year)].dropna(axis=0)
        if df_active.shape[0] != 0:
            df_active['alpha'] = df_active['Fund'] - df_active['Benchmark']
            batting_average = df_active.query("alpha > 0").shape[0] / df_active.shape[0]
        else:
            batting_average = np.nan
        df_new[str(year)] = [batting_average]
    return df_new

def get_fund_data(ticker: str):
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1EY3hOemRHTnpGRFJrSTRPRGswTmtaRU1FSkdOekl5TXpORFJrUTROemd6TWtOR016bEdOdyJ9.eyJodHRwczovL21vcm5pbmdzdGFyLmNvbS9tc3Rhcl9pZCI6Ijc2NjU2NkFELTkxMjEtNDJDMS05RjM2LTkwREM1RkNENUUxQyIsImh0dHBzOi8vbW9ybmluZ3N0YXIuY29tL3Bhc3N3b3JkQ2hhbmdlUmVxdWlyZWQiOmZhbHNlLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9lbWFpbCI6Imo2M2s4OTVuanBhOG1ubXE0cDJqbXN0dGNoOTBzYWhiQG1hYXMtbXN0YXIuY29tIiwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vcm9sZSI6WyJFQy5TZXJ2aWNlLkNvbmZpZ3VyYXRpb24iLCJFQy5TZXJ2aWNlLkhvc3RpbmciLCJFQ1VTLkFQSS5BdXRvY29tcGxldGUiLCJFQ1VTLkFQSS5TY3JlZW5lciIsIkVDVVMuQVBJLlNlY3VyaXRpZXMiLCJQQUFQSVYxLlhyYXkiLCJWZWxvVUkuQWxsb3dBY2Nlc3MiXSwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vY29tcGFueV9pZCI6IjI4MmNjODY1LTM1MDUtNGUyMC04ZDI0LTg0YTQzOGIyMTkyNCIsImh0dHBzOi8vbW9ybmluZ3N0YXIuY29tL2ludGVybmFsX2NvbXBhbnlfaWQiOiJDbGllbnQwIiwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vZGF0YV9yb2xlIjpbIkVDVVMuRGF0YS5VUy5PcGVuRW5kRnVuZHMiLCJRUy5NYXJrZXRzIiwiUVMuUHVsbHFzIiwiU0FMLlNlcnZpY2UiXSwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vbGVnYWN5X2NvbXBhbnlfaWQiOiIyNGJmMGE4NS0zMjcxLTRiMWItYWIxZS0wZTlmZDE4ODE4YmQiLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS91aW1fcm9sZXMiOiJFQU1TLE1VX01FTUJFUl8xXzEiLCJpc3MiOiJodHRwczovL2xvZ2luLXByb2QubW9ybmluZ3N0YXIuY29tLyIsInN1YiI6ImF1dGgwfDc2NjU2NkFELTkxMjEtNDJDMS05RjM2LTkwREM1RkNENUUxQyIsImF1ZCI6WyJodHRwczovL2F1dGgwLWF3c3Byb2QubW9ybmluZ3N0YXIuY29tL21hYXMiLCJodHRwczovL3VpbS1wcm9kLm1vcm5pbmdzdGFyLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE3MTY0MzMwNjUsImV4cCI6MTcxNjQzNjY2NSwic2NvcGUiOiJvcGVuaWQiLCJndHkiOiJwYXNzd29yZCIsImF6cCI6ImlRa1d4b2FwSjlQeGw4Y0daTHlhWFpzYlhWNzlnNjRtIn0.s1rNdxrU87bYxx27OyCAL-Q-LH2pTuvgkd3SwvBBMiW_cLPLgdXpUg4HL9cCenyhThedxfxSzoOA6oF43zo-KQVe5x8ssSzcVhvCii9iDHgqMwBIC8Wyx7eznag4jyGMQL3ge52Tp0czuU0O_gEWdfYRHwcgctlxi_rP56TaYtFSYEsCpBjL22UE3LyFCO1yB2cchujVG0KR7YJiigAjO8MDP7e-9v9FrMiM5DNxDVvO8FgAHRtvW9qwsXjlEXFRo1pZDE7upfqXuG9xGQxUr6m63aZuUZ4quTp9VGWP4T2qrOCFTdc_dsd1xE0eJ3uZkJM6NME8IKdjz5jh6X7Cbg',
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

    response = requests.get(
        f'https://www.us-api.morningstar.com/sal/sal-service/fund/performance/v3/{ticker}',
        params=params,
        headers=headers,
    )
    try:
        data = response.json()["graphData"]
    except:
        print("graphData problem")
        return
    return data

def clean_data(ticker):
    global start, end, freq
    global fund_df
    print(f"Now scraping {df[ticker]}...")
    data = get_fund_data(ticker)
    try:
        category = pd.json_normalize(data["category"]).set_index("date").rename(columns={"value": "Benchmark"})
    except KeyError:
        print(f"Failed to scrape {df[ticker]}")
        with open("error.txt", 'a') as wf:
            wf.write(f"{df[ticker]}\n")
        return
    fund = pd.json_normalize(data["fund"]).set_index("date").rename(columns={"value": "Fund"})
    temp_df = pd.concat([fund, category], axis=1)
    temp_df.index = pd.to_datetime(temp_df.index)
    temp_df = temp_df.sort_index()
    temp_df = temp_df.loc[:, :]
    start = temp_df.index[0].year
    end = 2024
    latest_date = temp_df.dropna().index[-1].date()
    last_date = str((latest_date - pd.DateOffset(months=11)).date())
    latest_date = str(latest_date)
    first_year = str(start)
    freq = freqs['M']
    print(temp_df)
    # ======= start cleaning data and caluclate fund's performance

    c_return = calendar_year_return(temp_df, latest_date=latest_date, last_date=last_date, first_year=first_year)
    c_return.rename(lambda x: "return:" + x, axis=0, inplace=True)
    excess_return = (c_return.loc["return:Fund", :] - c_return.loc["return:Benchmark", :]).to_frame().T
    excess_return.rename(lambda x: "excess return:Fund", axis=0, inplace=True)
    mx_draw = drawdown(temp_df, latest_date, last_date, first_year, w1=w1, w2=w2)
    mx_draw.rename(lambda x: "drawdown:" + x, axis=0, inplace=True)
    df_rets = rets(temp_df, w1=w1, w2=w2)
    annualised_stds = annalised_std(temp_df, df_rets, latest_date=latest_date, last_date=last_date, first_year=first_year, w1=w1, w2=w2)
    annualised_stds.rename(lambda x: "std:" + x, axis=0, inplace=True)
    betas = beta(temp_df, latest_date, last_date, first_year, w1=w1, w2=w2)
    betas.rename(lambda x: "beta", axis=0, inplace=True)
    tracking_errors = tracking_error(temp_df, latest_date, last_date, first_year, w1=w1, w2=w2)
    tracking_errors.rename(lambda x: "tracking error", axis=0, inplace=True)
    sharpe_ratios = sharpe_ratio(temp_df, latest_date, last_date, first_year, w1=w1, w2=w2)
    sharpe_ratios.rename(lambda x: "Sharpe: " + x, axis=0, inplace=True)
    information_ratios = information_ratio(temp_df, latest_date, last_date, first_year, w1=w1, w2=w2)
    information_ratios.rename(lambda x: "Information Ratio", axis=0, inplace=True)
    capture_ratios = capture_ratio(temp_df, latest_date, last_date, first_year, w1=w1, w2=w2)
    capture_ratios.rename(lambda x: {0: "Upside Capture", 1: "Downside Capture", 2: "Capture Ratio"}[x] , axis=0, inplace=True)
    batting_averages = batting_average(temp_df, latest_date, last_date, first_year, w1=w1, w2=w2)
    batting_averages.rename(lambda x: "Batting Average", axis=0, inplace=True)
    info_df = pd.concat([c_return, excess_return , mx_draw, annualised_stds, betas, tracking_errors, sharpe_ratios, information_ratios, capture_ratios, batting_averages], axis=0)
    info_df = info_df.loc[[True if "Benchmark" not in row else False for row in info_df.index], ["1 Year", "3 Year", "5 Year"]]
    info_df.index = info_df.index.str.replace(r"(\:.*)", "", regex=True)
    info_df = info_df.stack().to_frame()
    info_df.columns = [df[ticker]]
    fund_df = pd.concat([fund_df, info_df], axis=1)
    print(fund_df)

if __name__ == "__main__":
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    page = 1
    max_page = 40
    page_size = 50

    freqs = {
        "D": 252,
        "M": 12,
        "Q": 252/4,
    }
    risk_free_rate = 0.03
    w1 = 1; w2 = 0
    temp_df = pd.read_excel("/Users/tkyw/Library/Mobile Documents/com~apple~CloudDocs/Documents/work/work Doc/Due Diligence/Fund Reviews/Morningstar Screener.xlsx")
    df = temp_df.set_index("SecId")["Name"].to_dict()
    category = temp_df.set_index("Name")["CategoryName"].to_dict()
    fund_df = pd.DataFrame()
    destination = "testing.csv"
    if os.path.exists(destination):
        scraped_fund = pd.read_csv(destination, index_col=[0,1], encoding="ISO-8859-1")
    else:
        scraped_fund = pd.DataFrame()
    with open("error.txt") as rf:
        error_data = [item.strip() for item in rf.readlines()]
    scraped_fund_names = scraped_fund.columns
    # with concurrent.futures.ThreadPoolExecutor(2) as executor:
    for ticker in list(df.keys())[:20]:
        if  (df[ticker] not in scraped_fund_names) and (df[ticker] not in error_data):
            clean_data(ticker)
                # executor.submit(clean_data, ticker)

    fund_df.loc["Category", :] = [category[col] for col in fund_df.columns]
    fund_df = pd.concat([fund_df, scraped_fund], axis=1)
    fund_df.to_clipboard(excel=True)
    fund_df.to_csv(destination)
