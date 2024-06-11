import pandas as pd
import numpy as np

class Financial_metrics:

  def __init__(self, df, risk_free_rate = 0.027, periods=12):
    self.df = df
    self.risk_free_rate = risk_free_rate
    self.periods = periods - 1

  def compute_daily_return(self) -> pd.DataFrame:
    return self.df.pct_change()

  def compute_return(self) -> pd.DataFrame :
    return self.df.pct_change(periods=self.periods + 1).dropna(axis=0, thresh=1)

  def compute_downside_risk(self) -> pd.DataFrame:
    daily_return = self.compute_daily_return()
    df_semistd = pd.DataFrame()
    for idx, data in daily_return.iterrows():
      current_date = daily_return.index.get_loc(idx)
      periodic_df = daily_return.iloc[ current_date - self.periods: current_date +1 ]
      df_semistd[idx] = np.sqrt((periodic_df[periodic_df < 0] ** 2).sum() / self.periods) * np.sqrt(self.periods + 1)
    return df_semistd.T.replace(0, np.nan)

  def compute_max_drawdown(self) -> pd.Series:
    df_max = self.df.cummax()
    df_drawdown = (self.df - df_max) / df_max
    max_drawdown = df_drawdown.min()
    return max_drawdown

  def compute_excess_returns(self) -> pd.DataFrame :
    daily_return = self.compute_daily_return()
    df_rolling_returns = self.compute_return()
    fund_rolling_returns = df_rolling_returns.iloc[:, ::2]
    benchmark_rolling_returns = df_rolling_returns.iloc[:, 1::2]
    benchmark_rolling_returns.columns = fund_rolling_returns.columns
    excess_returns = fund_rolling_returns - benchmark_rolling_returns
    return excess_returns

  def compute_infortmation_ratio(self) -> pd.DataFrame:
    daily_return = self.compute_daily_return()
    excess_returns = self.compute_excess_returns()

    fund_daily_returns = daily_return.iloc[:, ::2]
    benchmark_daily_returns = daily_return.iloc[:, 1::2]
    benchmark_daily_returns.columns = fund_daily_returns.columns
    daily_excess_returns = fund_daily_returns - benchmark_daily_returns


    df_std = pd.DataFrame()
    for idx, data in daily_excess_returns.iterrows():
      current_date = daily_excess_returns.index.get_loc(idx)
      periodic_df = daily_excess_returns.iloc[ current_date - self.periods: current_date +1 ]
      df_std[idx] = periodic_df.std() * np.sqrt(self.periods + 1)

    df_std = df_std.T.replace(0, np.nan)
    return excess_returns / df_std

  def compute_std(self) -> pd.DataFrame:
    daily_return = self.compute_daily_return()
    df_std = pd.DataFrame()
    for idx, data in daily_return.iterrows():
      current_date = daily_return.index.get_loc(idx)
      periodic_df = daily_return.iloc[ current_date - self.periods: current_date +1 ]
      df_std[idx] = periodic_df.std() * np.sqrt(self.periods + 1)
    df_std = df_std.T.replace(0, np.nan)
    return df_std

  def compute_sharpe_ratio(self) -> pd.DataFrame:
    daily_return = self.compute_daily_return()
    df_rolling_returns = self.compute_return()

    df_std = self.compute_std()

    sharpe_ratio = (df_rolling_returns -  (self.risk_free_rate * ((self.periods + 1) / 12)) ) / df_std

    return sharpe_ratio

  def compute_m2_alpha(self) -> pd.DataFrame:
    daily_return = self.compute_daily_return()
    df_rolling_returns = self.compute_return()
    sharpe_ratio = self.compute_sharpe_ratio()
    fund_sharpe_ratio = sharpe_ratio.iloc[:, ::2]

    df_std = self.compute_std()

    benchmark_std = df_std.iloc[:, 1::2]
    benchmark_std.columns = fund_sharpe_ratio.columns
    benchmark_rolling_returns = df_rolling_returns.iloc[:, 1::2]
    benchmark_rolling_returns.columns = fund_sharpe_ratio.columns
    m2 = (fund_sharpe_ratio * benchmark_std) + (self.risk_free_rate * ((self.periods + 1) / 12))
    m2_alpha = m2 - benchmark_rolling_returns
    return m2_alpha

def compute_financial_metrics(df):
    df = df.resample("M").last() ## remove the last row
    periods = [12, 36, 60]
    years = df.index.year.unique()
    start_dates = [years[-1], years[-3], years[-5]]
    df_metrics = pd.DataFrame()
    metrics = ["Return", "Downside Risk", "Information Ratio", "M2 Alpha"]
    for start_date, period in zip(start_dates, periods):
        periodic_year_financial_metrics = Financial_metrics(df, risk_free_rate=0.027, periods=period)
        periodic_year_return = periodic_year_financial_metrics.compute_return().loc[f"{start_date}":].mean().loc[::2]
        periodic_year_downside_risk = periodic_year_financial_metrics.compute_downside_risk().loc[f"{start_date}":].mean().loc[::2]
        periodic_year_information_ratio = periodic_year_financial_metrics.compute_infortmation_ratio().loc[f"{start_date}":].mean()
        periodic_year_m2_alpha = periodic_year_financial_metrics.compute_m2_alpha().loc[f"{start_date}":].mean()
        print(periodic_year_return)
        print(periodic_year_downside_risk)
        print(periodic_year_m2_alpha)
        print(periodic_year_information_ratio)
        df_metrics[[f"{period // 12 }Y {metric}" for metric in metrics]] = pd.concat([periodic_year_return, periodic_year_downside_risk, periodic_year_information_ratio, periodic_year_m2_alpha], axis=1)

    periodic_year_financial_metrics = Financial_metrics(df, risk_free_rate=0.027)
    periodic_year_max_drawdown = periodic_year_financial_metrics.compute_max_drawdown()
    df_metrics["Max Drawdown"] = periodic_year_max_drawdown.loc[::2]
    return df_metrics


if __name__ == "__main__":
    ## Load Dataset
    df = pd.read_csv('output.csv', index_col=0, parse_dates=True).astype(float)
    ## Compute financial metrics
    metrics = compute_financial_metrics(df)
    metrics.to_excel(r'metrics.xlsx') ## export to csv
