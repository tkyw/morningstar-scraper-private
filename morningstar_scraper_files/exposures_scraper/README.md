# MorningStar Exposures Scraper

This project aims to collect the different exposures of the funds from morningstar, including:

1. Liquidity Exposures
2. Countries Exposures
3. Sector Expsures
4. Top Holdings

**To ensure the scraping speed and minimize the error when scraping, I have split each exposure's scraper into different files, please run them seperately and use the combine_exposures.py to merge them**

> I haven't have the time to handle the requests error, so in case the amount of fund you scraped are different for each exposures, run them several time to ensure the missing funds are scraped. I will handle this in the future...
