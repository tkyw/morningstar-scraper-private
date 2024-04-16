from playwright.sync_api import sync_playwright
from time import sleep
import pandas as pd
from datetime import datetime

df = pd.DataFrame()
fund_name = "ASTUTE MALAYSIA GROWTH TRUST".upper()
with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto("https://www.astutefm.com.my/resources/fundinfo")
    # sleep(5)
    page.get_by_role("link", name="I agree").click()
    container = page.query_selector("#historical_fund_prices")
    fund = container.query_selector("div > select").select_option(fund_name)

    page.locator("#calendar1").fill("1978-08-29")
    today_date = str(datetime.today().date())
    page.locator("#calendar2").fill(today_date)
    page.mouse.wheel(0, -250)
    page.locator("#MainContent_search").scroll_into_view_if_needed()
    page.locator("#MainContent_search").click()
    # sleep(8)
    page.locator("select[name=\"show_funds_2_length\"]").select_option("100")


    pages = [text.inner_text() for text in page.query_selector_all(".paginate_button")]
    print(pages)
    for p in range(1, int(pages[-5])):
        print("Scraping page: {}".format(p))
        table = page.content()
        temp_df = pd.read_html(table)[0]
        df = pd.concat([df, temp_df], axis=0)
        df.drop_duplicates(inplace=True)
        print(df)
        page.locator("#show_funds_2_next").click()
    browser.close()
df.to_csv("{}.csv".format(fund_name))
