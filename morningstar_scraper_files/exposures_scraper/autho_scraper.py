from playwright.sync_api import sync_playwright
from time import sleep

ticker = "F00001443C"  # 0P00016LLS, F00001443C, F00001443D
user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
authos = []  # Global variable to store authorization headers

def track_exposure_links(response):
    url = response.url
    if url.startswith("https://www.us-api.morningstar.com"):
        try:
            headers = response.all_headers()
            autho = headers["authorization"]
            authos.append(autho)
        except Exception as e:
            print(f"Error accessing headers: {e}")

def trigger_page(page):
    page.locator("div.sal--wrapper--nav > ul > li").nth(5).click()
    sleep(3)

def process_links(playwright, ticker):
    url = f"https://my.morningstar.com/my/report/fund/portfolio.aspx?t={ticker}&fundservcode=&lang=en-MY"
    chromium = playwright.chromium
    browser = chromium.launch(headless=False)
    context = browser.new_context(user_agent=user_agent)
    page = context.new_page()
    page.on("request", track_exposure_links)  # get the authorization
    page.goto(url)
    trigger_page(page)
    page.close()
    context.close()
    browser.close()

def get_exposure_links(ticker):
    with sync_playwright() as playwright:
        process_links(playwright, ticker)
    return authos

def main():
    global authos
    exposure_links = get_exposure_links(ticker)
    autho = list(set(exposure_links))[0]
    with open("autho.txt", "w") as wf:
        wf.write(autho)
    return autho

if __name__ == "__main__":
    main()
