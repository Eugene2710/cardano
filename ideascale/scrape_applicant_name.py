import asyncio
from typing import Optional
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup, Tag


def fetch_html_selenium(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        print(f"Invalid URL: {url}. Skipping.")
        return ""
    # clean the url first
    url = url.strip()
    if not url:
        print(f"Empty url, skipping.")
        return ""
    # configure selenium for headless chrome
    chrome_options: Options = Options()
    # run Chrome without opening a browser window
    chrome_options.add_argument("--headless")
    # intialize chrome driver
    driver = webdriver.Chrome(options=chrome_options)
    try:
        try:
            driver.get(url)
        except Exception as e:
            print(f"Error loading URL {url}: {e}")
            return ""
        wait = WebDriverWait(driver, 60)
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.author-name.text-truncate")))
        except:
            print(f"Timeout waiting for element on {url}. Proceeding with available HTML.")
        return driver.page_source
    finally:
        driver.quit()


async def fetch_html(url: str) -> str:
    """
    wrap Selenium html fetcher in an async function
    """
    loop = asyncio.get_running_loop()
    html: str = await loop.run_in_executor(None, fetch_html_selenium, url)
    return html


def find_author_title(soup: BeautifulSoup) -> Optional[str]:
    author_tag: Optional[Tag] = soup.find('a', class_="author-name text-truncate")
    if author_tag:
        return author_tag.get("title") or author_tag.get_text(strip=True)
    return None


async def process_url(url: str) -> Optional[str]:
    """
    Responsible for processing a URL:
    fetch HTML -> parse HTML -> extract author title
    """
    html: str = await fetch_html(url)
    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    return find_author_title(soup)


async def main() -> None:
    # read csv
    df = pd.read_csv("Cardano Funding MasterSheet - data.csv")
    if "Link" not in df.columns:
        print(f"CSV does not contain a 'Link' column")
        return
    # from "Link" column
    url_list: list[str] = df['Link'].to_list()
    # process each url asynchronously, to figure out the exact type instead of listing Optional subsequently
    res: list[Optional[str]] = await asyncio.gather(*(process_url(url) for url in url_list))

    # update DataFrame with extracted titles
    df['Name of Applicant'] = res

    df.to_csv("Cardano Funding MasterSheet - data_updated_with_most_names.csv", index=False)
    print(f"CSV updated with author titles")

    # url: str = "https://cardano.ideascale.com/c/cardano/idea/64028"
    # html: str = await fetch_html(url)
    # print(f"Fetched HTML length: {len(html)}")
    # # print(html[:10000])
    #
    # soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    # author_title: Optional[str] = find_author_title(soup)
    # if author_title:
    #     print("Author Title:")
    #     print(author_title)
    # else:
    #     print("Author title not found.")


if __name__ == '__main__':
    asyncio.run(main())

