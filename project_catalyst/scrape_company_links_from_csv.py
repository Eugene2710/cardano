import asyncio
import pandas as pd
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup, Tag


def fetch_html_selenium(url: str) -> str:
    """
    Responsible for:
    - checking if input utl is of type string
    - initialising selenium.webdriver to execute JS to load dynamic contents
    - check for presence of element class, "a.sc-ffb617e7-0.fTVUpX"
    """
    # check if input url is of type string
    if not isinstance(url, str) or not url.strip():
        print(f"Invalid URL: {url}. Skipping.")
        return ""
    # clean the url
    url = url.strip()
    if not url:
        print(f"Empty url, skipping.")
        return ""
    # configure selenium for headless chrome
    chrome_options: Options = Options()
    # run chrome without opening a browser window
    chrome_options.add_argument("--headless")
    # initialise chrome driver
    driver = webdriver.Chrome(options=chrome_options)
    try:
        try:
            driver.get(url)
        except Exception as e:
            print(f"Error loading url {url}: {e}")
            return ""
        wait = WebDriverWait(driver, 30)
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a.sc-ffb617e7-0.fTVUpX")
            ))
        except TimeoutException:
            print(f"Timeout waiting for website/company link on {url}. Proceeding with next available HTML.")
        return driver.page_source
    finally:
        driver.quit()


async def fetch_html(url: str) -> str:
    """
    Responsible for wrapping fetch_html_selenium into an async function
    """
    event_loop = asyncio.get_running_loop()
    html: str = await event_loop.run_in_executor(None, fetch_html_selenium, url)
    return html


def find_website_link(soup: BeautifulSoup) -> Optional[str]:
    """
    Responsible for finding the link from the html elements
    """
    tag: Optional[Tag] = soup.find('a', class_="sc-ffb617e7-0 fTVUpX")
    if tag:
        return tag.get("href") or tag.get_text(strip=True)
    return None


async def process_url(url:str) -> Optional[str]:
    """
    Responsible for processing a url:
    fetch HTML -> parase HTML -> extract link
    """
    html: str = await fetch_html(url)
    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    return find_website_link(soup)


async def main() -> None:
    df = pd.read_csv("/Users/eugeneleejunping/Documents/cardano_grants/project_catalyst/to_extract_links.csv")
    if "Link" not in df.columns:
        print(f"CSV does not contain a 'Link' column")
        return
    url_list: list[str] = df['Link'].to_list()
    res: list[Optional[str]] = await asyncio.gather(*(process_url(url) for url in url_list))

    df['Company Link'] = res

    df.to_csv("to_extract_links_scraped.csv")
    print(f"CSV updated with company links from project catalyst site")


if __name__ == "__main__":
    asyncio.run(main())

