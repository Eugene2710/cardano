import asyncio
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup, Tag
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


def fetch_html_selenium(url: str) -> str:
    """
    Responsible for:
    - checking if url is of type string and stripping the url
    - starting Selenium's chrome driver
    - render the page and capture specified html element
    - quit chrome driver
    """
    if not isinstance(url, str):
        print(f"Invalid URL: {url}. Skipping.")
        return ""
    # strip url
    url = url.strip()
    # configure selenium for headless chrom
    chrome_options: Options = Options()
    chrome_options.add_argument("--headless")
    # intialize chrome driver
    driver = webdriver.Chrome(options=chrome_options)
    try:
        try:
            driver.get(url)
        except Exception as e:
            print(f"Error loading URL {url}: {e}")
            return ""
        wait = WebDriverWait(driver, 30)
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a.unfurl-url.classic-link")
            ))
        except TimeoutException:
            print(f"Timeout waiting for company link element on {url}. Proceeding with next available HTML")
        return driver.page_source
    finally:
        driver.quit()


async def fetch_html(url: str) -> str:
    """
    Responsible for wrapping fetch_selenium_html function into an async fucntion
    """
    loop = asyncio.get_running_loop()
    html: str = await loop.run_in_executor(None, fetch_html_selenium, url)
    return html


def find_company_link(soup: BeautifulSoup) -> str | None:
    """
    Responsible for querying and extracting the company link and returning it as a string
    """
    tag: Tag | None = soup.find('a', class_="unfurl-url classic-link")
    if tag:
        return tag.get("href") or tag.get_text(strip=True)
    return None


async def process_url(url: str) -> str | None:
    """
    Responsible for processing a URL:
    fetch HTML -> parse HTML -> extract author title
    """
    html: str = await fetch_html(url)
    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    return find_company_link(soup)


async def main() -> None:
    # to test for single URL before fetching all URL from CSV
    # url: str = "https://cardano.ideascale.com/a/dtd/417270-48088"
    # res: str = await process_url(url)
    # print(res)
    df = pd.read_csv("/Users/eugeneleejunping/Documents/cardano_grants/ideascale/Cardano Funding MasterSheet - data_updated_with_some_names - project_catalyst_test_updated - ideascale_before_company_link_scraped.csv")
    if "Company Link" not in df.columns:
        print(f"CSV does not contain a 'Company Link' column")
        return
    url_list: list[str] = df['Link'].to_list()
    res: list[str] = await asyncio.gather(*(process_url(url) for url in url_list))

    df['Company Link'] = res

    df.to_csv("company_links_scraped_ideascale.csv")
    print("company links from ideascale scraped")


if __name__ == "__main__":
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())