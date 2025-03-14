import asyncio
from typing import Optional
import pandas as pd

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
    - checking if input url of type string and cleaning the input url
    - intialilising selenium.webdriver to complete the rendering until either
     a.author-name.text-truncate" or "div.member-list-dropdown" is found
    - tearing down selenium.webdriver
    """
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
        wait = WebDriverWait(driver, 45)
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.author-name.text-truncate, div.member-list-dropdown")))
        except TimeoutException:
            print(f"Timeout waiting for element on {url}. Proceeding with available HTML.")
        return driver.page_source
    finally:
        driver.quit()


async def fetch_html(url: str) -> str:
    """
    Responsible for wrap Selenium html fetcher in an async function
    """
    loop = asyncio.get_running_loop()
    html: str = await loop.run_in_executor(None, fetch_html_selenium, url)
    return html



def find_author_title(soup: BeautifulSoup) -> Optional[str]:
    """
    Responsible for extracting the applicant name for 2 possible cases:
      1. Single author/name: Look for an <a> element with class "author-name text-truncate"
      2. Multiple authors/names: If not found, search for a dropdown container (with class "member-list-dropdown")
         and then return the text inside the first <strong> tag within the first <a> element
    """
    # Case 1: Single author element
    author_tag: Tag | None = soup.find('a', class_="author-name text-truncate")
    if author_tag:
        return author_tag.get("title") or author_tag.get_text(strip=True)

    # Case 2: Multiple authors
    # Look for a container that holds multiple names by useing the dropdown container
    dropdown_container: Tag | None = soup.find('div', class_="member-list-dropdown")
    if dropdown_container:
        first_author_link: Tag | None = dropdown_container.find('a')
        if first_author_link:
            # Look for a <strong> tag inside the first author link
            strong_tag: Tag | None = first_author_link.find('strong')
            if strong_tag:
                return strong_tag.get_text(strip=True)
            else:
                # Fallback: if there's no <strong> tag, return the text of the link
                return first_author_link.get_text(strip=True)
    return None



async def process_url(url: str) -> str | None:
    """
    Responsible for processing a URL:
    fetch HTML -> parse HTML -> extract author and title
    """
    html: str = await fetch_html(url)
    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    return find_author_title(soup)


async def main() -> None:
    """
    This portion is a local test to test the 2 types of urls: single autohor, multiple authors:
    single profile/name: https://cardano.ideascale.com/a/dtd/422389-48088 -> expected result: "LoxeInc"
    multiple profiles/names:
    https://cardano.ideascale.com/c/cardano/idea/64371 -> expected result: "
    https://cardano.ideascale.com/a/dtd/414088-48088

    Possible TODO: add a more detailed integration test to test this portion
    """
    # url: str = "https://cardano.ideascale.com/a/dtd/422389-48088"
    # res: str = await process_url(url)
    # print(res)
    """
    -- end of local test portion --
    """
    # read csv
    df = pd.read_csv("/Users/eugeneleejunping/Documents/cardano_grants/ideascale/ideascale_before_multiple_name_extraction.csv")
    if "Link" not in df.columns:
        print(f"CSV does not contain a 'Link' column")
        return
    # from "Link" column
    url_list: list[str] = df['Link'].to_list()
    # process each url asynchronously
    res: list[str] = await asyncio.gather(*(process_url(url) for url in url_list))

    # update DataFrame with extracted titles
    df['Name of Applicant'] = res

    df.to_csv("ideascale_after_multiple_name_extraction.csv", index=False)
    print(f"CSV updated with author titles")


if __name__ == '__main__':
    asyncio.run(main())

