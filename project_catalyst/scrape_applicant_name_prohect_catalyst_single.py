import asyncio
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
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
        wait = WebDriverWait(driver, 30)
        try:
            wait.until(EC.presence_of_element_located(
                (By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '[general] name and surname of main applicant')]")
            ))
        except TimeoutException:
            print(f"Timeout waiting for applicant name element on {url}. Proceeding with available HTML.")
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
    h3_tag: Optional[Tag] = soup.find('h3', string=lambda s: s and "[general] name and surname of main applicant" in s.lower())
    if h3_tag:
        # the applicant name is expected to be in the next sibling text node
        next_node = h3_tag.next_sibling
        if next_node:
            name = next_node.strip()
            return name if name else None


async def process_url(url: str) -> Optional[str]:
    """
    Responsible for processing a URL:
    fetch HTML -> parse HTML -> extract author title
    """
    html: str = await fetch_html(url)
    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    return find_author_title(soup)


async def main() -> None:
    url: str = "https://projectcatalyst.io/funds/10/products-and-integrations/open-standard-for-cross-game-achievement-system-to-gamify-onchain-participation"
    html: str = await fetch_html(url)
    print(html)

    res: Optional[str] = await process_url(url)
    print(res)


    # if "Link" not in df.columns:
    #     print(f"CSV does not contain a 'Link' column")
    #     return
    # # from "Link" column
    # url_list: list[str] = df['Link'].to_list()
    # # process each url asynchronously, to figure out the exact type instead of listing Optional subsequently
    # res: list[Optional[str]] = await asyncio.gather(*(process_url(url) for url in url_list))
    #
    # # update DataFrame with extracted titles
    # df['Name of Applicant'] = res
    #
    # df.to_csv("Cardano Funding MasterSheet - data_updated_with_some_names - project_catalyst_test_updated.csv", index=False)
    # print(f"CSV updated with applicant names from Project Catalyst site")

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

