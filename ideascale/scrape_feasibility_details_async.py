import asyncio
from typing import Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup, Tag


def fetch_html_selenium(url: str) -> str:
    # configure selenium for headless chrome
    chrome_options: Options = Options()
    chrome_options.add_argument("--headless")
    # intialize chrome driver (ensure chromedriver is in PATH)
    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.get(url)
        # use explicit wait to wait for the target element to appear
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.ID, "custom-field-section-4027")))
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


def find_feasibility_details(soup: BeautifulSoup) -> Optional[str]:
    """
    Find and extract text details from the feasibility section
    Look for <dl> element with id 'custom-field-section-4027 and retrieve text from nested <span>
    """
    # Locate the <dl> element with the specific id for the feasibility details section.
    dl_element: Optional[Tag] = soup.find('dl', id='custom-field-section-4027')
    if dl_element:
        print("dl_exists")
        # Within the <dl>, find the descendant <span> that holds the HTML preview content.
        span_element: Optional[Tag] = dl_element.find('span', class_='ql-editor ql-render')
        if span_element:
            # Extract all the <p> tags inside the span and join their text content.
            paragraphs = span_element.find_all('p')
            details_text: str = "\n".join(p.get_text(strip=True) for p in paragraphs)
            return details_text
    return None


async def main() -> None:
    url: str = "https://cardano.ideascale.com/c/cardano/idea/64028"
    html: str = await fetch_html(url)
    # print(f"Fetched HTML length: {len(html)}")
    # print(html[:10000])

    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')

    details: Optional[str] = find_feasibility_details(soup)
    if details:
        print("Feasibility Details:")
        print(details)
    else:
        print("Feasibility details not found.")


if __name__ == '__main__':
    asyncio.run(main())
