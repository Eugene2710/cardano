from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup, Tag

import asyncio
import pandas as pd


def fetch_html_selenium(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        print(f"Invalid URL: {url}. Skipping.")
        return ""

    url = url.strip()
    # confifure selenium for headless chrome
    chrome_options: Options = Options()
    # run chrome without opening a browser window
    chrome_options.add_argument("--headless")
    # initialize chrome driver
    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get(url)

    except Exception as e:
        print(f"Error loading URL {url}: {e}")
        driver.quit()
        return ""
    wait = WebDriverWait(driver, 30)

    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "section#team")))
    except TimeoutException:
        print(f"Timeout waiting for element on {url}. Proceeding with next available HTML")
    page_source: str = driver.page_source
    driver.quit()
    return page_source


async def fetch_html(url: str) -> str:
    """
    Responsible for wrapping Selenium html fetcher in an async fucntion
    """
    loop = asyncio.get_running_loop()
    html: str = await loop.run_in_executor(None, fetch_html_selenium, url)
    return html


def find_team_name(soup: BeautifulSoup) -> list[str]:
    """
    Responsible for extracting the team names
    Case 1: 1 name in team: https://projectcatalyst.io/funds/10/products-and-integrations/open-standard-for-cross-game-achievement-system-to-gamify-onchain-participation
    Case 2: More than 1 name in team: https://projectcatalyst.io/funds/11/cardano-use-cases-solution/cardano-ai-sentiment-tracker
    """
    names: list[str] = []
    team_section: Tag | None = soup.find("section", id="team")
    if team_section:
        proposer_links = team_section.find_all("a", href=lambda h:h and "/proposers/" in h)
        for link in proposer_links:
            name_span: Tag | None = link.find("span", class_="sc-368c58fa-1 jBOfHU")
            if name_span:
                name = name_span.get_text(strip=True)
                if name:
                    names.append(name)

    return names


async def process_url(url: str) -> list[str]:
    """
    Responsible for processing a URL:
    fetch HTML -> parse HTML -> extract author and title
    """
    html: str = await  fetch_html(url)
    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    return find_team_name(soup)


async def main() -> None:
    """
    Case 1: 1 name in team: https://projectcatalyst.io/funds/10/products-and-integrations/open-standard-for-cross-game-achievement-system-to-gamify-onchain-participation
    Case 2: More than 1 name in team: https://projectcatalyst.io/funds/11/cardano-use-cases-solution/cardano-ai-sentiment-tracker
    """
    # url: str = "https://projectcatalyst.io/funds/10/products-and-integrations/open-standard-for-cross-game-achievement-system-to-gamify-onchain-participation"
    # names: list[str] = await process_url(url=url)
    # print(f"Extracted Team Member Names: {names}")
    df = pd.read_csv("/Users/eugeneleejunping/Documents/cardano_grants/project_catalyst/catalyst_before_extracting_team_names.csv")
    if "Link" not in df.columns:
        print(f"CSV does not contain a 'Link' column")
        return
    url_list: list[str] = df["Link"].to_list()

    res: list[list[str]] = await asyncio.gather(*(process_url(url=url) for url in url_list))

    # define max number of names
    max_names: int = 10
    # build a list lists where each inner list has exactly max_names items

    names_matrix = [
        (names if len(names) >= max_names else names + [""] * (max_names - len(names)))[:max_names]
        for names in res
    ]

    # create a new DataFrame for the matrix
    name_cols = [f"Name {i+1}" for i in range(max_names)]
    names_df: pd.DataFrame = pd.DataFrame(names_matrix, columns=name_cols)

    # concat the new columns with the original DataFrame
    df = pd.concat([df, names_df], axis=1)

    output_csv = "catalyst_after_extracting_team_names.csv"
    df.to_csv(output_csv, index=False)
    print(f"CSV updated with team names. Output saved.")


if __name__ == "__main__":
    asyncio.run(main())
