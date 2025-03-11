import requests
from bs4 import BeautifulSoup

# to include a list of url to scrape from subsequently
url: str = "https://cardano.ideascale.com/c/cardano/idea/64028"

response: requests = requests.get(url)

soup: BeautifulSoup = BeautifulSoup(response.text, 'html.parser')

page_
