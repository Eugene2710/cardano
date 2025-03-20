import requests
import os
from dotenv import load_dotenv
"""
- By default, results are returned 100 at a time
    - Use "?page=2" to list through results
- data is being returned in ascending order(oldest first, newest last)
    - Use "?order=desc" to reverse order whenever available
- All time related field, except server_time, are in seconds of UNIX time
- All amounts are returned in Lovelaces, i.e 1 ADA = 1 000 000 Lovelaces
- Addresses, accounts and pool IDs are in Bech 32 format
- All values are case sensitive
- All hex encoded values are lower cased
"""
# get 3 recent blocks, check if they are in consecutive number
# map them into transactions


# what's next: get the unix timestamp of 3 days ago
# ingest them in


load_dotenv()
# change this to the "/blocks/{hash_or_number}" and replace the hash or number with a variable
url: str = "https://cardano-mainnet.blockfrost.io/api/v0/blocks/4865265"

headers = {
    "Project_id": os.getenv("BLOCKFROST_PROJECT_ID")
}

response = requests.get(url, headers=headers)

print(response.json())