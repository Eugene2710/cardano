import requests
import os
from dotenv import load_dotenv
from pprint import pprint


def get_tx_hash_from_block_num_or_hash(block_num_or_hash: str) -> list[str]:
    """
    Responsible to getting the list of transactions by passing in a block number ot hash
    source: https://docs.blockfrost.io/#tag/cardano--blocks/GET/blocks/{hash_or_number}/txs
    """
    url: str = f"https://cardano-mainnet.blockfrost.io/api/v0/blocks/{block_num_or_hash}/txs"

    headers: dict[str: str] = {
        "Project_id": os.getenv("BLOCKFROST_PROJECT_ID")
    }

    response = requests.get(url, headers=headers)

    return response.json()


if __name__ == "__main__":
    load_dotenv()
    # tested block numbers: 4873401, 4865265
    block_num: str = "4873401"
    txs: list[str] = get_tx_hash_from_block_num_or_hash(block_num_or_hash=block_num)
    pprint(txs)

