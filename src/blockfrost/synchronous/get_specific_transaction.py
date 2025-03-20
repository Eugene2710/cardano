import requests
import os
from dotenv import load_dotenv
from typing import Any
from pprint import pprint


def get_specific_tx(tx_hash: str) -> dict[str, Any]:
    """
    Responsible for getting tx details, but only the total output amount transacted
    source: https://docs.blockfrost.io/#tag/cardano--transactions
    Needs to be paired with get_transaction_utxo to get the input and output addresses
    """
    url: str = f"https://cardano-mainnet.blockfrost.io/api/v0/txs/{tx_hash}"

    headers: dict[str, str] = {
        "Project_id": os.getenv("BLOCKFROST_PROJECT_ID")
    }

    response = requests.get(url, headers=headers)

    return response.json()


if __name__ == "__main__":
    load_dotenv()
    # tested tx_hash: "f11922f09b7d282a4b368c5bb66cee3c98d75d584783b0252f3074c26befaa52" -> from block 4865265
    # tested tx_hash: "8788591983aa73981fc92d6cddbbe643959f5a784e84b8bee0db15823f575a5b",
    tx_hash: str = "8788591983aa73981fc92d6cddbbe643959f5a784e84b8bee0db15823f575a5b"
    tx_details: dict[str, Any] = get_specific_tx(tx_hash=tx_hash)
    pprint(tx_details)