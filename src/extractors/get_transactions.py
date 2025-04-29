import asyncio
import aiohttp
from pprint import pprint
import os
from typing import Any
from dotenv import load_dotenv

from tenacity import retry, wait_fixed, stop_after_attempt
from src.models.blockfrost_models.raw_cardano_transactions import CardanoTransactions


class CardanoTransactionsExtractor:
    @staticmethod
    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def get_transaction(tx_hash: str) -> CardanoTransactions:
        """
        Responsible for getting tx details, but only the total output amount transacted
        source: https://docs.blockfrost.io/#tag/cardano--transactions
        Needs to be paired with get_transaction_utxo to get the input and output addresses
        """
        url: str = f"https://cardano-mainnet.blockfrost.io/api/v0/txs/{tx_hash}"

        headers: dict[str, str] = {"Project_id": os.getenv("BLOCKFROST_PROJECT_ID")}

        async with aiohttp.ClientSession() as client:
            async with client.get(url=url, headers=headers) as response:
                if response.status == 200:
                    data: dict[str, Any] = await response.json()
                    cardano_transaction_info: CardanoTransactions = CardanoTransactions.model_validate(data)
                    return cardano_transaction_info
                else:
                    raise Exception(f"Received non-status code 200: {response.status}")


if __name__ == "__main__":
    load_dotenv()
    # sample tx_hash= "f11922f09b7d282a4b368c5bb66cee3c98d75d584783b0252f3074c26befaa52"
    # "8788591983aa73981fc92d6cddbbe643959f5a784e84b8bee0db15823f575a5b"
    tx_hash: str = "f11922f09b7d282a4b368c5bb66cee3c98d75d584783b0252f3074c26befaa52"
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    tx_details: CardanoTransactions = event_loop.run_until_complete(
        CardanoTransactionsExtractor.get_transaction(tx_hash=tx_hash)
    )
    pprint(tx_details)