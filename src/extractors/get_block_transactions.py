import asyncio
import aiohttp
from pprint import pprint
import os
from dotenv import load_dotenv

from tenacity import retry, wait_fixed, stop_after_attempt
from src.models.blockfrost_models.cardano_block_transactions import CardanoBlockTransactions


class CardanoBlockTransactionsExtractor:
    @staticmethod
    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def get_block_transactions(block_height: str) -> CardanoBlockTransactions:
        """
        Responsible for getting transaction hashes by parsing in the block number
        Blockfrost allows block hash to be passed instead of block number, but we will be sticking to only block number
        """
        url: str = f"https://cardano-mainnet.blockfrost.io/api/v0/blocks/{block_number}/txs"

        headers: dict[str:str] = {"Project_id": os.getenv("BLOCKFROST_PROJECT_ID")}

        async with aiohttp.ClientSession() as client:
            async with client.get(url=url, headers=headers) as response:
                if response.status == 200:
                    data: list[str] = await response.json()
                    cardano_block_transaction: CardanoBlockTransactions = CardanoBlockTransactions.from_json(data)
                    return cardano_block_transaction

                else:
                    raise Exception(f"Received non-status code 200: {response.status}")


if __name__ == "__main__":
    load_dotenv()
    block_number: str = "4873401"
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    result: CardanoBlockTransactions = event_loop.run_until_complete(
        CardanoBlockTransactionsExtractor.get_block_transactions(block_height=block_number)
    )
    pprint(result)

