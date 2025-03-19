import asyncio
import aiohttp
from pprint import pprint
import os
from typing import Any
from dotenv import load_dotenv

from tenacity import retry, wait_fixed, stop_after_attempt
from src.models.blockfrost_models.cardano_blocks import BlockfrostCardanoBlockInfo


@retry(
    wait=wait_fixed(0.01),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def get_block(block_number: str) -> BlockfrostCardanoBlockInfo:
    """
    note that Blockfrost allows block_hash to be passed instead of block_number as well
    but we will go with block number
    """
    url: str = f"https://cardano-mainnet.blockfrost.io/api/v0/blocks/{block_number}"

    headers: dict[str, str] = {
        "Project_id": os.getenv("BLOCKFROST_PROJECT_ID")
    }

    async with aiohttp.ClientSession() as client:
        async with client.get(url=url, headers=headers) as response:
            if response.status == 200:
                data: dict[str, Any] = await response.json()
                cardano_block_info: BlockfrostCardanoBlockInfo = BlockfrostCardanoBlockInfo.model_validate(data)
                return cardano_block_info

            else:
                raise Exception(f"Received non-status code 200: {response.status}")


if __name__ == "__main__":
    load_dotenv()
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    result: BlockfrostCardanoBlockInfo = event_loop.run_until_complete(get_block("4865265"))
    pprint(result)