import asyncio
import aiohttp
from pprint import pprint
import os
from typing import Any
from dotenv import load_dotenv

from tenacity import retry, wait_fixed, stop_after_attempt
from src.models.blockfrost_models.cardano_transaction_utxo import TransactionUTxO


class CardanoTxUtxoExtractor:
    @staticmethod
    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def get_tx_utxo(tx_hash: str) -> TransactionUTxO:
        """
        Responsible for getting the input and output addresses and amounts given the input tx_hash
        source: https://docs.blockfrost.io/#tag/cardano--transactions/GET/txs/{hash}/stakes
        """
        url: str = f"https://cardano-mainnet.blockfrost.io/api/v0/txs/{tx_hash}/utxos"

        headers: dict[str, str] = {"Project_id": os.getenv("BLOCKFROST_PROJECT_ID")}

        async with aiohttp.ClientSession() as client:
            async with client.get(url=url, headers=headers) as response:
                if response.status == 200:
                    data: dict[str, Any] = await response.json()
                    cardano_tx_utxo: TransactionUTxO = TransactionUTxO.model_validate(data)
                    return cardano_tx_utxo
                else:
                    raise Exception(f"Received non-status code 200: {response.status}")


if __name__ == "__main__":
    load_dotenv()
    tx_hash: str = "f11922f09b7d282a4b368c5bb66cee3c98d75d584783b0252f3074c26befaa52"
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    tx_utxo: TransactionUTxO = event_loop.run_until_complete(
        CardanoTxUtxoExtractor.get_tx_utxo(tx_hash=tx_hash)
    )
    pprint(tx_utxo)