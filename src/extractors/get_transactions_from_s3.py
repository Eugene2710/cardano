import io
import asyncio
import json
import os
from typing import Any
import boto3
from dotenv import load_dotenv
from pprint import pprint

from tenacity import retry, wait_fixed, stop_after_attempt
from src.models.blockfrost_models.raw_cardano_transactions import CardanoTransactions
from src.models.database_transfer_objects.cardano_transactions import CardanoTransactionsDTO
from src.models.database_transfer_objects.cardano_transactions_output_amount import CardanoTransactionsOutputAmountDTO
from src.file_explorer.s3_file_explorer import S3Explorer


class CardanoTransactionsS3Extractor:
    def __init__(self, s3_explorer: S3Explorer) -> None:
        self._s3_explorer: S3Explorer = s3_explorer

    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def get_tx_from_s3(self, s3_path: str) -> list[CardanoTransactionsDTO]:
        """
        - get specified json file from s3 using S3Explorer.download_to_buffer
        - add json file to database
        """
        # download JSON file into a BytesIO buffer
        buffer: io.BytesIO = self._s3_explorer.download_to_buffer(s3_path=s3_path)
        # load JSON content into a list of dict
        tx_data: list[dict[str, Any]] = json.load(buffer)

        dtos: list[CardanoTransactionsDTO] = []
        for record in tx_data:
            raw_tx: CardanoTransactions = CardanoTransactions(**record)
            dto: CardanoTransactionsDTO = CardanoTransactionsDTO.from_raw_cardano_tx(hash=raw_tx.hash,input=raw_tx)
            dtos.append(dto)

        return dtos

    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def get_tx_output_amount_from_s3(self, s3_path: str) -> list[CardanoTransactionsOutputAmountDTO]:
        """
        - get specified json file from s3 using S3Explorer.download_to_buffer
        - add json file to database
        """
        # download JSON file into a BytesIO buffer
        buffer: io.BytesIO = self._s3_explorer.download_to_buffer(s3_path=s3_path)
        # load JSON content into a list of dict
        tx_data: list[dict[str, Any]] = json.load(buffer)

        dtos: list[CardanoTransactionsOutputAmountDTO] = []
        for record in tx_data:
            raw_tx: CardanoTransactions = CardanoTransactions(**record)
            dto: CardanoTransactionsOutputAmountDTO = CardanoTransactionsOutputAmountDTO.from_raw_cardano_tx(hash=raw_tx.hash, input=raw_tx)
            dtos.append(dto)

        return dtos


if __name__ == "__main__":
    load_dotenv()
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )
    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""), client=client
    )
    extractor: CardanoTransactionsS3Extractor = CardanoTransactionsS3Extractor(s3_explorer=s3_explorer)
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    tx_res: list[CardanoTransactionsDTO] = event_loop.run_until_complete(
        extractor.get_tx_from_s3(s3_path="cardano/transactions/raw/11292900/cardano_transactions_11292900.json")
    )
    tx_output_amt_res: list[CardanoTransactionsOutputAmountDTO] = event_loop.run_until_complete(
        extractor.get_tx_output_amount_from_s3(s3_path="cardano/transactions/raw/11292900/cardano_transactions_11292900.json")
    )
    pprint(tx_output_amt_res)