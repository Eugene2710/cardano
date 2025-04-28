import io
import asyncio
import json
import os
from typing import Any
from dotenv import load_dotenv
from pprint import pprint

from tenacity import retry, wait_fixed, stop_after_attempt
from src.models.blockfrost_models.cardano_block_transactions import CardanoBlockTransactions
from src.models.database_transfer_objects.cardano_block_transactions import CardanoBlocksTransactionsDTO
from src.file_explorer.s3_file_explorer import S3Explorer


class CardanoBlockTransactionsS3Extractor:
    def __init__(
            self,
            s3_explorer: S3Explorer
    ) -> None:
        self._s3_explorer: S3Explorer = s3_explorer

    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def get_block_transactions_from_s3(self, s3_path: str) -> list[CardanoBlocksTransactionsDTO]:
        """
        - get specified json file from s3 using S3Explorer.download_to_buffer
        - add json file to database
        """
        # download JSON file into a BytesIO buffer
        buffer: io.BytesIO = self._s3_explorer.download_to_buffer(s3_path=s3_path)
        # load JSON content into a list of dict
        block_tx_data: list[dict[str, Any]] = json.load(buffer)

        dtos: list[CardanoBlocksTransactionsDTO] = []
        for record in block_tx_data:
            raw_block_tx: CardanoBlockTransactions = CardanoBlockTransactions(**record)
            dto: CardanoBlocksTransactionsDTO = CardanoBlocksTransactionsDTO.from_raw_cardano_blocks_tx(input=raw_block_tx)
            dtos.append(dto)

        return dtos


if __name__ == "__main__":
    load_dotenv()
    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""),
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )
    extractor: CardanoBlockTransactionsS3Extractor = CardanoBlockTransactionsS3Extractor(s3_explorer=s3_explorer)
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    res: list[CardanoBlocksTransactionsDTO] = event_loop.run_until_complete(
        extractor.get_block_transactions_from_s3(s3_path="cardano/block_tx/raw/2000/cardano_blocks_tx_raw2000.json")
    )
    pprint(res)