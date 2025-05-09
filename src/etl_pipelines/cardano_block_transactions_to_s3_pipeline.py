import io
from typing import Any
import json
import os
from asyncio import AbstractEventLoop, new_event_loop
from datetime import datetime
import boto3
from dotenv import load_dotenv

from src.extractors.get_block_transactions import CardanoBlockTransactionsExtractor
from src.models.blockfrost_models.cardano_block_transactions import CardanoBlockTransactions
from src.dao.provider_to_s3_import_status_dao import ProviderToS3ImportStatusDAO
from src.dao.s3_to_db_import_status_dao import S3ToDbImportStatusDAO
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.database_transfer_objects.provider_to_s3_import_status import ProviderToS3ImportStatusDTO


class CardanoBlockTransactionsToETLPipeline:
    """
    Responsible for:
    - checking if last block height from block_transactions in database < latest block in provider_to_s3_import_status_table with cardano_block_transactions column vs cardano_block_column
    - extracting raw cardano block transactions from Blockfrost in batches of 2000
    - convert list of extracted block transactions data (dict type) to json -> bytes
    - upload extracted data to S3 in json format
    - update provider_to_s3 import_status table with latest block_number for columns with cardano_block_transactions
    """
    def __init__(
        self,
        provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO,
        s3_to_db_import_status_dao: S3ToDbImportStatusDAO,
        table: str,
        s3_explorer: S3Explorer,
        extractor: CardanoBlockTransactionsExtractor
    ) -> None:
        self._provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = provider_to_s3_import_status_dao
        self._s3_to_db_import_status_dao: S3ToDbImportStatusDAO = s3_to_db_import_status_dao
        self._table: str = table
        self._s3_explorer: S3Explorer = s3_explorer
        self._extractor: CardanoBlockTransactionsExtractor = extractor

    async def run(self) -> None:
        """
        To check which block to start ingesting from:
        - compare last block_height of cardano_blocks column of provider_to_s3_import_status
          VS last block_height of cardano_block_transactions column of provider_to_s3_import_status_table from database
        - if cardano_blocks_tx_last_height >= cardano_blocks_last_height: do nothing
        else:
            start_block = provider_to_s3_import_status_table.read_latest_import_status("cardano_block_transactions) + 1
            end_block = provider_to_s3_import_status_table.read_latest_import_status("cardano_blocks)
        """
        blocks_tx_latest_block_height: int | None = (
            await self._provider_to_s3_import_status_dao.read_latest_import_status(
                "cardano_block_transactions"
            )
        )
        print(f"blocks_tx_latest_block_height = {blocks_tx_latest_block_height}")
        blocks_latest_block_height: int | None = (
            await self._provider_to_s3_import_status_dao.read_latest_import_status(
                "cardano_blocks"
            )
        )
        print(f"blocks_latest_block_height = {blocks_latest_block_height}")
        if blocks_tx_latest_block_height and blocks_latest_block_height and blocks_tx_latest_block_height >= blocks_latest_block_height:
            print(f"block transactions in S3 up to date")
            return None

        start_block_height: int = blocks_tx_latest_block_height+1 or 11292700
        print(f"start_block_height = {start_block_height}")
        end_block_height: int = blocks_latest_block_height
        print(f"end block height = {end_block_height}")
        # list to collect all block transactions data into a list of dict
        block_tx_info_list: list[dict[str, Any]] = []

        # chunk all of these into the while loop and limit each json batch file to 2000 blocks
        batch_limit: int = 2000
        curr: int = start_block_height
        while curr <= end_block_height:
            end_batch: int = min(curr+batch_limit-1, end_block_height)
            # fetch and collect up to batch limit blocks
            for height in range(curr, end_batch+1):
                block_tx_info: CardanoBlockTransactions = await self._extractor.get_block_transactions(str(height))
                block_tx_info_list.append(block_tx_info.model_dump())

            # convert the entire list to a JSON string and encode it to bytesIO
            combined_json_bytes = json.dumps(block_tx_info_list).encode('utf-8')
            bytes_io = io.BytesIO(combined_json_bytes)

            self._s3_explorer.upload_buffer(bytes_io, source_path=f"cardano/block_tx/raw/{end_batch}/cardano_blocks_tx_raw{end_batch}.json")

            updated_s3_import_status: ProviderToS3ImportStatusDTO = ProviderToS3ImportStatusDTO(
                table=self._table,
                block_height=end_batch,
                created_at=datetime.utcnow(),
            )
            await self._provider_to_s3_import_status_dao.insert_latest_import_status(
                updated_s3_import_status
            )
            print(f"Uploaded batch ending at block: {end_batch}")
            curr = end_batch+1


def run():
    """
    Responsible for running the S3ETLPipeline
    """
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
    provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = ProviderToS3ImportStatusDAO(
        os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    s3_to_db_import_status_dao: S3ToDbImportStatusDAO = S3ToDbImportStatusDAO(
        os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    extractor: CardanoBlockTransactionsExtractor = CardanoBlockTransactionsExtractor()
    cardano_block_tx_to_s3_etl_pipeline: CardanoBlockTransactionsToETLPipeline = CardanoBlockTransactionsToETLPipeline(
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        table="cardano_block_transactions",
        s3_explorer=s3_explorer,
        extractor=extractor
    )
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(cardano_block_tx_to_s3_etl_pipeline.run())


if __name__ == "__main__":
    run()