import io
from typing import Any
import json
import os
from asyncio import AbstractEventLoop, new_event_loop
from datetime import datetime

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
    - checking if last block height from block_transactions table in database < latest block in provider_to_s3_import_status_table with cardano_block_transactions column
    - extracting raw cardano block transactions from Blockfrost in batches of 1000
    - convert list of extracted block transactions data (dict type) to json -> bytes
    - upload extracted data to S3 in json format
    - udpate provider_to_s3 import_status table with latest block_number for columns with cardano_block_transactions
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
        - compare last block_height of cardano_blocks column of s3_to_db_import_status_table VS last block_height of cardano_block_transactions provider_to_s3_import_status_table from database
        if cardano_blocks_tx_last_height >= cardano_blocks_last_height: do nothing
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
            await self._s3_to_db_import_status_dao.read_latest_import_status(
                "cardano_blocks"
            )
        )
        print(f"blocks_latest_block_height = {blocks_latest_block_height}")
        if blocks_tx_latest_block_height >= blocks_latest_block_height:
            print(f"block transactions in S3 up to date")
            return None

        # num_of_blocks: int = 500
        # start_block_height: int = (latest_block_height + 1) or 11292700

        start_block_height: int = blocks_tx_latest_block_height or 1
        print(f"start_block_height = {start_block_height}")
        end_block_height: int = blocks_latest_block_height
        print(f"end block height = {end_block_height}")
        curr_block_height: int = start_block_height
        # list to collect all block transactions data into a list of dict
        block_tx_info_list: list[dict[str, Any]] = []
        print(block_tx_info_list)
        while curr_block_height <= end_block_height:
            block_tx_info: CardanoBlockTransactions = await self._extractor.get_block_transactions(str(curr_block_height))
            block_tx_info_list.append(block_tx_info.model_dump())
            curr_block_height += 1

        # convert the entire list to a JSON string and encode it to bytesIO
        combined_json_bytes = json.dumps(block_tx_info_list).encode('utf-8')
        bytes_io = io.BytesIO(combined_json_bytes)

        self._s3_explorer.upload_buffer(bytes_io, source_path=f"cardano/block_transactions/{end_block_height}/cardano_blocks_tx{end_block_height}.json")

        updated_s3_import_status: ProviderToS3ImportStatusDTO = ProviderToS3ImportStatusDTO(
            table=self._table,
            block_height=end_block_height,
            created_at=datetime.utcnow(),
        )
        await self._provider_to_s3_import_status_dao.insert_latest_import_status(
            updated_s3_import_status
        )


def run():
    """
    Responsible for running the S3ETLPipeline
    """
    load_dotenv()
    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""),
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
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