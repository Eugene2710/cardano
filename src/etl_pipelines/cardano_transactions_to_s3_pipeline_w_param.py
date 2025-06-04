import io
import json
import os
from asyncio import AbstractEventLoop, new_event_loop
from datetime import datetime
import boto3
from dotenv import load_dotenv
from sqlalchemy import select, Select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
import click

from src.extractors.get_transactions import CardanoTransactionsExtractor
from src.models.blockfrost_models.raw_cardano_transactions import CardanoTransactions
from src.dao.provider_to_s3_import_status_dao import ProviderToS3ImportStatusDAO
from src.dao.s3_to_db_import_status_dao import S3ToDbImportStatusDAO
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.database_transfer_objects.provider_to_s3_import_status import ProviderToS3ImportStatusDTO
from database_management.cardano.cardano_tables import cardano_block_transactions_table


class CardanoTransactionsTOETLPipeline:
    """
    Responsible for:
    - checking if last block height from transactions in database < latest block in provider_to_s3_import_table with
    cardano_transactions column vs cardano_block_transactions column
    - extracting raw cardano transactions from Blockfrost in batches of 1000 blocks worth of transactions
    - convert list of extracted transactions to json -> bytes
    - upload extracted data to S3 in json format
    - update provider_to_s3_import_status table with latest block number for columns with cardano_transactions
    """
    def __init__(
            self,
            provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO,
            s3_to_db_import_status_dao: S3ToDbImportStatusDAO,
            table: str,
            s3_explorer: S3Explorer,
            extractor: CardanoTransactionsExtractor
    ) -> None:
        self._provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = provider_to_s3_import_status_dao
        self._s3_to_db_import_status_dao: S3ToDbImportStatusDAO = s3_to_db_import_status_dao
        self._table: str = table
        self._s3_explorer: S3Explorer = s3_explorer
        self._extractor: CardanoTransactionsExtractor = extractor
        self._engine: AsyncEngine = create_async_engine(
            os.getenv("ASYNC_PG_CONNECTION_STRING", "")
        )

    async def run(self, start_block_height: int, end_block_height: int) -> None:
        """
        To check which block of transactions to start ingesting from:
        - compare last block_height of cardano_block_transactions column of provider_to_s3_import_status
        VS last block_height of cardano_transactions column of provider_to_s3_import_status
        - if cardano_transactions_last_height >= cardano_blocks_transactions_last_height: do nothing
        else
            start_block = provider_to_s3_import_status_table.read_latest_import_status("cardano_transactions") + 1
            end_block = provider_to_s3_import_status_table.read_latest_import_status("cardano_blocks_transactions)
        """
        tx_latest_block_height: int | None = (
            await self._provider_to_s3_import_status_dao.read_latest_import_status(
                "cardano_transactions"
            )
        )
        print(f"tx_latest_block_height = {tx_latest_block_height}")
        blocks_tx_latest_block_height: int | None = (
            await self._provider_to_s3_import_status_dao.read_latest_import_status(
                "cardano_block_transactions"
            )
        )
        print(f"blocks_tx_latest_block_height = {blocks_tx_latest_block_height}")
        if tx_latest_block_height and blocks_tx_latest_block_height and tx_latest_block_height >= blocks_tx_latest_block_height:
            print(f"transactions in S3 up to date")
            return None

        print(f"start_block_height = {start_block_height}")
        print(f"end block height = {end_block_height}")
        # list to collect all  transactions data into a list of dict
        tx_info_list: list[CardanoTransactions] = []

        # chunk all of these into the while loop and limit each json batch file to 1000 blocks
        batch_limit: int = 6
        while start_block_height <= end_block_height:
            end_batch: int = min(start_block_height+batch_limit-1, end_block_height)
            block_ids: list[str] = [str(i) for i in range(start_block_height, end_batch+1)]
            # query database for the transaction hash from cardano_block_transactions table
            stmt: Select = (
                select(cardano_block_transactions_table.c.tx_hash)
                .where(cardano_block_transactions_table.c.block.in_(block_ids))
            )
            async with self._engine.begin() as conn:
                result = await conn.execute(stmt)
                # results.scalar() yields each tx_hash column value
                rows: list[list[str]] = result.scalars().all()

            # iterate and call extractor for each hash at a time
            for hashes_in_block in rows:
                for tx_hash in hashes_in_block:
                    tx_info: CardanoTransactions = await self._extractor.get_transaction(tx_hash=tx_hash)
                    tx_info_list.append(tx_info.model_dump())

            start_block_height = end_batch+1

        # convert entire list to a JSON string and encode it to a bytesIO
        combined_json_bytes = json.dumps(tx_info_list).encode('utf-8')
        bytes_io = io.BytesIO(combined_json_bytes)

        self._s3_explorer.upload_buffer(bytes_io, source_path=f"cardano/transactions/raw/{end_batch}/cardano_transactions_{end_batch}.json")

        updated_s3_import_status: ProviderToS3ImportStatusDTO = ProviderToS3ImportStatusDTO(
            table=self._table,
            block_height=end_batch,
            created_at=datetime.utcnow()
        )
        await self._provider_to_s3_import_status_dao.insert_latest_import_status(
            updated_s3_import_status
        )
        print(f"Uploaded batch ending at block: {end_batch}")


@click.command()
@click.option("--tx_start-block", type=int, required=True, help="Start block height for ingestion od cardano tx")
@click.option("--tx_end-block", type=int, required=True, help="Start block height for ingestion od cardano tx")
def run(tx_start_block: int, tx_end_block: int):
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
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    extractor: CardanoTransactionsExtractor = CardanoTransactionsExtractor()
    cardano_tx_to_s3_etl_pipeline: CardanoTransactionsTOETLPipeline = CardanoTransactionsTOETLPipeline(
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        table="cardano_transactions",
        s3_explorer=s3_explorer,
        extractor=extractor
    )
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(cardano_tx_to_s3_etl_pipeline.run(start_block_height=tx_start_block, end_block_height=tx_end_block))


if __name__ == "__main__":
    run()