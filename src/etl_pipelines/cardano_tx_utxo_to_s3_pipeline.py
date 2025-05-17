import io
from typing import Any
import json
import os
from asyncio import AbstractEventLoop, new_event_loop
from datetime import datetime

import boto3
from dotenv import load_dotenv
from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import  AsyncEngine, create_async_engine

from src.extractors.get_tx_utxo import CardanoTxUtxoExtractor
from src.models.blockfrost_models.cardano_transaction_utxo import TransactionUTxO
from src.dao.provider_to_s3_import_status_dao import ProviderToS3ImportStatusDAO
from src.dao.s3_to_db_import_status_dao import S3ToDbImportStatusDAO
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.database_transfer_objects.provider_to_s3_import_status import ProviderToS3ImportStatusDTO
from database_management.cardano.cardano_tables import cardano_transactions_table


class CardanoTxUtxoToETLPipeline:
    """
    Responsible for:
    - checking if last transaction hash from tx_utxo in database
    != to latest tx hash in transaction column of 'table' in provider_to_s3_import_status_table
    - extracting raw cardano transaction utxo from Blockfrost in batches of 200
    - convert list of extracted transaction utxo data to json -> bytesIO
    - upload extracted data to s3 in json bytesio format
    - update provider_to_s3 import_status table with latest block_number for columns with cardano_block_transactions
    """
    def __init__(
            self,
            provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO,
            s3_to_db_import_status_dao: S3ToDbImportStatusDAO,
            table: str,
            s3_explorer: S3Explorer,
            extractor: CardanoTxUtxoExtractor
    ) -> None:
        self._provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = provider_to_s3_import_status_dao
        self._s3_to_db_import_status_dao: S3ToDbImportStatusDAO = s3_to_db_import_status_dao
        self._table: str = table
        self._s3_explorer: S3Explorer = s3_explorer
        self._extractor: CardanoTxUtxoExtractor = extractor
        self._engine: AsyncEngine = create_async_engine(
            os.getenv("ASYNC_PG_CONNECTION_STRING", "")
        )

    async def run(self) -> None:
        """
        To check which block of transactions to start ingesting from:
        - compare last block height of cardano_transactions column of provider_to_s3_import_status
        VS last block_height of cardano_transactions_utxo column of provider_to_s3_import_status_table from database
        - if cardano_tx_utxo_last_height >= cardano_tx_last_height: do nothing
        else:
            start_block = provider_to_s3_import_status_table.read_latest_import_status("cardano_transactions_utxo) + 1
            end_block = provider_to_s3_import_status_table.read_latest_import_status("cardano_transactions)
        """
        tx_utxo_latest_block_height: int | None = (
            await self._provider_to_s3_import_status_dao.read_latest_import_status(
                "cardano_transactions_utxo"
            )
        )
        print(f"tx_utxo_latest_block_height = {tx_utxo_latest_block_height}")
        tx_latest_block_height: int | None = (
            await self._provider_to_s3_import_status_dao.read_latest_import_status(
                "cardano_transactions"
            )
        )
        print(f"blocks_latest_block_height = {tx_latest_block_height}")
        if tx_utxo_latest_block_height and tx_latest_block_height and tx_utxo_latest_block_height >= tx_latest_block_height:
            print(f"transactions utxo in S3 up to date")
            return None

        start_block_height: int = tx_utxo_latest_block_height+1
        # start_block_height: int = 11292700
        start_block_height: int = 11300208
        print(f"start_block_height = {start_block_height}")
        # end_block_height: int = tx_latest_block_height
        end_block_height: int = 11302209
        print(f"end block height = {end_block_height}")
        # list to collect all transactions utxo data into a list of dict
        tx_utxo_info_list: list[TransactionUTxO] = []

        # chunk all of these into the while loop and limit each json batch file to 200 blocks
        batch_limit: int = 1000
        curr: int = start_block_height
        while curr <= end_block_height:
            end_batch: int = min(curr+batch_limit-1, end_block_height)

            block_ids: list[int] = [i for i in range(curr, end_batch+1)]

            # query database for transaction hash from cardano transactions
            stmt: Select = (
                select(cardano_transactions_table.c.hash)
                .where(cardano_transactions_table.c.block_height.in_(block_ids))
            )

            async with self._engine.begin() as conn:
                result = await conn.execute(stmt)
                rows: list[list[str]] = result.scalars().all()
                print(f"Rows: {rows}")

            # fetch and collect up to batch limit blocks
            # for hashes_in_block in rows:
            for tx_hash in rows:
                tx_utxo_info: TransactionUTxO = await self._extractor.get_tx_utxo(tx_hash=tx_hash)
                if tx_utxo_info is None:
                    continue
                tx_utxo_info_list.append(tx_utxo_info.model_dump())

            curr = end_batch+1

        # convert entire list to a json string and encode it to a bytesIO
        combined_json_bytes = json.dumps(tx_utxo_info_list).encode('utf-8')
        bytes_io = io.BytesIO(combined_json_bytes)

        self._s3_explorer.upload_buffer(bytes_io, source_path=f"cardano/transaction_utxo/raw/{end_batch}/cardano_tx_utxo_{end_batch}.json")

        updated_s3_import_status: ProviderToS3ImportStatusDTO = ProviderToS3ImportStatusDTO(
            table=self._table,
            block_height=end_batch,
            created_at=datetime.utcnow()
        )
        await self._provider_to_s3_import_status_dao.insert_latest_import_status(
            updated_s3_import_status
        )
        print(f"Uploaded batch ending at block: {end_batch}")


def run():
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
    extractor: CardanoTxUtxoExtractor = CardanoTxUtxoExtractor()
    cardano_tx_utxo_to_s3_etl_pipeline: CardanoTxUtxoToETLPipeline = CardanoTxUtxoToETLPipeline(
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        table="cardano_transactions_utxo",
        s3_explorer=s3_explorer,
        extractor=extractor
    )
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(cardano_tx_utxo_to_s3_etl_pipeline.run())


if __name__ == "__main__":
    run()