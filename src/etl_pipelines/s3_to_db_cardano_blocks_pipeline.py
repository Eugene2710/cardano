import io
from datetime import datetime
import os
from dotenv import load_dotenv
from asyncio import AbstractEventLoop, new_event_loop
from typing import Generator

from src.models.file_info.file_info import FileInfo
from src.dao.s3_to_db_import_status_dao import S3ToDbImportStatusDAO
from src.dao.provider_to_s3_import_status_dao import ProviderToS3ImportStatusDAO
from src.dao.cardano_block_dao import CardanoBlockDAO
from src.file_explorer.s3_file_explorer import S3Explorer
from src.extractors.get_block_from_s3 import CardanoBlockS3Extractor
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import select, CursorResult, Sequence, Row, Select, Table
from database_management.cardano.cardano_tables import provider_to_s3_import_status_table, s3_to_db_import_status_table, cardano_block_table
from src.models.database_transfer_objects.cardano_blocks import CardanoBlocksDTO
from src.models.database_transfer_objects.s3_to_db_import_status_dto import S3ToDBImportStatusDTO
from pprint import pprint


class S3ToDBCardanoBlocksETLPipeline:
    """
    Responsible for:
        1) Get latest modified date from s3_import status table
        Use s3_to_db_import_status_dao.read_latest_import_status()
        2) Get all files whose modified date is after s3_import_status modified_date
        Use s3_explorer.list_files()
        3) For each file, save it into DB with DAO.insert_blocks
        provider_to_s3_import_status_table: get block_heights from provider_to_s3_import_status_table
        4) Insert file latest modified date into DB
        CardanoBlockS3Extractor.get_block_from_s3: get block info from S3 + batch the block info into a list of CardanoBlocksDTO
        4) cardano_block_dao.insert_blocks: insert the list of block info into cardano_block table of DB
        5) s3_to_db_import_status_dao.insert_latest_import_status: update s3_to_db_import_status_table - insert the latest block_height, "cardano_block" into table

    - checking if last block height in s3_to_db_import_status_table in DB < provider_to_s3_import_status table DB
    - extract the "transformed" cardano blocks from uploaded json files in S3 in batches of 1000 to cardano_blocks table in DB
    - update s3_to_db_import_status_table in DB with latest block number/height for "cardano_blocks" on 'table' column
    """
    def __init__(
        self,
        s3_to_db_import_status_dao: S3ToDbImportStatusDAO,
        provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO,
        table: str,
        prefix_path: str,
        cardano_block_dao: CardanoBlockDAO,
        s3_explorer: S3Explorer,
    ) -> None:
        self._s3_to_db_import_status_dao: S3ToDbImportStatusDAO = s3_to_db_import_status_dao
        self._table: str = table
        self._s3_prefix_path = prefix_path
        self._provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = provider_to_s3_import_status_dao
        self._s3_to_db_import_status: str = "s3_to_db_import_status"
        self._provider_to_s3_import_status: str = "provider_to_s3_import_status"
        self._s3_explorer: S3Explorer = s3_explorer
        self._engine: AsyncEngine = create_async_engine(
            os.getenv("ASYNC_PG_CONNECTION_STRING", "")
        )
        self._extractor: CardanoBlockS3Extractor = CardanoBlockS3Extractor(s3_explorer=self._s3_explorer)
        self._cardano_block_dao: CardanoBlockDAO = cardano_block_dao

    async def run(self) -> None:
        """
        async version to run the pipeline
        """
        # db_latest_block_height: int | None = (
        #     await self._s3_to_db_import_status_dao.read_latest_import_status(
        #         table="cardano_blocks"
        #     )
        # )
        latest_modified_date: datetime | None = (
            await self._s3_to_db_import_status_dao.read_latest_import_status(
                self._table
            )
        )
        # this is for listing files after import_status_modified_date - for S3 specific
        default_modified_date: datetime = latest_modified_date or datetime(
            year=2020, month=12, day=1
        )

        s3_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_prefix_path, default_modified_date
        )
        # this is for current batch's latest modified date - for batch specific
        current_batch_latest_modified_date: datetime = datetime(
            year=2020, month=12, day=1
        )

        async with self._engine.begin() as conn:
            for file_info in s3_file_info:
                # get file path of each file in S3
                csv_bytes: io.BytesIO = self._s3_explorer.download_to_buffer(
                    file_info.file_path
                )
            # convert file
            await self._cardano_block_dao.insert_blocks(
                async_connection=conn, input=
            )


        # the very last block that is present in  S3
        # s3_last_block_height: int | None = (
        #     await self._provider_to_s3_import_status_dao.read_latest_import_status(
        #         table="cardano_blocks"
        #     )
        # )
        # if db_latest_block_height is not None and db_latest_block_height >= s3_last_block_height:
        # if db_latest_block_height >= s3_last_block_height:
        #     print(f"blocks in db is up to date with s3")
        #     return None
        """
        e.g. 8500 blocks
        refer to the smallest block number in the provider_to_s3_import_status table that is > latest_block_height
        extract in blocks of max 1000 blocks, ideally in blocks between the block_number in provider_to_s3_import_status
        in that row and the next row
        """

        # start_block_height: int = db_latest_block_height + 1 if db_latest_block_height else 0
        #
        # curr_block_height: int = start_block_height
        # get a list of block_numbers to extract till
        # block_number_list: list[int] = []

        """
        example:
        provider_to_s3_import_status_table
        table   |   block_height  |   created_at
        cardano_blocks  |   11293700    |   
        cardano_blocks  |   11294700    |   
        cardano_blocks  |   11295700    |   
        cardano_blocks  |   11295900    |   
        
        s3_to_db_import_status_table
        table   |   block_height  |   created_at
        cardano_blocks  |   11293700    |  
        """
        async with self._engine.begin() as conn:
            stmt: Select = (
                select(provider_to_s3_import_status_table.c.block_height)
                .where(
                    provider_to_s3_import_status_table.c.block_height >= start_block_height,
                    provider_to_s3_import_status_table.c.block_height <= s3_last_block_height
                )
            )
            result: CursorResult = await conn.execute(stmt)
            rows: Sequence[Row] = result.fetchall()

        block_height_list: list[int] = [row[0] for row in rows]
        # pprint(block_height_list)
        # batch all block info into block_info_batch
        all_block_info: list[CardanoBlocksDTO] = []
        for block_height in block_height_list:
            block_info_batch: list[CardanoBlocksDTO] = await self._extractor.get_block_from_s3(end_block_height=str(block_height))
            all_block_info.extend(block_info_batch)
        print(f"list of CardanoBlocksDTO extracted")
        # insert into cardano_blocks db
        async with self._engine.begin() as conn:
            await self._cardano_block_dao.insert_blocks(async_connection=conn, input=all_block_info)
        print(f"cardano block table updated with batch of CardanoBlocksDTO")

        # update s3_to_db_import_status_table
        db_import_status_dto: S3ToDBImportStatusDTO = S3ToDBImportStatusDTO(
            table="cardano_blocks",
            block_height=s3_last_block_height,
            created_at=datetime.utcnow(),
    )
        await self._s3_to_db_import_status_dao.insert_latest_import_status(db_import_status_dto)


def run():
    """
    Responsible for running the ETLPipeline
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
    cardano_block_dao: CardanoBlockDAO = CardanoBlockDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )

    # extractor:
    s3_to_db_cardano_blocks_etl_pipeline: S3ToDBCardanoBlocksETLPipeline = S3ToDBCardanoBlocksETLPipeline(
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        cardano_block_dao=cardano_block_dao,
        s3_explorer=s3_explorer,
    )
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(s3_to_db_cardano_blocks_etl_pipeline.run())


if __name__ == "__main__":
    run()
