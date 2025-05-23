import io
from io import BytesIO
from datetime import datetime, timezone
import os

import boto3
import pandas as pd
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
from src.models.database_transfer_objects.s3_to_db_import_status_dto import S3ToDBImportStatusDTO
from src.models.database_transfer_objects.cardano_blocks import CardanoBlocksDTO
from src.transformer.transform_cardano_block_dto_to_df import TransformCardanoBlockDTOToDF


class S3ToDBCardanoBlocksETLPipeline:
    """
    Responsible for:
        1) Get Raw Cardano Block json data from S3 - s3_file_path: cardano/blocks/raw
            Get latest modified date from s3_import status table where table column = "cardano_blocks"
            Use s3_to_db_import_status_dao.read_latest_import_status()
        2) Transform Raw Cardano Block data to Cardano Blocks DTO in csv format
        3) Upload transformed Cardano Blocks DTO CSV data to S3 - s3_file_path: cardano/blocks/transformed
        4) Copy blocks to DB - cardano_blocks table
            create temp table using cardano_block_dao.create_temp_table()
            Iterate through the generator, and for each file from S3,
             use s3_explorer.download_to_buffer()
             use cardano_block_dao.copy_blocks_to_db()
        5) Update import status into S3ToDBImportStatusDTO
    """
    def __init__(
        self,
        s3_to_db_import_status_dao: S3ToDbImportStatusDAO,
        provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO,
        table: str,
        s3_raw_blocks_path: str,
        extractor: CardanoBlockS3Extractor,
        transformer: TransformCardanoBlockDTOToDF,
        s3_transformed_blocks_path: str,
        cardano_block_dao: CardanoBlockDAO,
        s3_explorer: S3Explorer,
    ) -> None:
        self._s3_to_db_import_status_dao: S3ToDbImportStatusDAO = s3_to_db_import_status_dao
        self._table: str = table
        self._s3_raw_blocks_path = s3_raw_blocks_path
        self._extractor = extractor
        self._transformer = transformer
        self._s3_transformed_blocks_path = s3_transformed_blocks_path
        self._provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = provider_to_s3_import_status_dao
        self._s3_explorer: S3Explorer = s3_explorer
        self._engine: AsyncEngine = create_async_engine(
            os.getenv("ASYNC_PG_CONNECTION_STRING", "")
        )
        self._cardano_block_dao: CardanoBlockDAO = cardano_block_dao

    async def run(self) -> None:
        """
        async version to run the pipeline
        """
        latest_modified_date: datetime | None = (
            await self._s3_to_db_import_status_dao.read_latest_import_status(
                self._table
            )
        )
        print(f"latest_modified_date={latest_modified_date}")
        # this is for listing files after import_status_modified_date - for S3 specific
        default_modified_date: datetime = latest_modified_date or datetime(
            year=2020, month=1, day=1
        )
        # list files from cardano/blocks/raw
        s3_raw_blocks_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_raw_blocks_path, default_modified_date
        )

        for raw_file_info in s3_raw_blocks_file_info:
            latest_raw_blocks_file_modified_date = max(
                default_modified_date, raw_file_info.modified_date
            )
            print(latest_raw_blocks_file_modified_date)
            # get raw blocks json files from S3 and change it to CardanoBlocksDTO
            block_dto_list: list[CardanoBlocksDTO] = self._extractor.get_block_from_s3(s3_path=raw_file_info.file_path)

            # transform the dto list to a dataframe
            df: pd.DataFrame = self._transformer.transform(block_dto_list)
            # convert pandas dataframe into a csv bytesIO
            csv_buffer: BytesIO = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            # upload transformed CardanoblocksDTO in csv format to s3
            self._s3_explorer.upload_buffer(bytes_io=csv_buffer, source_path=f"cardano/blocks/transformed/{latest_raw_blocks_file_modified_date}/cardano_blocks_transformed_{latest_raw_blocks_file_modified_date}.csv")

        # list files from cardano/blocks/transformed
        s3_transformed_blocks_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_transformed_blocks_path, default_modified_date
        )

        async with self._engine.begin() as conn:
            # create temp table
            await self._cardano_block_dao.create_temp_table(async_connection=conn)
            # step 2: get all files from S3 cardano/blocks/transformed path-
            for transformed_file_info in s3_transformed_blocks_file_info:
                latest_transformed_blocks_file_modified_date = max(
                    default_modified_date, transformed_file_info.modified_date
                )
                # download CardanoBlocksDTO csv files from S3 to buffer and copy to DB
                csv_bytes: io.BytesIO = self._s3_explorer.download_to_buffer(
                    transformed_file_info.file_path
                )
                await self._cardano_block_dao.copy_blocks_to_db(
                    async_connection=conn, data_buffer=csv_bytes
                )

            # update import status - s3_to_db_import_status table
            import_status = S3ToDBImportStatusDTO.create_import_status(
                table=self._table, file_modified_date=latest_transformed_blocks_file_modified_date
            )
            await self._s3_to_db_import_status_dao.insert_latest_import_status(
                import_status, conn
            )


def run():
    """
    Responsible for running the ETLPipeline
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
    extractor: CardanoBlockS3Extractor = CardanoBlockS3Extractor(
        s3_explorer=s3_explorer
    )
    transformer: TransformCardanoBlockDTOToDF = TransformCardanoBlockDTOToDF()
    cardano_block_dao: CardanoBlockDAO = CardanoBlockDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )

    s3_to_db_cardano_blocks_etl_pipeline: S3ToDBCardanoBlocksETLPipeline = (
        S3ToDBCardanoBlocksETLPipeline(
            s3_to_db_import_status_dao=s3_to_db_import_status_dao,
            provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
            table="cardano_blocks",
            s3_raw_blocks_path="cardano/blocks/raw",
            extractor=extractor,
            transformer=transformer,
            s3_transformed_blocks_path="cardano/blocks/transformed",
            cardano_block_dao=cardano_block_dao,
            s3_explorer=s3_explorer,
        )
    )

    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(s3_to_db_cardano_blocks_etl_pipeline.run())


if __name__ == "__main__":
    run()
