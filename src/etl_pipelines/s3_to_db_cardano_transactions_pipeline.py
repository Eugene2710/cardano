import io
from io import BytesIO
from datetime import datetime
import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from asyncio import AbstractEventLoop, new_event_loop
from typing import Generator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from src.models.file_info.file_info import FileInfo
from src.dao.s3_to_db_import_status_dao import S3ToDbImportStatusDAO
from src.models.database_transfer_objects.cardano_transactions import CardanoTransactionsDTO
from src.extractors.get_transactions_from_s3 import CardanoTransactionsS3Extractor
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.database_transfer_objects.s3_to_db_import_status_dto import S3ToDBImportStatusDTO
from src.transformer.transform_cardano_tx_dto_to_df import TransformCardanoTransactionsDTOToDf
from src.dao.cardano_transactions_dao import CardanoTransactionsDAO


class S3ToDBCardanoTransactionsETLPipeline:
    """
    Responsible for:
    1) Get raw cardano transactions from S3 - s3_file_path: cardano/transactions/raw
        Get latest modified date from s3_import_status table where table column = "cardano_transactions"
        Use s3_to_db_import_status_dao.read_latest_import_status()
    2) Transform Raw Cardano Transactions data to Cardano Transactions DTO in csv format
    3)
    """
    def __init__(
            self,
            s3_to_db_import_status_dao: S3ToDbImportStatusDAO,
            table: str,
            s3_raw_tx_path: str,
            extractor: CardanoTransactionsS3Extractor,
            transformer: TransformCardanoTransactionsDTOToDf,
            s3_transformed_tx_path: str,
            s3_explorer: S3Explorer,
            cardano_transactions_dao: CardanoTransactionsDAO
    ) ->  None:
        self._s3_to_db_import_status_dao: S3ToDbImportStatusDAO = s3_to_db_import_status_dao
        self._table: str = table
        self._s3_raw_tx_path = s3_raw_tx_path
        self._extractor = extractor
        self._s3_explorer = s3_explorer
        self._transformer = transformer
        self._s3_transformed_tx_path = s3_transformed_tx_path
        self._engine: AsyncEngine = create_async_engine(
            os.getenv("ASYNC_PG_CONNECTION_STRING", "")
        )
        self._cardano_tx_dao = cardano_transactions_dao

    async def run(self) -> None:
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
        s3_raw_tx_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_raw_tx_path, default_modified_date
        )

        for raw_file_info in s3_raw_tx_file_info:
            latest_raw_tx_file_modified_date = max(
                default_modified_date, raw_file_info.modified_date
            )
            # get raw tx json file from  S3 and change it to CardanoTransactionsDTO
            tx_dto_list: list[CardanoTransactionsDTO] = self._extractor.get_tx_from_s3(
                s3_path=raw_file_info.file_path
            )
            # transform the dto list to a dataframe
            df: pd.DataFrame = self._transformer.transform(cardano_tx_dto_list=tx_dto_list)
            # convert pandas DataFrame into a csv bytesIO
            csv_buffer: BytesIO = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            # upload transformed CardanoTxDTO in csv format to s3
            self._s3_explorer.upload_buffer(bytes_io=csv_buffer, source_path=f"cardano/transactions/transformed/{latest_raw_tx_file_modified_date}/cardano_tx_transformed_{latest_raw_tx_file_modified_date}.csv")

        # list files from cardano/transactions/transformed
        s3_transformed_tx_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_transformed_tx_path, default_modified_date
        )

        async with self._engine.begin() as conn:
            # create temp table
            await self._cardano_tx_dao.create_temp_table(async_connection=conn)
            # get all files from s3 cardano/transactions/transformed path
            for transformed_file_info in s3_transformed_tx_file_info:
                latest_transformed_tx_file_modified_date = max(
                    default_modified_date, transformed_file_info.modified_date
                )
                # download CardanoTransactionsDTO csv files from S3 to buffer and copy to DB
                csv_bytes: io.BytesIO = self._s3_explorer.download_to_buffer(
                    transformed_file_info.file_path
                )
                await self._cardano_tx_dao.copy_tx_to_db(
                    async_connection=conn, data_buffer=csv_bytes
                )

            # update import status - s3_to_db_import_status table
            import_status = S3ToDBImportStatusDTO.create_import_status(
                table=self._table, file_modified_date=latest_transformed_tx_file_modified_date
            )
            await self._s3_to_db_import_status_dao.insert_latest_import_status(
                import_status, conn
            )


def run():
    """
    Responsible for running ETLpipeline
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
    s3_to_db_import_status_dao: S3ToDbImportStatusDAO = S3ToDbImportStatusDAO(
        os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    extractor: CardanoTransactionsS3Extractor = CardanoTransactionsS3Extractor(
        s3_explorer=s3_explorer
    )
    transformer: TransformCardanoTransactionsDTOToDf = TransformCardanoTransactionsDTOToDf()
    cardano_tx_dao: CardanoTransactionsDAO = CardanoTransactionsDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    s3_to_db_cardano_tx_etl_pipeline: S3ToDBCardanoTransactionsETLPipeline = S3ToDBCardanoTransactionsETLPipeline(
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        table="cardano_transactions",
        s3_raw_tx_path="cardano/transactions/raw",
        extractor=extractor,
        transformer=transformer,
        s3_transformed_tx_path="cardano/transactions/transformed",
        s3_explorer=s3_explorer,
        cardano_transactions_dao=cardano_tx_dao
    )

    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(s3_to_db_cardano_tx_etl_pipeline.run())


if __name__ == "__main__":
    run()
