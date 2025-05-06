import io
from io import BytesIO
from datetime import datetime, timezone
import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from asyncio import AbstractEventLoop, new_event_loop
from typing import Generator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from src.models.file_info.file_info import FileInfo
from src.dao.s3_to_db_import_status_dao import S3ToDbImportStatusDAO
from src.models.database_transfer_objects.cardano_transactions_utxo_dto import CardanoTransactionUtxoDTO
from src.extractors.get_tx_utxo_from_s3 import CardanoTxUtxoS3Extractor
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.database_transfer_objects.s3_to_db_import_status_dto import S3ToDBImportStatusDTO
from src.transformer.transform_cardano_tx_utxo_dto_to_df import TransformCardanoTxUtxoDTOToDf
from src.dao.cardano_tx_utxo_dao import CardanoTxUtxoDAO
from src.dao.cardano_tx_utxo_sub_dao import CardanoTxUtxoSubDAO
from database_management.cardano.cardano_tables import cardano_tx_utxo_input_table, cardano_tx_utxo_input_amount_table, cardano_tx_utxo_output_table, cardano_tx_utxo_output_amount_table


class S3ToDBCardanoTxUtxoETLPipeline:
    """
    Responsible for:
    1) Get raw cardano transaction utxo from s3 - s3_file_path: cardano/transaction_utxo/raw
        Get latest modified date from s3_import_status table where table column = "cardano_transaction_utxo"
        Use s3_to_db_import_status_dao.read_latest_import_status()
    2) Transform Raw Cardano Transaction UTXO data to Cardano Transaction UTXO DTO
    """
    def __init__(
            self,
            s3_to_db_import_status_dao: S3ToDbImportStatusDAO,
            table: str,
            s3_raw_tx_path: str,
            extractor: CardanoTxUtxoS3Extractor,
            transformer: TransformCardanoTxUtxoDTOToDf,
            s3_explorer: S3Explorer,
            s3_transformed_tx_utxo_path: str,
            s3_transformed_tx_utxo_input_path: str,
            s3_transformed_tx_utxo_input_amt_path: str,
            s3_transformed_tx_utxo_output_path: str,
            s3_transformed_tx_utxo_output_amt_path: str,
            cardano_tx_utxo_dao: CardanoTxUtxoDAO,
            cardano_tx_utxo_output_dao: CardanoTxUtxoSubDAO,
            cardano_tx_utxo_output_amt_dao: CardanoTxUtxoSubDAO,
            cardano_tx_utxo_input_dao: CardanoTxUtxoSubDAO,
            cardano_tx_utxo_input_amt_dao: CardanoTxUtxoSubDAO,

    ) -> None:
        self._s3_to_db_import_status_dao: S3ToDbImportStatusDAO = s3_to_db_import_status_dao
        self._table: str = table
        self._s3_raw_tx_path = s3_raw_tx_path
        self._extractor = extractor
        self._transformer = transformer
        self._s3_explorer = s3_explorer
        self._s3_transformed_tx_utxo_path = s3_transformed_tx_utxo_path
        self._s3_transformed_tx_utxo_input_path = s3_transformed_tx_utxo_input_path
        self._s3_transformed_tx_utxo_input_amt_path = s3_transformed_tx_utxo_input_amt_path
        self._s3_transformed_tx_utxo_output_path = s3_transformed_tx_utxo_output_path
        self._s3_transformed_tx_utxo_output_amt_path = s3_transformed_tx_utxo_output_amt_path
        self._engine: AsyncEngine = create_async_engine(
            os.getenv("ASYNC_PG_CONNECTION_STRING", "")
        )
        self._cardano_tx_utxo_dao = cardano_tx_utxo_dao
        self._cardano_tx_utxo_output_dao = cardano_tx_utxo_output_dao
        self._cardano_tx_utxo_output_amt_dao = cardano_tx_utxo_output_amt_dao
        self._cardano_tx_utxo_input_dao = cardano_tx_utxo_input_dao
        self._cardano_tx_utxo_input_amt_dao = cardano_tx_utxo_input_amt_dao

    async def run(self) -> None:
        latest_modified_date: datetime | None = (
            await self._s3_to_db_import_status_dao.read_latest_import_status(
                self._table
            )
        )
        print(f"latest_modified_date={latest_modified_date}")
        # this is for listing files after import_status_modified_date - for S3 specific
        default_modified_date: datetime = latest_modified_date or datetime(
            year=2024, month=12, day=30
        )
        s3_raw_tx_utxo_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_raw_tx_path, default_modified_date
        )

        for raw_file_info in s3_raw_tx_utxo_file_info:
            latest_raw_tx_utxo_file_modified_date = max(
                default_modified_date, raw_file_info.modified_date
            )
            # get raw tx json file from  S3 and change it to CardanoTransactionsDTO
            tx_utxo_dto_list: list[CardanoTransactionUtxoDTO] = self._extractor.get_tx_utxo_from_s3(
                s3_path=raw_file_info.file_path
            )
            # transform the dto list to a dictionary of dataframes - tx_utxo, tx_utxo_input, tx_utxo_output
            dfs: dict[str, pd.DataFrame] = self._transformer.transform(cardano_tx_utxo_dto_list=tx_utxo_dto_list)
            cardano_tx_utxo_df: pd.DataFrame = dfs["cardano_tx_utxo"]
            cardano_tx_utxo_input_df: pd.DataFrame = dfs["cardano_tx_utxo_input"]
            cardano_tx_utxo_input_amt_df: pd.DataFrame = dfs["cardano_tx_utxo_input_amt"]
            cardano_tx_utxo_output_df: pd.DataFrame = dfs["cardano_tx_utxo_output"]
            cardano_tx_utxo_output_amt_df: pd.DataFrame = dfs["cardano_tx_utxo_output_amt"]

            # convert the pandas DataFrames into csv bytesIO files
            csv_buffer: BytesIO = BytesIO()
            cardano_tx_utxo_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            self._s3_explorer.upload_buffer(bytes_io=csv_buffer, source_path=f"cardano/transaction_utxo/transformed/utxo/{latest_raw_tx_utxo_file_modified_date}/cardano_tx_utxo_transformed_{latest_raw_tx_utxo_file_modified_date}.csv")
            csv_buffer: BytesIO = BytesIO()
            cardano_tx_utxo_input_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            self._s3_explorer.upload_buffer(bytes_io=csv_buffer, source_path=f"cardano/transaction_utxo/transformed/utxo_input/{latest_raw_tx_utxo_file_modified_date}/cardano_tx_utxo_input_transformed_{latest_raw_tx_utxo_file_modified_date}.csv")
            csv_buffer: BytesIO = BytesIO()
            cardano_tx_utxo_input_amt_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            self._s3_explorer.upload_buffer(bytes_io=csv_buffer,source_path=f"cardano/transaction_utxo/transformed/utxo_input_amount/{latest_raw_tx_utxo_file_modified_date}/cardano_tx_utxo_input_amount_transformed_{latest_raw_tx_utxo_file_modified_date}.csv")
            csv_buffer: BytesIO = BytesIO()
            cardano_tx_utxo_output_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            self._s3_explorer.upload_buffer(bytes_io=csv_buffer, source_path=f"cardano/transaction_utxo/transformed/utxo_output/{latest_raw_tx_utxo_file_modified_date}/cardano_tx_utxo_output_transformed_{latest_raw_tx_utxo_file_modified_date}.csv")
            csv_buffer: BytesIO = BytesIO()
            cardano_tx_utxo_output_amt_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            self._s3_explorer.upload_buffer(bytes_io=csv_buffer, source_path=f"cardano/transaction_utxo/transformed/utxo_output_amount/{latest_raw_tx_utxo_file_modified_date}/cardano_tx_utxo_output_amount_transformed_{latest_raw_tx_utxo_file_modified_date}.csv")
        # list files from cardano/transaction_utxo/transformed/utxo
        s3_transformed_tx_utxo_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_transformed_tx_utxo_path, default_modified_date
        )
        s3_transformed_tx_utxo_input_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_transformed_tx_utxo_input_path, default_modified_date
        )
        s3_transformed_tx_utxo_input_amt_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_transformed_tx_utxo_input_amt_path, default_modified_date
        )
        s3_transformed_tx_utxo_output_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_transformed_tx_utxo_output_path, default_modified_date
        )
        s3_transformed_tx_utxo_output_amt_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_transformed_tx_utxo_output_amt_path, default_modified_date
        )

        async with self._engine.begin() as conn:
            # create temp table
            await self._cardano_tx_utxo_dao.create_temp_table(async_connection=conn)
            # get all files from s3 cardano/transaction_utxo/transformed path
            for transformed_file_info in s3_transformed_tx_utxo_file_info:
                latest_transformed_tx_utxo_file_modified_date = max(
                    default_modified_date, transformed_file_info.modified_date
                )
                print(latest_transformed_tx_utxo_file_modified_date)
                print("key fetched:", transformed_file_info.file_path)
                # download CardanoTxUtxoDTO csv files from S3 to buffer and copy to DB
                csv_bytes: io.BytesIO = self._s3_explorer.download_to_buffer(
                    transformed_file_info.file_path
                )
                csv_bytes.seek(0)
                print("peek:", transformed_file_info.file_path,
                      "→", csv_bytes.getvalue()[:120])
                await self._cardano_tx_utxo_dao.copy_tx_utxo_to_db(
                    async_connection=conn, data_buffer=csv_bytes
                )
            await self._cardano_tx_utxo_input_dao.create_temp_table(async_connection=conn)
            for transformed_file_info in s3_transformed_tx_utxo_input_file_info:
                # download  csv files from S3 to buffer and copy to DB
                csv_bytes: io.BytesIO = self._s3_explorer.download_to_buffer(
                    transformed_file_info.file_path
                )
                await self._cardano_tx_utxo_input_dao.copy_tx_utxo_to_db(
                    async_connection=conn, data_buffer=csv_bytes
                )
            await self._cardano_tx_utxo_input_amt_dao.create_temp_table(async_connection=conn)
            for transformed_file_info in s3_transformed_tx_utxo_input_amt_file_info:
                # download  csv files from S3 to buffer and copy to DB
                csv_bytes: io.BytesIO = self._s3_explorer.download_to_buffer(
                    transformed_file_info.file_path
                )
                await self._cardano_tx_utxo_input_amt_dao.copy_tx_utxo_to_db(
                    async_connection=conn, data_buffer=csv_bytes
                )
            await self._cardano_tx_utxo_output_dao.create_temp_table(async_connection=conn)
            for transformed_file_info in s3_transformed_tx_utxo_output_file_info:
                # download  csv files from S3 to buffer and copy to DB
                csv_bytes: io.BytesIO = self._s3_explorer.download_to_buffer(
                    transformed_file_info.file_path
                )
                await self._cardano_tx_utxo_output_dao.copy_tx_utxo_to_db(
                    async_connection=conn, data_buffer=csv_bytes
                )
            await self._cardano_tx_utxo_output_amt_dao.create_temp_table(async_connection=conn)
            for transformed_file_info in s3_transformed_tx_utxo_output_amt_file_info:
                # download  csv files from S3 to buffer and copy to DB
                csv_bytes: io.BytesIO = self._s3_explorer.download_to_buffer(
                    transformed_file_info.file_path
                )
                await self._cardano_tx_utxo_output_amt_dao.copy_tx_utxo_to_db(
                    async_connection=conn, data_buffer=csv_bytes
                )

            # update import status - s3_to_db_import_status table
            import_status = S3ToDBImportStatusDTO.create_import_status(
                table=self._table, file_modified_date=latest_transformed_tx_utxo_file_modified_date
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
    extractor: CardanoTxUtxoS3Extractor = CardanoTxUtxoS3Extractor(
        s3_explorer=s3_explorer
    )
    transformer: TransformCardanoTxUtxoDTOToDf = TransformCardanoTxUtxoDTOToDf()
    cardano_tx_utxo_dao: CardanoTxUtxoDAO = CardanoTxUtxoDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    cardano_tx_utxo_output_dao: CardanoTxUtxoSubDAO = CardanoTxUtxoSubDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", ""),
        table=cardano_tx_utxo_output_table
    )
    cardano_tx_utxo_output_amt_dao: CardanoTxUtxoSubDAO = CardanoTxUtxoSubDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", ""),
        table=cardano_tx_utxo_output_amount_table
    )
    cardano_tx_utxo_input_dao: CardanoTxUtxoSubDAO = CardanoTxUtxoSubDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", ""),
        table=cardano_tx_utxo_input_table
    )
    cardano_tx_utxo_input_amt_dao: CardanoTxUtxoSubDAO = CardanoTxUtxoSubDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", ""),
        table=cardano_tx_utxo_input_amount_table
    )
    s3_to_db_cardano_tx_utxo_etl_pipeline: S3ToDBCardanoTxUtxoETLPipeline = S3ToDBCardanoTxUtxoETLPipeline(
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        table="cardano_tx_utxo",
        s3_raw_tx_path="cardano/transaction_utxo/raw",
        extractor=extractor,
        transformer=transformer,
        s3_explorer=s3_explorer,
        s3_transformed_tx_utxo_path="cardano/transaction_utxo/transformed/utxo",
        s3_transformed_tx_utxo_input_path="cardano/transaction_utxo/transformed/utxo_input",
        s3_transformed_tx_utxo_input_amt_path="cardano/transaction_utxo/transformed/utxo_input_amount",
        s3_transformed_tx_utxo_output_path="cardano/transaction_utxo/transformed/utxo_output",
        s3_transformed_tx_utxo_output_amt_path="cardano/transaction_utxo/transformed/utxo_output_amount",
        cardano_tx_utxo_dao=cardano_tx_utxo_dao,
        cardano_tx_utxo_output_dao=cardano_tx_utxo_output_dao,
        cardano_tx_utxo_output_amt_dao=cardano_tx_utxo_output_amt_dao,
        cardano_tx_utxo_input_dao=cardano_tx_utxo_input_dao,
        cardano_tx_utxo_input_amt_dao=cardano_tx_utxo_input_amt_dao,
    )

    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(s3_to_db_cardano_tx_utxo_etl_pipeline.run())


if __name__ == "__main__":
    run()
