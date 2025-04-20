import csv
import io
from datetime import datetime, timezone
import os
import json
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


class S3ToDBCardanoBlocksETLPipeline:
    """
    Responsible for:
        1) Get latest modified date from s3_import status table where table column = "cardano_blocks"
        Use s3_to_db_import_status_dao.read_latest_import_status()
        2) Get a generator of files from S3 from specified prefix path and has modified date after the specified date
        Use s3_explorer.list_files()
        3) Set up connection from engine.begin() -> create temp table -> Insert files into DB
        Use cardano_block_dao.create_temp_table()
        Iterate through the generator, and for each file from S3,
         use s3_explorer.download_to_buffer()
         use cardano_block_dao.copy_blocks_to_db()
        4) Update import status into S3ToDBImportStatusDTO
        Use S3ToDBImportStatusDTO.create_import_status to create import status
        Use s3_to_import_status_dao.insert_latest_import_status to insert import status to s3_to_db_import_status table in DB
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

        s3_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_prefix_path, default_modified_date
        )
        # this is for current batch's latest modified date - for batch specific
        # current_batch_latest_modified_date: datetime = datetime(
        #     year=2020, month=12, day=1
        # )
        latest_file_modified_date: datetime | None = default_modified_date

        async with self._engine.begin() as conn:
            # step 1: create temp table
            await self._cardano_block_dao.create_temp_table(async_connection=conn)
            # step 2: insert all files into DB
            for file_info in s3_file_info:
                latest_file_modified_date = max(
                    latest_file_modified_date, file_info.modified_date
                )

                csv_bytes: io.BytesIO = self._s3_explorer.download_to_buffer(
                    file_info.file_path
                )
                file_bytes = csv_bytes.read()
                print(file_bytes[:200], "...")  # just log the first 200 bytes
                # csv_bytes.seek(0)
            try:
                # JSON path to list of dicts
                rows = json.loads(file_bytes)
            except json.JSONDecodeError:
                # CSV path to parse into list of dicts first
                txt = file_bytes.decode("utf-8")
                rdr = csv.DictReader(io.StringIO(txt))
                rows = [row for row in rdr]

            # in either case "rows" is a list of dict with epoch in time
            # convert and serialize to CSV
            cols = [c.name for c in self._cardano_block_dao.table.columns]
            out_txt = io.StringIO()
            writer = csv.DictWriter(out_txt, fieldnames=cols)
            writer.writeheader()

            for rec in rows:
                # convert epoch sec to iso utc timestamp string
                dt = datetime.fromtimestamp(int(rec["time"]), tz=timezone.utc)
                rec["time"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow({c: rec.get(c) for c in cols})

            csv_bytes = io.BytesIO(out_txt.getvalue().encode("utf-8"))
            csv_bytes.seek(0)

            # bulk load
            await self._cardano_block_dao.copy_blocks_to_db(
                    async_connection=conn, data_buffer=csv_bytes
                )
            # step 3: update import status
            import_status = S3ToDBImportStatusDTO.create_import_status(
                table=self._table, file_modified_date=latest_file_modified_date
            )
            await self._s3_to_db_import_status_dao.insert_latest_import_status(
                import_status, conn
            )

    #     async with self._engine.begin() as conn:
    #         stmt: Select = (
    #             select(provider_to_s3_import_status_table.c.block_height)
    #             .where(
    #                 provider_to_s3_import_status_table.c.block_height >= start_block_height,
    #                 provider_to_s3_import_status_table.c.block_height <= s3_last_block_height
    #             )
    #         )
    #         result: CursorResult = await conn.execute(stmt)
    #         rows: Sequence[Row] = result.fetchall()
    #
    #     block_height_list: list[int] = [row[0] for row in rows]
    #     # pprint(block_height_list)
    #     # batch all block info into block_info_batch
    #     all_block_info: list[CardanoBlocksDTO] = []
    #     for block_height in block_height_list:
    #         block_info_batch: list[CardanoBlocksDTO] = await self._extractor.get_block_from_s3(end_block_height=str(block_height))
    #         all_block_info.extend(block_info_batch)
    #     print(f"list of CardanoBlocksDTO extracted")
    #     # insert into cardano_blocks db
    #     async with self._engine.begin() as conn:
    #         await self._cardano_block_dao.insert_blocks(async_connection=conn, input=all_block_info)
    #     print(f"cardano block table updated with batch of CardanoBlocksDTO")
    #
    #     # update s3_to_db_import_status_table
    #     db_import_status_dto: S3ToDBImportStatusDTO = S3ToDBImportStatusDTO(
    #         table="cardano_blocks",
    #         block_height=s3_last_block_height,
    #         created_at=datetime.utcnow(),
    # )
    #     await self._s3_to_db_import_status_dao.insert_latest_import_status(db_import_status_dto)


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
    s3_to_db_cardano_blocks_etl_pipeline: S3ToDBCardanoBlocksETLPipeline = (
        S3ToDBCardanoBlocksETLPipeline(
            s3_to_db_import_status_dao=s3_to_db_import_status_dao,
            provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
            table="cardano_blocks",
            prefix_path="cardano/blocks",
            cardano_block_dao=cardano_block_dao,
            s3_explorer=s3_explorer,
        )
    )

    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(s3_to_db_cardano_blocks_etl_pipeline.run())


if __name__ == "__main__":
    run()
