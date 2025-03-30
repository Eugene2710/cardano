import io
from typing import Any
import json
import os
from asyncio import AbstractEventLoop, new_event_loop
from datetime import datetime
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from src.blockfrost.asynchronous.get_block import CardanoBlockExtractor
from src.models.blockfrost_models.raw_cardano_blocks import RawBlockfrostCardanoBlockInfo
from src.dao.provider_to_s3_import_status_dao import ProviderToS3ImportStatusDAO
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.database_transfer_objects.provider_to_s3_import_status import ProviderToS3ImportStatusDTO
from src.models.file_info.file_info import FileInfo


class CardanoBlocksToETLPipeline:
    """
    R
    """

    def __init__(
        self,
        provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO,
        table: str,
        s3_explorer: S3Explorer,
        extractor: CardanoBlockExtractor
    ) -> None:
        self._provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = provider_to_s3_import_status_dao
        self._table: str = table
        self._s3_explorer: S3Explorer = s3_explorer
        self._extractor: CardanoBlockExtractor = extractor

    async def run(self) -> None:
        """
        Step 1: Get latest file modified date from s3_import_status table
        Step 2: Get all files whole modified date is after the s3_import_status modified date
        Step 3: For each file, save it into DB with DAO
        Step 4: Insert file latest modified date into DB
        """
        latest_block_height: int | None = (
            await self._provider_to_s3_import_status_dao.read_latest_import_status(
                self._table
            )
        )
        # this is for listing files after import_status_modified_date - for S3 specific
        default_block_height: int = latest_block_height or 4865265

        block_info: RawBlockfrostCardanoBlockInfo = await self._extractor.get_block(str(default_block_height))
        block_info_dict: list[dict[str, Any]] = [block_info.model_dump()]

        # model dump to convert to dict, then use to convert to bytesio
        block_info_json_bytes = json.dumps(block_info_dict).encode('utf-8')
        bytes_io = io.BytesIO(block_info_json_bytes)

        self._s3_explorer.upload_buffer(bytes_io, source_path="cardano/catalyst/2025/03/30/catalyst_20250108.json")

        updated_s3_import_status: ProviderToS3ImportStatusDTO = ProviderToS3ImportStatusDTO(
            table=self._table,
            block_height=default_block_height+10,
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
    extractor: CardanoBlockExtractor = CardanoBlockExtractor()
    cardano_blocks_to_s3_etl_pipeline: CardanoBlocksToETLPipeline = CardanoBlocksToETLPipeline(
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        table="cardano_blocks",
        s3_explorer=s3_explorer,
        extractor=extractor
    )
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(cardano_blocks_to_s3_etl_pipeline.run())


if __name__ == "__main__":
    run()
