import os
import asyncio
from asyncio import new_event_loop, AbstractEventLoop

from dotenv import load_dotenv
from tenacity import retry, wait_fixed, stop_after_attempt
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncConnection
from sqlalchemy import (
    Table,
    insert,
    Insert,
    Select,
    select,
    func,
    CursorResult,
    Row,
)
from datetime import datetime
import logging
from database_management.cardano.cardano_tables import provider_to_s3_import_status_table
from src.models.database_transfer_objects.provider_to_s3_import_status import ProviderToS3ImportStatusDTO
from src.utils.logging_utils import setup_logging

logger = logging.getLogger(__name__)
setup_logging(logger)


class S3ImportStatusDAO:
    def __init__(self, connection_string: str) -> None:
        self._engine: AsyncEngine = create_async_engine(connection_string)
        self._table: Table = provider_to_s3_import_status_table

    @retry(
        wait=wait_fixed(0.01), # ~10ms before attempts
        stop=stop_after_attempt(5), # equivalent to 5 retries
        reraise=True
    )
    async def insert_latest_import_status(
        self, import_status: ProviderToS3ImportStatusDTO, conn: AsyncConnection
    ) -> None:
        insert_text_clause: Insert = insert(self._table).values(
            import_status.model_dump()
        )  # values take in a dict, hence the need to convert the dto to a dict
        try:
            await conn.execute(insert_text_clause)
        except SQLAlchemyError:
            logger.exception("Failed to insert latest import status")
            raise

    @retry(
        wait=wait_fixed(0.01),  # ~10ms before attempts
        stop=stop_after_attempt(5),  # equivalent to 5 retries
        reraise=True
    )
    async def read_latest_import_status(self, table: str) -> int | None:
        query_latest_import_status: Select = select(
            func.max(self._table.c.block_height)
        ).where(self._table.c.table == table)
        try:
            async with self._engine.begin() as conn:
                cursor_result: CursorResult = await conn.execute(
                    query_latest_import_status
                )
            result: Row | None = cursor_result.fetchone()
        except SQLAlchemyError:
            logger.exception("failed to fetch from s3 latest import status")
            raise
        return result[0] if result else None


if __name__ == "__main__":
    load_dotenv()
    connection_string: str = os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    print(connection_string)
    dao: S3ImportStatusDAO = S3ImportStatusDAO(connection_string)
    s3_import_status_dto: ProviderToS3ImportStatusDTO = ProviderToS3ImportStatusDTO(
        table="cardano_blocks",
        block_height=4865265,
        created_at=datetime.utcnow(),
    )

    async def run_insert_status() -> None:
        engine: AsyncEngine = create_async_engine(connection_string)
        async with engine.begin() as conn:
            await dao.insert_latest_import_status(s3_import_status_dto, conn)
    asyncio.run(run_insert_status())

    event_loop: AbstractEventLoop = new_event_loop()
    latest_file_date: datetime | None = event_loop.run_until_complete(
        dao.read_latest_import_status(table="cardano_blocks")
    )
    print(latest_file_date)
