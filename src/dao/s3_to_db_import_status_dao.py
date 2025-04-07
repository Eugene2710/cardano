import asyncio
import os
from asyncio import new_event_loop, AbstractEventLoop

from dotenv import load_dotenv
from tenacity import retry, wait_fixed, stop_after_attempt
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncConnection
from sqlalchemy import (
    Table,
    Insert,
    Select,
    select,
    func,
    CursorResult,
    Row,
)
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import logging
from database_management.cardano.cardano_tables import s3_to_db_import_status_table
from src.models.database_transfer_objects.s3_to_db_import_status_dto import S3ToDBImportStatusDTO
from src.utils.logging_utils import setup_logging

logger = logging.getLogger(__name__)
setup_logging(logger)


class S3ToDbImportStatusDAO:
    def __init__(self, connection_string: str) -> None:
        self._engine: AsyncEngine = create_async_engine(connection_string)
        self._table: Table = s3_to_db_import_status_table

    @retry(
        wait=wait_fixed(0.01),  # ~10ms before attempts
        stop=stop_after_attempt(5),  # equivalent to 5 retries
        reraise=True
    )
    async def insert_latest_import_status(
            self, import_status: S3ToDBImportStatusDTO, conn: AsyncConnection
    ) -> None:
        insert_text_clause: Insert = insert(self._table).values(
            import_status.model_dump()
        ).on_conflict_do_nothing()
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
    async def read_latest_import_status(
            self, table: str
    ) -> datetime | None:
        query_latest_import_status: Select = select(
            func.max(self._table.c.file_modified_date)
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
    connection_string: str = os.getenv("ASYNC_PG_CONNECTION_STRING")
    dao: S3ToDbImportStatusDAO = S3ToDbImportStatusDAO(connection_string)
    db_import_status_dto: S3ToDBImportStatusDTO = S3ToDBImportStatusDTO(
        table="cardano_blocks",
        file_modified_date=datetime.utcnow(),
        created_at=datetime.utcnow(),
    )

    async def run_insert_import_status() -> None:
        engine: AsyncEngine = create_async_engine(connection_string)
        async with engine.begin() as conn:
            await dao.insert_latest_import_status(db_import_status_dto, conn)
    asyncio.run(run_insert_import_status())

    event_loop: AbstractEventLoop = new_event_loop()
    # event_loop.run_until_complete(dao.insert_latest_import_status(db_import_status_dto))
    latest_file_date: datetime | None = event_loop.run_until_complete(
        dao.read_latest_import_status(table="cardano_blocks")
    )
    print(latest_file_date)
