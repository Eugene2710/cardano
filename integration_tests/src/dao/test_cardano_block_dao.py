import asyncio
from asyncio import AbstractEventLoop
import os
import pytest
from io import BytesIO
import freezegun
from datetime import datetime
import pytest_asyncio
from typing import Generator
from sqlalchemy import text, Table, CursorResult, Sequence, Row
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from dotenv import load_dotenv

from src.dao.cardano_block_dao import CardanoBlockDAO
from database_management.cardano.cardano_tables import cardano_block_table


@pytest.fixture
def input_tables() -> list[Table]:
    return [cardano_block_table]


@pytest.fixture
def cardano_blocks_dao(connection_string: str) -> CardanoBlockDAO:
    dao: CardanoBlockDAO = CardanoBlockDAO(connection_string)
    return dao


class TestCardanoBlockDAO:
    """
    Responsible for testing the CardanoBlockDAO, and in specific, the create_temp_table method and copy_blocks_to_db method
    """
    # @pytest_asyncio.fixture(scope="session")
    # def event_loop(self) -> Generator[asyncio.AbstractEventLoop, None, None]:
    #     """
    #     session-wide event loop for the tests to use
    #     """
    #     loop: AbstractEventLoop = asyncio.new_event_loop()
    #     yield loop
    #     loop.close()

    # @pytest.fixture
    # def data_buffer(self) -> BytesIO:
    #     """
    #     convert cardano blocks data into csv -> BytesIO
    #     """

    @pytest.mark.asyncio_cooperative
    async def test_create_temp_table(self, pg_engine: AsyncEngine, cardano_blocks_dao: CardanoBlockDAO) -> None:
        """
        GIVEN an async connection, cardano_blocks_dao
        WHEN create_temp_table is called
        THEN the temp_table name should exist and dropped automatically after
        """
        async with pg_engine.begin() as conn:
            await cardano_blocks_dao.create_temp_table(conn)
            tmp: str = cardano_blocks_dao._temp_table_name

            # check if table of the temp table name exists in postgres
            # if table does not exist, sqlalchemy.exc.ProgrammingError will be raised
            cursor_result: CursorResult = await conn.execute(text(f"SELECT * FROM {tmp}"))
            rows: Row | None = cursor_result.fetchone()

        # check if table of the temp table name does not exist anymore
        async with pg_engine.connect() as conn:
            row = await conn.execute(text("SELECT to_regclass(:t)"), {"t": tmp})
            assert row.scalar_one() is None


        # expected_temp_table_name: str = f"cardano_block_table_2025051748534401000001"
        # async with cardano_blocks_dao._engine.begin() as conn:
        #     await cardano_blocks_dao.create_temp_table(conn)
        #     assert expected_temp_table_name == cardano_blocks_dao._temp_table_name

    # @pytest.mark.asyncio_cooperative
    # async def test_copy_blocks_to_db(self, connection_string, ) -> None:
    #     """
    #     GIVEN a connection string and a data buffer from a csv file
    #     WHEN copy_blocks_to_db is called
    #     THEN the blocks_to_db, which has been converted to bytesIO already, will be inserted into DB
    #     """


