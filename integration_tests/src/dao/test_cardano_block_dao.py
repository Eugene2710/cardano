import asyncio
import csv
from asyncio import AbstractEventLoop
import os
import pandas as pd
import pytest
from io import BytesIO
import freezegun
from datetime import datetime
from typing import Generator
from sqlalchemy import text, Table, CursorResult, Sequence, Row
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from dotenv import load_dotenv

from src.dao.cardano_block_dao import CardanoBlockDAO
from database_management.cardano.cardano_tables import cardano_block_table
from src.models.database_transfer_objects.cardano_blocks import CardanoBlocksDTO


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

    @pytest.fixture
    def data_buffer(self) -> BytesIO:
        """
        convert cardano blocks data into csv -> BytesIO
        """
        # Method 1: pass in a csv file from utils
        # df: pd.DataFrame = pd.read_csv('integration_tests/utils/cardano_blocks_integration_test.csv')
        # convert the csv file to BytesIO and return
        # blocks_buffer: BytesIO = BytesIO()
        # df.to_csv(blocks_buffer, index=False)
        # blocks_buffer.seek(0)
        # return blocks_buffer
        # Method 2: build CSV your own
        rows = [
            ("time", "height", "hash", "slot", "epoch", "epoch_slot", "slot_leader", "size", "tx_count", "output", "fees", "block_vrf", "op_cert", "op_cert_counter", "previous_block", "next_block", "confirmations", "created_at"),
            ("2024-11-25 23:35:26", 11140700, "54be20372bf25f73108c8a1e661a54e8cd6bf43950914574044108ade7c3bb31", 141011435, 524, 6635, "pool1njjr0zn7uvydjy8067nprgwlyxqnznp9wgllfnag24nycgkda25", 43454, 14, "966188596446", 5432086, "vrf_vk197w95j9alkwt8l4g7xkccknhn4pqwx65c5saxnn5ej3cpmps72msgpw69d", "396888bf48d309a247db7a6c0a71db3a8903a426f1a5cb7bbef20ec52ad1eac7", "1659973039", "ba93915785be485980a15badcf7a9c7a2ef36ef8f7e8b6f1a8423836be7a3428", "f3d28e42870aaa6e148b49409b32502b0d108f2f7e45fe75aef4161bb5ce484e", 896065, "2025-06-24 03:09:57.612422")
        ]
        csv_str_io = []
        # writer to call .append for each line
        writer = csv.writer(csv_str_io.append)
        writer.writerow(row for row in rows)
        # full csv as a single str in memory
        csv_text = "".join(csv_str_io)

        # encode csv text
        csv_bytes = csv_text.encode("utf-8")

        # wrap in BytesIO
        buffer: BytesIO = BytesIO(csv_bytes)
        buffer.seek(0)
        return csv_bytes


    @pytest.fixture
    def sample_blocks(self) -> list[CardanoBlocksDTO]:
        return [
            CardanoBlocksDTO(
                time=datetime(2024, 11, 25, 23, 35, 26),
                height=11140700,
                hash="54be20372bf25f73108c8a1e661a54e8cd6bf43950914574044108ade7c3bb31",
                slot="54be20372bf25f73108c8a1e661a54e8cd6bf43950914574044108ade7c3bb31",
                epoch=141011435,
                epoch_slot=524,
                slot_leader=6635,

            )
        ]


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

    #
    # @pytest.mark.asyncio_cooperative
    # async def test_copy_blocks_to_db(
    #         self,
    #         pg_engine: AsyncEngine,
    #         data_buffer: BytesIO,
    #         cardano_blocks_dao: CardanoBlockDAO,
    #         sample_blocks,
    # ) -> None:
    #     """
    #     GIVEN a connection string and a data buffer from a csv file
    #     WHEN copy_blocks_to_db is called
    #     THEN the blocks_to_db, which has been converted to bytesIO already, will be copied from temp_table and inserted into DB
    #     """
    #     async with pg_engine.begin() as conn:
    #         await cardano_blocks_dao.create_temp_table(async_connection=conn)
    #         await cardano_blocks_dao.copy_blocks_to_db(async_connection=conn, data_buffer=data_buffer)
    #



