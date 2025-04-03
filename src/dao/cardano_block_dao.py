import os

from typing import Any
from sqlalchemy import (Table,)
from sqlalchemy.dialects.postgresql import insert

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from database_management.cardano.cardano_tables import cardano_block_table
from src.models.database_transfer_objects.cardano_blocks import CardanoBlocksDTO
from src.models.blockfrost_models.raw_cardano_blocks import RawBlockfrostCardanoBlockInfo

from dotenv import load_dotenv
import logging
from asyncio import new_event_loop, AbstractEventLoop
from tenacity import retry, wait_fixed, stop_after_attempt

from src.utils.logging_utils import setup_logging

logger = logging.getLogger(__name__)
setup_logging(logger)


class CardanoBlockDAO:
    """

    """
    def __init__(self, connection_string: str) -> None:
        self._engine: AsyncEngine = create_async_engine(connection_string)
        self._table: Table = cardano_block_table

    @retry(
        wait=wait_fixed(0.01),  # ~10ms before attempts
        stop=stop_after_attempt(5),  # equivalent to 5 retries
        reraise=True
    )
    async def insert_blocks(self, async_connection: AsyncConnection, input: list[CardanoBlocksDTO]) -> None:
        if not input:
            print("insert_blocks: No input. Exiting")
            return

        records: list[dict[str, Any]] = [
            {
                "time": block.time,
                "height": block.height,
                "hash": block.hash,
                "slot": block.slot,
                "epoch": block.epoch,
                "epoch_slot": block.epoch_slot,
                "slot_leader": block.slot_leader,
                "size": block.size,
                "tx_count": block.tx_count,
                "output": block.output,
                "fees": block.fees,
                "block_vrf": block.block_vrf,
                "op_cert": block.op_cert,
                "op_cert_counter": block.op_cert_counter,
                "previous_block": block.previous_block,
                "next_block": block.next_block,
                "confirmations": block.confirmations,
                "created_at": block.created_at,
            }
            for block in input
        ]

        stmt = insert(self._table).values(records).on_conflict_do_nothing(index_elements=["height"])

        await async_connection.execute(stmt)


if __name__ == "__main__":
    load_dotenv()
    connection_string: str = os.getenv("ASYNC_PG_CONNECTION_STRING")
    # Sample raw block data as a list of dictionaries
    sample_raw_cardano_blocks_list: list[dict[str, Any]] = [
        {
            "time": 1735708503,
            "height": 11293700,
            "hash": "33b2b6733638aaff1e40de61e33d8fe22a4b789375b553f801696997ca1295b2",
            "slot": 144142212,
            "epoch": 531,
            "epoch_slot": 113412,
            "slot_leader": "pool16pv24th2tzy8k70vk4ms35tff0fjxfm2dd2psk7cvs27wnjvlgh",
            "size": 8122,
            "tx_count": 5,
            "output": "2853987258",
            "fees": "1592853",
            "block_vrf": "vrf_vk1yy9xgcdvzk4xr2ymnhdrq42c07lt04s0kw432x7wamzele7k9rsqy2eeml",
            "op_cert": "09f4f7d2f7dd2a2cc56f574653538b4fcd314d8822fe09c64e7b9f67c793b837",
            "op_cert_counter": "12",
            "previous_block": "9b8b204705f21fbb64fc1663b044c11a63a3bd341c3020282753f6092297d05c",
            "next_block": "777b9346d3e5e95dcae1446471ad3402df836ba474473fd633c8df912bbc3f6e",
            "confirmations": 380386,
        },
        {
            "time": 1735708513,
            "height": 11293701,
            "hash": "777b9346d3e5e95dcae1446471ad3402df836ba474473fd633c8df912bbc3f6e",
            "slot": 144142222,
            "epoch": 531,
            "epoch_slot": 113422,
            "slot_leader": "pool1rjxdqghfjw5rv6lxg8qhedkechvfgnsqhl8rrzwck9g45n43yql",
            "size": 19388,
            "tx_count": 18,
            "output": "47767525335",
            "fees": "5905418",
            "block_vrf": "vrf_vk1fk9hzkejnmaxw8al4lj78zvca7u7xlrvfm7jar79dw9k9v4q5qysfg3l5y",
            "op_cert": "0bc647dcef90fb1f1f6efc9aef013be93cb8672d5b318af987d15b57e62a302a",
            "op_cert_counter": "21",
            "previous_block": "33b2b6733638aaff1e40de61e33d8fe22a4b789375b553f801696997ca1295b2",
            "next_block": "4e660569f4b4fe7b56f15f43df02aece7de729e5880232401889f2ce439cede8",
            "confirmations": 380385,
        },
    ]

    cardano_blocks_dto_list: list[CardanoBlocksDTO] = [CardanoBlocksDTO.from_raw_cardano_blocks(RawBlockfrostCardanoBlockInfo.model_validate(raw_cardano_blocks)) for raw_cardano_blocks in sample_raw_cardano_blocks_list]
    cardano_block_dao: CardanoBlockDAO = CardanoBlockDAO(
        connection_string=connection_string
    )

    async def run_test() -> None:
        async with cardano_block_dao._engine.begin() as conn:
            await cardano_block_dao.insert_blocks(async_connection=conn, input=cardano_blocks_dto_list)
            print("Blocks inserted successfully.")

    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(run_test())



