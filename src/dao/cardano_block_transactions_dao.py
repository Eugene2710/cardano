import io
import os
from dotenv import load_dotenv
import logging
import pandas as pd
from typing import Any
from datetime import datetime
from sqlalchemy import (Table, text)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from asyncio import new_event_loop, AbstractEventLoop
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type
from sqlalchemy.exc import OperationalError
from io import BytesIO
import ast

from database_management.cardano.cardano_tables import cardano_block_transactions_table
from src.models.database_transfer_objects.cardano_block_transactions import CardanoBlocksTransactionsDTO
from src.utils.logging_utils import setup_logging

logger = logging.getLogger(__name__)
setup_logging(logger)


class CardanoBlockTransactionsDAO:
    """
    Responsible for inserting a list of CardanoBlocksTransactionsDTO into the DB
    """
    def __init__(self, connection_string: str) -> None:
        self._engine: AsyncEngine = create_async_engine(connection_string)
        self._table: Table = cardano_block_transactions_table
        self._temp_table_name: str | None = None

    @retry(
        retry=retry_if_exception_type(OperationalError),
        wait=wait_fixed(0.01),  # ~10ms before attempts
        stop=stop_after_attempt(5),  # equivalent to 5 retries
        reraise=True
    )
    async def insert_block_transactions(self, async_connection: AsyncConnection, input: list[CardanoBlocksTransactionsDTO]) -> None:
        if not input:
            print("insert_blocks: No input. Exiting")
            return

        records: list[dict[str, Any]] = [
            {
                "block": block_tx.block,
                "tx_hash": block_tx.tx_hash,
                "created_at": block_tx.created_at
            } for block_tx in input
        ]
        try:
            # leave batching to etl pipeline file to retain single responsibility principle
            stmt = (
                insert(self._table).values(records).on_conflict_do_nothing(index_elements=["block"])
            )
            await async_connection.execute(stmt)
        except OperationalError as e:
            # Intermittent DB connection error. Retry for this.
            logger.warning(f"Insertion failed due to Operational Error. {e} Retrying...")
            raise
        except Exception:
            # Fallback exception. Do not retry for this.
            logging.exception("Insertion failed due to unexpected Exception. Retrying...")
            raise

    @retry(
        retry=retry_if_exception_type(OperationalError),
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def create_temp_table(self, async_connection: AsyncConnection) -> None:
        """
        Create a temp table, add the timestamp to the back of temporary table
        So each transaction has its own temp table
        temporary_table_name = f"cardano_block_tx_{datetime.utcnow().strftime('%Y%m%s%H%M%S')}"
        """
        self._temp_table_name = (
            f"{self._table.name}_{datetime.utcnow().strftime('%Y%m%s%H%M%S')}"
        )
        create_table = text(
            f"CREATE TEMPORARY TABLE {self._temp_table_name}(LIKE {self._table.name}) ON COMMIT DROP"
        )
        try:
            await async_connection.execute(create_table)
        except OperationalError as e:
            # Intermittent DB connection errors. Retry for this.
            logger.warning(f"Temporary table creation failed due to OperationalError. {e} Retrying..")
            raise
        except Exception:
            # Fallback exception. Do not retry for this.
            logging.exception("Temporary table creation failed due to unexpected Exception. Retrying..")
            raise

    @retry(
        retry=retry_if_exception_type(OperationalError),
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def copy_blocks_to_db(self, async_connection: AsyncConnection, data_buffer: BytesIO) -> None:
        # rewind and prepare column list without created_at
        data_buffer.seek(0)

        df = pd.read_csv(data_buffer)
        df["tx_hash"] = df['tx_hash'].apply(
            lambda s: '{' + ','.join(ast.literal_eval(s)) + '}'
        )

        new_buffer = io.BytesIO()
        df.to_csv(new_buffer, index=False)
        new_buffer.seek(0)

        # This 'raw_adapt' is not the real asyncpg.Connection:
        raw_adapt = await async_connection.get_raw_connection()
        # ***WARNING***: private attribute; can break in future SQLAlchemy versions
        actual_asyncpg_conn = raw_adapt._connection

        columns = [col.name for col in self._table.columns]
        await actual_asyncpg_conn.copy_to_table(
            table_name=self._temp_table_name,
            source=new_buffer,
            columns=columns,
            format="csv",
            header=True,
        )
        col_names = columns
        col_names_str = ",".join(f'"{col}"' for col in col_names)
        insert_clause = text(
            f"""
                INSERT INTO {self._table.name} ({col_names_str})
                SELECT {col_names_str}
                FROM {self._temp_table_name}
                ON CONFLICT (block) DO NOTHING
            """
        )
        await async_connection.execute(insert_clause)

    @property
    def table(self):
        return self._table


if __name__ == "__main__":
    load_dotenv()
    connection_string: str = os.getenv("ASYNC_PG_CONNECTION_STRING")
    sample_raw_cardano_block_tx_list = [
        CardanoBlocksTransactionsDTO(
            block=11292701,
            tx_hash=[
                "c41f1c3dbdc7c03ea767a58cbccf672189772169e7755333a772f8098af3bff0",
                "3517b4662fd8b98056d69c41b755cd651d1206dbfa2c5b2d9e3c447eb7165752",
                "66987b0f9ad5d403d0516ab2a6c924f939cb438aacd7248cc9c96cc96890c21b",
                "121b9196bc4914a0023dbe914fe558c2e14de140f4727f9af862ef19461c8ad6",
                "4bb0528b56f5537bfb78de880a612ab199202d9be1c4f443b679670232ff7800",
                "b0bebe8b342cd78ee0914732ae16bcbd69bacfb723e0a3143dc219649cae2438",
                "e938352a8a416d2ef42ed2789c695694be6b44c1381b0727f11ab69e162ac63e",
                "282558ec4590bf59c5c6337dffdc71e6eb0fc46ff3f172f6cf023fad057f334e",
                "2a3aa7b07d5558307b9056627a41f8e8b7cc0bdbe5b30c5ad5855733c1125631",
                "17825ca8e23be97899d4fe52ded89b821cefd19d392619fa55f3f9d4e46ffe27",
                "9d5ab292698e01962ea4782dddddd6a8265278bbc0333c968c9b92d3705c2737",
                "d69456cd042304f5891e2c670731864c8e1f90994bfc75846d12979369246061",
                "aad64cecbf1a7901e9a1cb2eeb1de7a10ec211c840a494796c02e53564edc8fe",
                "0a11c9673f5621f99c0004f28e2ea6a980aceba4200d508ec5cca5b5ed3ba861",
                "736c9f55ec9582692454f4f9c02a9b20601b2dc812eaaa08feab5e067f6f5d84",
                "02e7568c38cdce71489292888d07ebb2f77e741e7c546d9cee513196aa4fe7f8",
                "36853d143c862f96ae5554788b1b8c0f976344bf2db5b97ffbbf6e74777850ff",
                "7c80bcae8ee667f4da216c732cba8a7907f190bcfd69680bf25e6252ea60ff7a",
                "7854fd6aac56308764ac2996215c5db6402859e6ac28d04d30e8cc2985870ade"
            ],
            created_at=datetime.utcnow(),
        )
    ]
    records = [dto.model_dump() for dto in sample_raw_cardano_block_tx_list]
    print("model dumped successfully")


    # ----- TEST FOR CSV COPY -----
    # Create a sample file for csv copy
    df = pd.DataFrame(records)
    df.to_csv("cardano_block_tx.csv", index=False)

    cardano_block_tx_dao: CardanoBlockTransactionsDAO = CardanoBlockTransactionsDAO(
        connection_string=connection_string
    )

    async def run_copy_test() -> None:
        async with cardano_block_tx_dao._engine.begin() as conn:
            with open("cardano_block_tx.csv", "rb") as f:
                data_buffer = BytesIO(f.read())
            await cardano_block_tx_dao.create_temp_table(async_connection=conn)
            await cardano_block_tx_dao.copy_blocks_to_db(async_connection=conn, data_buffer=data_buffer)
            print("Blocks copied to DB successfully.")


    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(run_copy_test())