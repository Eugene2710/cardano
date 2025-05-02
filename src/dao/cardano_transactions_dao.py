import io
from io import BytesIO
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

from asyncio import new_event_loop, AbstractEventLoop
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type
from src.utils.logging_utils import setup_logging
from database_management.cardano.cardano_tables import cardano_transactions_table
from src.models.database_transfer_objects.cardano_transactions import CardanoTransactionsDTO

logger = logging.getLogger(__name__)
setup_logging(logger)


class CardanoTransactionsDAO:
    """
    Responsible for inserting a list of CardanoTransactionsDTO into DB
    """
    def __init__(self, connection_string: str) -> None:
        self._engine: AsyncEngine = create_async_engine(connection_string)
        self._table: Table = cardano_transactions_table
        self._temp_table_name: str | None = None

    @retry(
        retry=retry_if_exception_type(OperationalError),
        wait=wait_fixed(0.01),  # ~10ms before attempts
        stop=stop_after_attempt(5),  # equivalent to 5 retries
        reraise=True
    )
    async def create_temp_table(self, async_connection: AsyncConnection) -> None:
        """
        create a temp table, add the timestamp to the back of temporary table
        so each transaction has is own temp table
        temporary_table_name = f"cardano_tx_{datetime.utcnow().strftime('%Y%m%s%H%M%S')}"
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
    async def copy_tx_to_db(self, async_connection: AsyncConnection, data_buffer: BytesIO) -> None:
        # this 'raw_adapt' is not the real asyncpg.Connection:
        raw_adapt = await async_connection.get_raw_connection()
        # ***WARNING***: private attribute; can break in future SQLAlchemy versions
        actual_asyncpg_conn = raw_adapt._connection

        # rewind and prepare column list without created_at
        data_buffer.seek(0)
        columns: list[str] = [col.name for col in self._table.columns]
        df: pd.DataFrame = pd.read_csv(data_buffer)
        df = df[columns]
        # normalise boolean text → true/false  (Postgres accepts either case)
        df["valid_contract"] = df["valid_contract"].map(
            {True: "true", False: "false", "True": "true", "False": "false"}
        )

        new_buffer: BytesIO = io.BytesIO()
        df.to_csv(new_buffer, index=False, header=True)
        new_buffer.seek(0)

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
                ON CONFLICT (hash) DO NOTHING
            """
        )
        await async_connection.execute(insert_clause)

    @property
    def table(self):
        return self._table


if __name__ == "__main__":
    load_dotenv()
    connection_string: str = os.getenv("ASYNC_PG_CONNECTION_STRING")
    sample_cardano_tx: list[CardanoTransactionsDTO] = [
        CardanoTransactionsDTO(
            hash="b1e4f64ce8b378a6b12913840d7ab5304d3adfa06a6ea767fa355da3e1e589dd",
            block="30c6bba25fcf7b0cbd821efd1d18cbd48bbba5ff68055971d9a8d0a348cb4e92",
            block_height=11292702,
            block_time=datetime(2024, 12, 31, 23, 41, 7),
            delegation_count=0,
            deposit="0",
            fees="183600",
            index=9,
            invalid_before=None,
            invalid_hereafter="144125752",
            mir_cert_count=0,
            pool_retire_count=0,
            pool_update_count=0,
            redeemer_count=0,
            size=261,
            slot=144122176,
            stake_cert_count=0,
            utxo_count=3,
            valid_contract=True,
            withdrawal_count=0,
            asset_mint_or_burn_count=0,
            created_at=datetime(2025, 4, 30, 15, 18, 39, 630720)
        )
    ]
    records = [dto.model_dump() for dto in sample_cardano_tx]
    print("model dumped successfully")

    # ----- TEST FOR CSV COPY -----
    # Create a sample file for csv copy
    df = pd.DataFrame(records)
    df.to_csv("cardano_tx.csv", index=False, header=True)

    cardano_tx_dao: CardanoTransactionsDAO = CardanoTransactionsDAO(
        connection_string=connection_string
    )

    async def run_copy_test() -> None:
        async with cardano_tx_dao._engine.begin() as conn:
            with open("cardano_tx.csv", "rb") as f:
                data_buffer = BytesIO(f.read())
            await cardano_tx_dao.create_temp_table(async_connection=conn)
            await cardano_tx_dao.copy_tx_to_db(async_connection=conn, data_buffer=data_buffer)
            print("Tx copied to database successfully")

    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(run_copy_test())
