import io
from io import BytesIO
import os
from dotenv import load_dotenv
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import (Table, text)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from sqlalchemy.exc import OperationalError
from asyncio import new_event_loop, AbstractEventLoop
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type
from src.utils.logging_utils import setup_logging

from database_management.cardano.cardano_tables import  cardano_tx_utxo_output_amount_table

logger = logging.getLogger(__name__)
setup_logging(logger)


class CardanoTxUtxoInputAmtDAO:
    """
    Responsible for inserting a list of CardanoTransactionUtxoDTO into DB
    """
    def __init__(self, connection_string: str, table: Table) -> None:
        self._engine: AsyncEngine = create_async_engine(connection_string)
        self._table: Table = table
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
        temporary_table_name = f"cardano_tx_utxo_{datetime.utcnow().strftime('%Y%m%s%H%M%S')}"
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
    async def copy_tx_utxo_input_amt_to_db(self, async_connection: AsyncConnection, data_buffer: BytesIO) -> None:
        # this 'raw_adapt' is not the real asyncpg.Connection:
        raw_adapt = await async_connection.get_raw_connection()
        # ***WARNING***: private attribute; can break in future SQLAlchemy versions
        actual_asyncpg_conn = raw_adapt._connection

        # 1. Read and sanitise csv into DataFrame
        data_buffer.seek(0)
        df: pd.DataFrame = pd.read_csv(data_buffer, dtype=str, encoding="utf-8-sig")
        # Remove non‑printable characters / trim spaces from header names
        df.columns = (
            df.columns.str.replace(r"[\x00-\x1F]", "", regex=True)
            .str.strip()
        )

        # 2. Re-order columns to match DB table's physical order
        expected_columns: list[str] = [col.name for col in self._table.columns]
        missing = set(expected_columns) - set(df.columns)
        if missing:
            raise ValueError(
                f"CSV is missing required column(s): {', '.join(sorted(missing))}"
            )
        copy_columns: list[str] = list(df.columns)
        df = df[copy_columns]

        # 3. Stream with COPY -> temp table
        new_buffer: BytesIO = io.BytesIO()
        df.to_csv(new_buffer, index=False, header=True)
        new_buffer.seek(0)

        await actual_asyncpg_conn.copy_to_table(
            table_name=self._temp_table_name,
            source=new_buffer,
            columns=copy_columns,
            format="csv",
            header=True,
        )

        # 4. Merge temp into real table
        col_sql = ",".join(f'"{col}"' for col in copy_columns)
        insert_clause = text(
            f"""
                INSERT INTO {self._table.name} ({col_sql})
                SELECT {col_sql}
                FROM {self._temp_table_name}
                ON CONFLICT (id) DO NOTHING
            """
        )
        await async_connection.execute(insert_clause)
        logger.info("%s → inserted %d rows", self._table.name, len(df))

    @property
    def table(self):
        return self._table


if __name__ == "__main__":
    load_dotenv()
    connection_string: str = os.getenv("ASYNC_PG_CONNECTION_STRING")

    cardano_tx_utxo_sub_dao: CardanoTxUtxoInputAmtDAO = CardanoTxUtxoInputAmtDAO(
        connection_string=connection_string, table=cardano_tx_utxo_output_amount_table
    )

    async def run_copy_test() -> None:
        async with cardano_tx_utxo_sub_dao._engine.begin() as conn:
            with open("cardano_tx_utxo_csv/cardano_tx_utxo_input_amt.csv", "rb") as f:
                data_buffer = BytesIO(f.read())
            await cardano_tx_utxo_sub_dao.create_temp_table(async_connection=conn)
            await cardano_tx_utxo_sub_dao.copy_tx_utxo_input_amt_to_db(async_connection=conn, data_buffer=data_buffer)
            print("Copied to database successfully")

    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(run_copy_test())

