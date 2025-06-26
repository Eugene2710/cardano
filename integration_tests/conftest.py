"""
contains fixtures/reusable components common to all tests
"""
import os
import pytest_asyncio
import pytest
from sqlalchemy import Engine,create_engine, text, TextClause, Table
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from dotenv import load_dotenv, find_dotenv

env_file = find_dotenv("test.env") or ""
if env_file:
    load_dotenv(env_file, override=False)
else:
    raise RuntimeError("Cannot find test.env – set ASYNC_PG_CONNECTION_STRING")

# def connection_string(self) -> str:
#     load_dotenv()
#     return os.getenv("ASYNC_PG_CONNECTION_STRING")


@pytest_asyncio.fixture(scope="session")
async def pg_engine() -> AsyncEngine:
    """
    sets up an AsyncEngine and tears it down when you are done with it
    """
    engine: AsyncEngine = create_async_engine(os.getenv("ASYNC_PG_CONNECTION_STRING"))
    yield engine
    await engine.dispose()


@pytest.fixture
def db_name() -> str:
    """
    create and return a unique dab name
    """
    return "test_db_" + str(uuid.uuid4()).replace("-", "_")


@pytest.fixture
def create_and_drop_db_table(db_name: str, input_tables: list[Table]) -> None:
    """
    Responsible for creating a unique DB to be used for integration tests by yielding it and dropping it after use
    input_tables param is to be created as a separate pytest fixture in the individual test files
    """
    # default db engine is responsible for creating a database before dropping it
    default_db_engine: Engine | None = create_engine("postgresql://localhost:5432/postgres")
    test_db_engine: Engine | None = None
    try:
        with default_db_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            text_clause: TextClause = text(f"CREATE DATABASE {db_name}")
            conn.execute(text_clause)
        default_db_engine.dispose()
        test_db_engine = create_engine(f"postgresql://localhost:5432/{db_name}")
        with test_db_engine.begin() as conn:
            for table in input_tables:
                table.create(conn)
        test_db_engine.dispose()
        yield
    except Exception as e:
        raise e
    finally:
        # ensure all connections to the DB are closed before dropping database
        test_db_engine.dispose()
        with default_db_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            drop_text_clause: TextClause = text(f"DROP DATABASE {db_name}")
            conn.execute(drop_text_clause)
        default_db_engine.dispose()
