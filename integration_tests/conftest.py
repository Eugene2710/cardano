"""
contains fixtures/reusable components common to all tests
"""
import os
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from dotenv import load_dotenv, find_dotenv

env_file = find_dotenv("test.env") or ""
if env_file:
    load_dotenv(env_file, override=False)
else:
    raise RuntimeError("Cannot find test.env – set ASYNC_PG_CONNECTION_STRING")


@pytest_asyncio.fixture(scope="session")
async def pg_engine() -> AsyncEngine:
    """
    sets up an AsyncEngine and tears it down when you are done with it
    """
    engine: AsyncEngine = create_async_engine(os.getenv("ASYNC_PG_CONNECTION_STRING"))
    yield engine
    await engine.dispose()




