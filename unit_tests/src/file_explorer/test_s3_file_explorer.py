import io
from datetime import datetime
from typing import Generator
from pandas.testing import assert_frame_equal
import pandas as pd
from mypy_boto3_s3 import Client
import pytest
import boto3
from moto import mock_aws
from src.file_explorer.s3_file_explorer import S3Explorer

ENDPOINT_URL = "http://localhost:9001"
ACCESS_KEY_ID = "my_dummy_access_key_id"
SECRET_ACCESS_KEY = "my_precious"


class TestS3Explorer:
    @pytest.fixture
    def bucket_name(self) -> str:
        return f"cardano{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    @pytest.fixture
    def s3_client(self, bucket_name: str) -> Generator[Client, None, None]:
        with mock_aws():
            client: Client = boto3.client("s3")
            client.create_bucket(Bucket=bucket_name)
            yield client

    @pytest.fixture
    def s3_explorer(
        self, s3_client: Client, bucket_name: str
    ) -> Generator[S3Explorer, None, None]:
        yield S3Explorer(bucket_name=bucket_name, client=s3_client)

    def test_upload_file(
        self, s3_client: Client, s3_explorer: S3Explorer, bucket_name: str
    ) -> None:
        """
        GIVEN a test file
        WHEN I upload the test file with the S3Explorer
        THEN I expect to be able to get the test file from S3
        """
        s3_explorer.upload_file(
            local_file_path="unit_tests/src/file_explorer/files/sample_file.csv",
            s3_path="sample_source_path/sample_file.csv",
        )
        buffer: io.BytesIO = io.BytesIO()
        s3_client.download_fileobj(
            bucket_name, "sample_source_path/sample_file.csv", buffer
        )
        buffer.seek(0)
        df: pd.DataFrame = pd.read_csv(buffer)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["created_at"] = pd.to_datetime(df["created_at"])
        expected_df: pd.DataFrame = pd.DataFrame(
            [
                {
                    "instrument": "A",
                    "timestamp": datetime(2025, 5, 2),
                    "price": 1.00,
                    "created_at": datetime(2025, 5, 2),
                }
            ]
        )
        expected_df["timestamp"] = pd.to_datetime(expected_df["timestamp"])
        expected_df["created_at"] = pd.to_datetime(expected_df["created_at"])
        assert_frame_equal(df, expected_df)
