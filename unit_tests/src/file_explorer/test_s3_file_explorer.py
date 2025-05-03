import io
from datetime import datetime
from typing import Generator

import freezegun
from pandas.testing import assert_frame_equal
import pandas as pd
from mypy_boto3_s3 import Client
import pytest
import boto3
from moto import mock_aws
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.file_info.file_info import FileInfo

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

    def test_upload_buffer(
        self, bucket_name: str, s3_client: Client, s3_explorer: S3Explorer
    ) -> None:
        """
        GIVEN CSV file stored in a buffer (io.BytesIO)
        WHEN I upload buffer to S3
        THEN I expect to be able to get the test file from S3
        """
        buffer = io.BytesIO(
            b"instrument,timestamp,price,created_at\nA,2025-05-02 00:00:00,1.00,2025-05-02 00:00:00"
        )
        s3_explorer.upload_buffer(
            bytes_io=buffer,
            source_path="sample_source_path/sample_file.csv",
        )
        actual_buffer: io.BytesIO = io.BytesIO()
        s3_client.download_fileobj(
            bucket_name, "sample_source_path/sample_file.csv", actual_buffer
        )
        actual_buffer.seek(0)
        actual_df: pd.DataFrame = pd.read_csv(actual_buffer)
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
        actual_df["timestamp"] = pd.to_datetime(actual_df["timestamp"])
        actual_df["created_at"] = pd.to_datetime(actual_df["created_at"])
        assert_frame_equal(actual_df, expected_df)

    def test_download_to_buffer(
        self, bucket_name: str, s3_client: Client, s3_explorer: S3Explorer
    ) -> None:
        """
        GIVEN CSV file in S3
        WHEN I download to buffer
        THEN I expect to be able to get the test file from S3 into the buffer
        """
        buffer = io.BytesIO(
            b"instrument,timestamp,price,created_at\nA,2025-05-02 00:00:00,1.00,2025-05-02 00:00:00"
        )
        s3_client.upload_fileobj(
            buffer, bucket_name, "sample_source_path/sample_file.csv"
        )
        actual_buffer = s3_explorer.download_to_buffer(
            s3_path="sample_source_path/sample_file.csv",
        )
        actual_df: pd.DataFrame = pd.read_csv(actual_buffer)
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
        actual_df["timestamp"] = pd.to_datetime(actual_df["timestamp"])
        actual_df["created_at"] = pd.to_datetime(actual_df["created_at"])
        assert_frame_equal(actual_df, expected_df)

    @freezegun.freeze_time(time_to_freeze=datetime(2025, 5, 3))
    def test_list_files(
        self, bucket_name: str, s3_client: Client, s3_explorer: S3Explorer
    ) -> None:
        """
        GIVEN CSV file in S3
        WHEN I list S3 files
        THEN I expect to be able to get the test file's FileInfo
        """
        buffer = io.BytesIO(
            b"instrument,timestamp,price,created_at\nA,2025-05-02 00:00:00,1.00,2025-05-02 00:00:00"
        )
        s3_client.upload_fileobj(
            buffer, bucket_name, "sample_source_path/sample_file.csv"
        )
        file_infos: list[FileInfo] = list(
            s3_explorer.list_files(
                "sample_source_path", last_modified_date=datetime(2025, 5, 2)
            )
        )
        expected_file_infos: list[FileInfo] = [
            FileInfo(
                file_path="sample_source_path/sample_file.csv",
                modified_date=datetime(2025, 5, 3),
            )
        ]
        assert file_infos == expected_file_infos
