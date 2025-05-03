import io
import asyncio
import json
import os
from typing import Any
import boto3
from dotenv import load_dotenv
from pprint import pprint
from retry import retry

from src.models.blockfrost_models.raw_cardano_blocks import RawBlockfrostCardanoBlockInfo
from src.models.database_transfer_objects.cardano_blocks import CardanoBlocksDTO
from src.file_explorer.s3_file_explorer import S3Explorer

class CardanoBlockS3Extractor:
    def __init__(
            self,
            s3_explorer: S3Explorer
    ):
        self._s3_explorer: S3Explorer = s3_explorer

    @retry(
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    def get_block_from_s3(self, s3_path: str) -> list[CardanoBlocksDTO]:
        """
        - get specified json file from s3 using S3Explorer.download_to_buffer
        - add json file to database
        NOTE: This only extracts 1 file every time it is called
        the file can contain either batch of blocks or 1 block
        """
        # download the JSON file into a BytesIO buffer
        buffer: io.BytesIO = self._s3_explorer.download_to_buffer(s3_path)
        # buffer: io.BytesIO = self._s3_explorer.download_to_buffer(s3_path=f"cardano/blocks/{end_block_height}/cardano_blocks_{end_block_height}.json")
        # load the JSON content into a list of dict
        block_data: list[dict[str, Any]] = json.load(buffer)

        dtos: list[CardanoBlocksDTO] = []
        records_to_insert = []
        for record in block_data:
            raw_block: RawBlockfrostCardanoBlockInfo = RawBlockfrostCardanoBlockInfo(**record)
            dto: CardanoBlocksDTO = CardanoBlocksDTO.from_raw_cardano_blocks(raw_block)
            dtos.append(dto)

        return dtos


if __name__ == "__main__":
    load_dotenv()
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )
    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""), client=client
    )
    extractor: CardanoBlockS3Extractor = CardanoBlockS3Extractor(s3_explorer=s3_explorer)
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    res: list[CardanoBlocksDTO] = extractor.get_block_from_s3(s3_path="cardano/blocks/6000/cardano_blocks_6000.json")
    pprint(res)

