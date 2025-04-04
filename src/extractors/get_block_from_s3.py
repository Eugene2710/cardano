import io
import asyncio
import json

import os
from typing import Any
from dotenv import load_dotenv
from pprint import pprint

from tenacity import retry, wait_fixed, stop_after_attempt
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
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def get_block_from_s3(self, end_block_height: str) -> list[CardanoBlocksDTO]:
        """
        - get specified json file from s3 using S3Explorer.download_to_buffer
        - add json file to database
        NOTE: This only extracts 1 file every time it is called
        the file can contain either batch of blocks or 1 block
        """
        # download the JSON file into a BytesIO buffer
        buffer: io.BytesIO = self._s3_explorer.download_to_buffer(s3_path=f"cardano/blocks/{end_block_height}/cardano_blocks_{end_block_height}.json")
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
    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""),
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )
    extractor: CardanoBlockS3Extractor = CardanoBlockS3Extractor(s3_explorer=s3_explorer)
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    res: list[CardanoBlocksDTO] = event_loop.run_until_complete(
        extractor.get_block_from_s3(end_block_height="11293700")
    )
    pprint(res)

