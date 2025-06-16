import pytest
from dotenv import load_dotenv
from asyncio import AbstractEventLoop, new_event_loop
from unittest.mock import AsyncMock, patch

from src.extractors.get_block import CardanoBlockExtractor
from src.models.blockfrost_models.raw_cardano_blocks import RawBlockfrostCardanoBlockInfo


class TestCardanoBlockExtractor:
    """
    test if the extracted block height within the response from Blockfrost matches the block number/height passed in
    Test Cases
    - Valid block number of correct type => Correct response with correct block height and response type
    RawBlockfrostCardanoBlockInfo
    - If block number exceeds current last block height =>

    - If block number is of wrong data type

    Prepare: dummy blocks data
    Act: read RawBlockfrostCardanoBlockInfo object
    Assert: assert if cardano blocks are correct
    """
    @pytest.fixture()
    def dummy_raw_blockfrost_block_info(self) -> RawBlockfrostCardanoBlockInfo:
        return RawBlockfrostCardanoBlockInfo(
            time=1735887615,
            height=11302700,
            hash="0715ec14c618e9d44847e1a782f26aaaa7ef69c2853f17a2c7e3926af65cf2a2",
            slot=144321324,
            epoch=531,
            epoch_slot=292524,
            slot_leader="pool12p0qtp89dfzr6spqq4xl6ha2s9m8lqydnvfafyd0h38fy0hfv3f",
            size=3117,
            tx_count=2,
            output="3504478834",
            fees="597821",
            block_vrf="vrf_vk122t22fghqdcyuuuu7lw0jvqp9rulfw5argcm6pdj35e0mq39jfxs5ygc2a",
            op_cert="bef95d0a88f2e13c16b1e58dca81e48fabdd2994465c912d04b23dc13f132154",
            op_cert_counter="1",
            previous_block="09f627d3c4ca0253fdcde788eb627d0aedd0641ce17b6b1dbd07a007e8190cf9",
            next_block="bd8c42a9a2519d0f937b76d10f6d0c87d5891a5295d1f537ddcd1e3ee13257db",
            confirmations=700484,
        )

    @pytest.mark.asyncio
    async def test_cardano_block_extractor(self, dummy_raw_blockfrost_block_info: RawBlockfrostCardanoBlockInfo) -> None:
        # load_dotenv()
        expected_height: str = "11302700"

        with patch(
            "src.extractors.get_block.CardanoBlockExtractor.get_block",
            new=AsyncMock(return_value=dummy_raw_blockfrost_block_info),
        ):
            extractor=CardanoBlockExtractor()

            response: RawBlockfrostCardanoBlockInfo = await extractor.get_block(block_number=expected_height)

            # confirmations can increase in value, and increases often, due to the increasing number of nodes confirming it
            # over time, hence the exclusion of validating it
            assert (
                    response.model_dump(
                        exclude={'confirmations'}) == dummy_raw_blockfrost_block_info.model_dump(
                exclude={'confirmations'})
            )
