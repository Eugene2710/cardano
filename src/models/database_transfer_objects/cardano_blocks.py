from pydantic import BaseModel
from datetime import datetime

from src.models.blockfrost_models.raw_cardano_blocks import RawBlockfrostCardanoBlockInfo


class CardanoBlocksDTO(BaseModel):
    """
    - convert time from unix to datetime
    - include a created_at column of type datetime to specify the time the cardano block was ingested
    """
    time: datetime
    height: int
    hash: str
    slot: int
    epoch: int | None
    epoch_slot: int | None
    slot_leader: str
    size: int
    tx_count: int
    output: str | None
    fees: str | None
    block_vrf: str | None
    op_cert: str | None
    op_cert_counter: str | None
    previous_block: str | None
    next_block: str | None
    confirmations: int
    created_at: datetime

    @staticmethod
    def from_raw_cardano_blocks(
            input: RawBlockfrostCardanoBlockInfo
    ) -> "CardanoBlocksDTO":
        return CardanoBlocksDTO(
            time=datetime.fromtimestamp(input.time, tz=timezone.utc),
            height=input.height,
            hash=input.hash,
            slot=input.slot,
            epoch=input.epoch,
            epoch_slot=input.epoch_slot,
            slot_leader=input.slot_leader,
            size=input.size,
            tx_count=input.tx_count,
            output=input.output,
            fees=input.fees,
            block_vrf=input.block_vrf,
            op_cert=input.op_cert,
            op_cert_counter=input.op_cert_counter,
            previous_block=input.previous_block,
            next_block=input.next_block,
            confirmations=input.confirmations,
            created_at=datetime.utcnow(),
        )