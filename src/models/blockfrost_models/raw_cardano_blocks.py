from pydantic import BaseModel


class BlockfrostCardanoBlockInfo(BaseModel):
    """
    Represents the block information
    Source: https://docs.blockfrost.io/#tag/cardano--blocks/GET/blocks/{hash_or_number}
    """

    time: int
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
