from pydantic import BaseModel
from datetime import datetime
from src.models.blockfrost_models.cardano_block_transactions import CardanoBlockTransactions


class CardanoBlocksTransactionsDTO(BaseModel):
    """
    - appends a block_height to the original list of tx which got transformed to dict
    - appends a created_at datetime
    """
    block: int
    tx_hash: list[str]
    created_at: datetime

    @staticmethod
    def from_raw_cardano_blocks_tx(
            input: CardanoBlockTransactions
    ) -> "CardanoBlocksTransactionsDTO":
        return CardanoBlocksTransactionsDTO(
            block=input.block_height,
            tx_hash=input.tx_hash,
            created_at=datetime.utcnow()
        )
