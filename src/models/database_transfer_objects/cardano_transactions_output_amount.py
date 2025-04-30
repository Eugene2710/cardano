import uuid
from pydantic import BaseModel
from datetime import datetime

from src.models.blockfrost_models.raw_cardano_transactions import CardanoTransactions


class CardanoTransactionsOutputAmountDTO(BaseModel):
    id: uuid.UUID
    hash: str
    unit: str
    quantity: str
    created_at: datetime

    @staticmethod
    def from_raw_cardano_tx(
        hash: str, input: CardanoTransactions
    ) -> "CardanoTransactionsOutputAmountDTO":
        return CardanoTransactionsOutputAmountDTO(
            id=uuid.uuid4(),
            hash=input.hash,
            unit=input.unit,
            quantity=input.quantity,
            created_at=datetime.utcnow()
        )