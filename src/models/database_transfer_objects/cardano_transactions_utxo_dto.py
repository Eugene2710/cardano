import uuid

from pydantic import BaseModel
from datetime import datetime
from src.models.blockfrost_models.cardano_transaction_utxo import TransactionUTxO


class TxAmountDTO(BaseModel):
    id: uuid.UUID
    parent_id: uuid.UUID
    tx_utxo_hash: str
    unit: str
    quantity: str
    created_at: datetime


class CardanoTxUtxoInputDTO(BaseModel):
    id: uuid.UUID
    hash: str  # transaction hash (same as `hash` in parent)
    address: str
    tx_utxo_hash: str
    output_index: int
    data_hash: str | None = None
    inline_datum: str | None = None
    reference_script_hash: str | None = None
    collateral: bool
    reference: bool | None = None
    created_at: datetime
    amounts: list[TxAmountDTO]


class CardanoTxUtxoOutputDTO(BaseModel):
    id: uuid.UUID
    hash: str  # transaction hash (same as `hash` in parent)
    address: str
    output_index: int
    data_hash: str | None = None
    inline_datum: str | None = None
    collateral: bool
    reference_script_hash: str | None = None
    consumed_by_tx: str | None = None
    created_at: datetime
    amounts: list[TxAmountDTO]


class CardanoTransactionUtxoDTO(BaseModel):
    """
    - convert time from unix to
    - include a created_at column of type datetime to specify the time cardano transaction utxo was ingested
    """
    hash: str # tx_hash
    created_at: datetime
    inputs: list[CardanoTxUtxoInputDTO]
    outputs: list[CardanoTxUtxoOutputDTO]

    @staticmethod
    def from_raw_cardano_tx_utxo(hash: str, input: TransactionUTxO) -> "CardanoTransactionUtxoDTO":
        parent_hash = hash
        input_dtos: list[CardanoTxUtxoInputDTO] = []
        for inp in input.inputs:
            input_id = uuid.uuid4()
            amounts = [
                TxAmountDTO(
                    id=uuid.uuid4(),
                    parent_id=input_id,
                    tx_utxo_hash=inp.tx_hash,
                    unit=a.unit,
                    quantity=a.quantity,
                    created_at=datetime.utcnow()
                ) for a in inp.amount
            ]
            input_dtos.append(
                CardanoTxUtxoInputDTO(
                    id=input_id,
                    hash=parent_hash,
                    address=inp.address,
                    tx_utxo_hash=inp.tx_hash,
                    output_index=inp.output_index,
                    data_hash=inp.data_hash,
                    inline_datum=inp.inline_datum,
                    reference_script_hash=inp.reference_script_hash,
                    collateral=inp.collateral,
                    # reference=inp.reference if inp.reference else None,
                    amounts=amounts,
                    created_at=datetime.utcnow()
                )
            )

        output_dtos: list[CardanoTxUtxoOutputDTO] = []
        for out in input.outputs:
            output_id = uuid.uuid4()
            amounts = [
                TxAmountDTO(
                    id=uuid.uuid4(),
                    parent_id=output_id,
                    tx_utxo_hash=parent_hash,
                    unit=a.unit,
                    quantity=a.quantity,
                    created_at=datetime.utcnow()
                ) for a in out.amount
            ]
            output_dtos.append(
                CardanoTxUtxoOutputDTO(
                    id=output_id,
                    hash=parent_hash,
                    address=out.address,
                    output_index=out.output_index,
                    data_hash=out.data_hash,
                    inline_datum=out.inline_datum,
                    collateral=out.collateral,
                    reference_script_hash=out.reference_script_hash,
                    consumed_by_tx=out.consumed_by_tx,
                    amounts=amounts,
                    created_at=datetime.utcnow()
                )
            )

        return CardanoTransactionUtxoDTO(
            hash=input.hash,
            created_at=datetime.utcnow(),
            inputs=input_dtos,
            outputs=output_dtos,
        )


