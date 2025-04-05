from typing import Any
from pydantic import BaseModel


class CardanoTransactionOutput(BaseModel):
    unit: str
    quantity: str


class CardanoTransactions(BaseModel):
    block: str
    block_height: int
    block_time: int
    delegation_count: int
    deposit: str
    fees: str
    hash: str
    index: int
    invalid_before: str | None = None
    invalid_hereafter: str | None = None
    mir_cert_count: int
    output_amount: list[CardanoTransactionOutput]
    pool_retire_count: int
    pool_update_count: int
    redeemer_count: int
    size: int
    slot: int
    stake_cert_count: int
    utxo_count: int
    valid_contract: bool
    withdrawal_count: int
    asset_mint_or_burn_count: int

    @staticmethod
    def from_json(input: dict[str, Any]) -> "CardanoTransactions":
        return CardanoTransactions.model_validate(
            {
                **input,
                "output_amount": [
                    CardanoTransactionOutput.model_validate(single_output_amount)
                    for single_output_amount in input["output_amount"]
                ],
            }
        )
