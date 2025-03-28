from pydantic import BaseModel
from typing import Any


class Amount(BaseModel):
    """
    Represents an amount object with a unit and its quantity
    e.g.
    {
      "unit": "lovelace",
      "quantity": "42000000"
    },
    {
      "unit": "b0d07d45fe9514f80213f4020e5a61241458be626841cde717cb38a76e7574636f696e",
      "quantity": "12"
    }
    """

    unit: str
    quantity: str


class TransactionInput(BaseModel):
    address: str
    amount: list[Amount]
    tx_hash: str
    output_index: int
    data_hash: str | None = None
    inline_datum: str | None = None
    reference_script_hash: str | None = None
    collateral: bool
    collateral: bool


class TransactionOutput(BaseModel):
    address: str
    amount: list[Amount]
    output_index: int
    data_hash: str | None = None
    inline_datum: str | None = None
    collateral: bool
    reference_script_hash: str | None = None
    consumed_by_tx: str | None = None


class TranscationUTxO(BaseModel):
    """
    the top level model for Transaction UTxO
    """

    hash: str
    inputs: list[TransactionInput]
    output: list[TransactionOutput]

    @staticmethod
    def from_json(data: dict[str, Any]) -> "TranscationUTxO":
        return TranscationUTxO.model_validate(
            {
                **input,
                "inputs": [
                    TranscationUTxO.from_json(single_input)
                    for single_input in data["inputs"]
                ],
                "outputs": [
                    TranscationUTxO.from_json(single_output)
                    for single_output in data["outputs"]
                ],
            }
        )
