from pydantic import BaseModel
from datetime import datetime
from src.models.blockfrost_models.raw_cardano_transactions import CardanoTransactions


class CardanoTransactionsDTO(BaseModel):
    """
    - convert time from unix to datetime
    - include a created_at column of type datetime to specify the time the cardano transaction was ingested
    """
    hash: str # tx_hash
    block: str # block hash
    block_height: int
    block_time: datetime # convert from unix timestamp in raw transactions to datetime
    delegation_count: int
    deposit: str
    fees: str
    index: int
    invalid_before: str | None = None
    invalid_hereafter: str | None = None
    mir_cert_count: int
    # output_amount: list[CardanoTransactionOutput]
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
    created_at: datetime

    @staticmethod
    def from_raw_cardano_tx(
        hash: str, input: CardanoTransactions
    ) -> "CardanoTransactionsDTO":
        return CardanoTransactionsDTO(
            hash=input.hash,
            block=input.block,
            block_height=input.block_height,
            block_time=input.block_time,
            delegation_count=input.delegation_count,
            deposit=input.deposit,
            fees=input.fees,
            index=input.index,
            invalid_before=input.invalid_before,
            invalid_hereafter=input.invalid_hereafter,
            mir_cert_count=input.mir_cert_count,
            pool_retire_count=input.pool_retire_count,
            pool_update_count=input.pool_update_count,
            redeemer_count=input.redeemer_count,
            size=input.size,
            slot=input.slot,
            stake_cert_count=input.stake_cert_count,
            utxo_count=input.utxo_count,
            valid_contract=input.valid_contract,
            withdrawal_count=input.withdrawal_count,
            asset_mint_or_burn_count=input.asset_mint_or_burn_count,
            created_at=datetime.utcnow()
        )