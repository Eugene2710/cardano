import pandas as pd
from datetime import datetime
import logging
from typing import Any
from src.utils.logging_utils import setup_logging
from src.models.database_transfer_objects.cardano_transactions import CardanoTransactionsDTO

logger = logging.getLogger(__name__)
setup_logging(logger)


class TransformCardanoTransactionsDTOToDf:
    """
    Responsible for transforming a list of cardano_tx_dto into a pandas DataFrame
    , so that it can converted to bytesIO outside of this class in main pipeline
    """
    @staticmethod
    def transform(cardano_tx_dto_list: list[CardanoTransactionsDTO]) -> pd.DataFrame:
        records: list[dict[str, Any]] = []
        for dto in cardano_tx_dto_list:
            record: dict[str, Any] = {
                "hash": dto.hash,
                "block": dto.block,
                "block_height": dto.block_height,
                "block_time": dto.block_time,
                "delegation_count": dto.delegation_count,
                "deposit": dto.deposit,
                "fees": dto.fees,
                "index": dto.index,
                "invalid_before": dto.invalid_before,
                "invalid_hereafter": dto.invalid_hereafter,
                "mir_cert_count": dto.mir_cert_count,
                "pool_retire_count": dto.pool_retire_count,
                "pool_update_count": dto.pool_update_count,
                "redeemer_count": dto.redeemer_count,
                "size": dto.size,
                "slot": dto.slot,
                "stake_cert_count": dto.stake_cert_count,
                "utxo_count": dto.utxo_count,
                "valid_contract": dto.valid_contract,
                "withdrawal_count": dto.withdrawal_count,
                "asset_mint_or_burn_count": dto.asset_mint_or_burn_count,
                "created_at": pd.to_datetime(dto.created_at),
            }
            records.append(record)

        df:pd.DataFrame = pd.DataFrame.from_records(records)
        return df


if __name__ == "__main__":
    sample_tx_dto: list[CardanoTransactionsDTO] = [
        CardanoTransactionsDTO(
            hash="b1e4f64ce8b378a6b12913840d7ab5304d3adfa06a6ea767fa355da3e1e589dd",
            block="30c6bba25fcf7b0cbd821efd1d18cbd48bbba5ff68055971d9a8d0a348cb4e92",
            block_height=11292702,
            block_time=datetime(2024, 12, 31, 23, 41, 7),
            delegation_count=0,
            deposit="0",
            fees="183600",
            index=9,
            invalid_before=None,
            invalid_hereafter="144125752",
            mir_cert_count=0,
            pool_retire_count=0,
            pool_update_count=0,
            redeemer_count=0,
            size=261,
            slot=144122176,
            stake_cert_count=0,
            utxo_count=3,
            valid_contract=True,
            withdrawal_count=0,
            asset_mint_or_burn_count=0,
            created_at=datetime(2025, 4, 30, 15, 18, 39, 630720)
        )
    ]
    res: pd.DataFrame = TransformCardanoTransactionsDTOToDf.transform(cardano_tx_dto_list=sample_tx_dto)
    print(res)