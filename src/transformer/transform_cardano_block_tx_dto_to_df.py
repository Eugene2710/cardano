import pandas as pd
from datetime import datetime
import logging
from src.utils.logging_utils import setup_logging
from src.models.database_transfer_objects.cardano_block_transactions import CardanoBlocksTransactionsDTO

logger = logging.getLogger(__name__)
setup_logging(logger)


class TransformCardanoBlockTxDTOToDf:
    """
    Responsible for transforming a list of cardano_block_tx_dto into a pandas DataFrame, so that it can converted to bytesIO outside of this class in main pipeline
    """
    @staticmethod
    def transform(cardano_block_tx_dto_list: list[CardanoBlocksTransactionsDTO]) -> pd.DataFrame:
        records: list[dict] = []
        for dto in cardano_block_tx_dto_list:
            record: dict = {
                "block": dto.block,
                "tx_hash": dto.tx_hash,
                "created_at": pd.to_datetime(dto.created_at),
            }
            records.append(record)

        df: pd.DataFrame = pd.DataFrame.from_records(records)
        return df


if __name__ == "__main__":
    """
    for local testing
    """
    sample_block_tx_dto: list[CardanoBlocksTransactionsDTO] = [
        CardanoBlocksTransactionsDTO(
            block=11292757,
            tx_hash=[
                "6d7075da930360fb086114188c30642a924df014b9f431b187f72411d4c106cf",
                "1689da390ce0006e8b565fb5a4a7050157e11c0addd6437d5081907e9d0f7646",
                "7eb567035331d4da789610874485421f6ac29fa5e1ce19d57c7ce1138d3dae8a",
                "5ed4d3381f46f04cf58fbb14c9f9106872a589079568efbc3dd322421f3affee",
                "f513a278bcaaebde233bce650291249128a1d30339ce77c43c6d28c85fb569f5",
                "ae603ae339c98e7c4e664b8032055aac4d3971988e2e707a31677d0db75a9f74",
                "ada40917fbc2c4f12b99d828a018c2990c06c9153397eb518298733c78e88301",
                "f81d8c31fa27d94a189d15ceb5233fac41d6166ef886e79da202bf7ba2ad0e10",
                "bbaf75e23bb7f42d29edfb9c0be9b8cfad1fe76a9b9a389902cffd50fb6d77de",
                "152522e50b6e7b2cdacc18def69a736b6e3f94523d8d87aa3e03f35970063fa3",
                "57b6592e79d12e53c5d409f7ad23a33de9020796c33d83922db6898bd45dd16f",
                "7968cbc22ba895787569933ebcf75ecb082f87860054c10aad8b42f6825b9127"
            ],
            created_at=datetime(2025, 4, 21, 15, 18, 39, 630720)
        )
    ]
    res: pd.DataFrame = TransformCardanoBlockTxDTOToDf.transform(sample_block_tx_dto)
    print(res)