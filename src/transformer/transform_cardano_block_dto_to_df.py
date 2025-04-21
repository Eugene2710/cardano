import pandas as pd
from datetime import datetime
import logging
from src.utils.logging_utils import setup_logging
from src.models.database_transfer_objects.cardano_blocks import CardanoBlocksDTO

logger = logging.getLogger(__name__)
setup_logging(logger)


class TransformCardanoBlockDTOToDF:
    """
    Responsible for transforming a list of cardano_block_dto into a pandas DataFrame, so that it can converted to bytesIO outside of this class in main pipeline
    """
    @staticmethod
    def transform(cardano_block_dto_list: list[CardanoBlocksDTO]) -> pd.DataFrame:
        records: list[dict] = []
        for dto in cardano_block_dto_list:
            record: dict = {
                "time": pd.to_datetime(dto.time),
                "height": dto.height,
                "hash": dto.hash,
                "slot": dto.slot,
                "epoch": dto.epoch,
                "epoch_slot": dto.epoch_slot,
                "slot_leader": dto.slot_leader,
                "size": dto.size,
                "tx_count": dto.tx_count,
                "output": dto.output,
                "fees": dto.fees,
                "block_vrf": dto.block_vrf,
                "op_cert": dto.op_cert,
                "op_cert_counter": dto.op_cert_counter,
                "previous_block": dto.previous_block,
                "next_block": dto.next_block,
                "confirmations": dto.confirmations,
                "created_at": pd.to_datetime(dto.created_at),
            }
            records.append(record)

        df: pd.DataFrame = pd.DataFrame.from_records(records)
        return df


if __name__ == "__main__":
    """
    for local testing purpose
    """
    sample_blocks: list[CardanoBlocksDTO] = [
        CardanoBlocksDTO(
            time=datetime(2017, 9, 25, 7, 4, 11),
            height=5999,
            hash='a679dd3cfa28c8a19574990c78bbd6c7ee8c1f27ddb241516f55bd973c4213b7',
            slot=5998,
            epoch=0,
            epoch_slot=5998,
            slot_leader='ByronGenesis-8e8a7b0f4a23f07a',
            size=631,
            tx_count=0,
            output=None,
            fees=None,
            block_vrf=None,
            op_cert=None,
            op_cert_counter=None,
            previous_block='0720dd97d5ffee27979e527a6d1c339f2a372bb6d748362bb991be9fb3b54842',
            next_block='d12e92363bab7538983a4feae087f7748ec7bade76efd17c13d8b672e857e7f1',
            confirmations=11754906,
            created_at=datetime(2025, 4, 21, 15, 18, 39, 630720))
    ]
    res: pd.DataFrame = TransformCardanoBlockDTOToDF.transform(cardano_block_dto_list=sample_blocks)
    print(res)

