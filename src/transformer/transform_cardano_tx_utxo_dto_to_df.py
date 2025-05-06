import pandas as pd
import logging
from typing import Any
from uuid import UUID
from datetime import datetime
from pprint import pprint
from src.utils.logging_utils import setup_logging
from pathlib import Path
from src.models.database_transfer_objects.cardano_transactions_utxo_dto import CardanoTransactionUtxoDTO, CardanoTxUtxoInputDTO, CardanoTxUtxoOutputDTO, TxAmountDTO

logger = logging.getLogger(__name__)
setup_logging(logger)


class TransformCardanoTxUtxoDTOToDf:
    """
    Responsible for transforming a list of cardano_tx_utxo_dto into a pandas DataFrame
    , so that it can be converted to BytesIO outside of this class
    """
    @staticmethod
    def transform(cardano_tx_utxo_dto_list: list[CardanoTransactionUtxoDTO]) -> dict[str, pd.DataFrame]:
        utxo_list: list[dict[str, Any]] = []
        input_list: list[dict[str, Any]] = []
        input_amount_list: list[dict[str, Any]] = []
        output_list: list[dict[str, Any]] = []
        output_amount_list: list[dict[str, Any]] = []

        for dto in cardano_tx_utxo_dto_list:
            utxo_list.append(
                {"hash": dto.hash, "created_at": dto.created_at}
            )
            for inp in dto.inputs:
                input_list.append(
                    {
                        "id": inp.id,
                        "hash": inp.hash,
                        "address": inp.address,
                        "tx_utxo_hash": inp.tx_utxo_hash,
                        "output_index": inp.output_index,
                        "data_hash": inp.data_hash,
                        "inline_datum": inp.inline_datum,
                        "reference_script_hash": inp.reference_script_hash,
                        "collateral": inp.collateral,
                        "reference": inp.reference,
                        "created_at": inp.created_at,
                    }
                )
                for amt in inp.amounts:
                    input_amount_list.append(
                        {
                            "id": amt.id,
                            "parent_id": inp.id,
                            "tx_utxo_hash": amt.tx_utxo_hash,
                            "unit": amt.unit,
                            "quantity": amt.quantity,
                            "created_at": amt.created_at,
                        }
                    )

            for out in dto.outputs:
                output_list.append(
                    {
                        "id": out.id,
                        "hash": out.hash,
                        "address": out.address,
                        "output_index": out.output_index,
                        "data_hash": out.data_hash,
                        "inline_datum": out.inline_datum,
                        "reference_script_hash": out.reference_script_hash,
                        "collateral": out.collateral,
                        "consumed_by_tx": out.consumed_by_tx,
                        "created_at": out.created_at,
                    }
                )
                for amt in out.amounts:
                    output_amount_list.append(
                        {
                            "id": amt.id,
                            "parent_id": out.id,
                            "data_hash": out.data_hash,
                            "unit": amt.unit,
                            "quantity": amt.quantity,
                            "created_at": amt.created_at,
                        }
                    )

        dfs = {
            "cardano_tx_utxo": pd.DataFrame.from_records(utxo_list),
            "cardano_tx_utxo_input": pd.DataFrame.from_records(input_list),
            "cardano_tx_utxo_input_amt": pd.DataFrame.from_records(input_amount_list),
            "cardano_tx_utxo_output": pd.DataFrame.from_records(output_list),
            "cardano_tx_utxo_output_amt": pd.DataFrame.from_records(output_amount_list),
        }

        for name, df in dfs.items():
            if not df.empty:
                df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
            logger.info("%s → %d rows", name, len(df))

        return dfs


if __name__ == "__main__":
    sample_tx_utxo_dto: list[CardanoTransactionUtxoDTO] = [
        CardanoTransactionUtxoDTO(
            hash='5924322a9ceb1d804cca62846310d69ff0ffa175f302923316dece025e404785',
            created_at=datetime(2025, 5, 4, 18, 15, 47, 731563),
            inputs=[
                CardanoTxUtxoInputDTO(
                    id=UUID('deff8d2e-a1a7-4c74-b2e2-eb2877c32f2a'),
                    hash='5924322a9ceb1d804cca62846310d69ff0ffa175f302923316dece025e404785',
                    address='addr1qywgh46dqu7lq6mp5c6tzldpmzj6uwx335ydrpq8k7rru4q6yhkfqn5pc9f3z76e4cr64e5mf98aaeht6zwf8xl2nc9qr66sqg',
                    tx_utxo_hash='a000c3044fca0ee824ec532c5cebe23a5ff05da31bce16c9cac4d07e0f0d3bde',
                    output_index=1,
                    data_hash=None,
                    inline_datum=None,
                    reference_script_hash=None,
                    collateral=False,
                    reference=None,
                    created_at=datetime(2025, 5, 4, 18, 15, 47, 731546),
                    amounts=[
                        TxAmountDTO(
                            id=UUID('fe7474fe-5c1d-445e-b28d-83822a3f56d0'),
                            parent_id=UUID('deff8d2e-a1a7-4c74-b2e2-eb2877c32f2a'),
                            tx_utxo_hash='a000c3044fca0ee824ec532c5cebe23a5ff05da31bce16c9cac4d07e0f0d3bde',
                            unit='lovelace',
                            quantity='26015021282',
                            created_at = datetime(2025, 5, 4, 18, 15, 47, 731546),
                        )
                    ]
                )
            ],
            outputs=[
                CardanoTxUtxoOutputDTO(
                    id=UUID('498520d9-e1fc-4203-b25c-50d591b837db'),
                    hash='5924322a9ceb1d804cca62846310d69ff0ffa175f302923316dece025e404785',
                    address='addr1vxv3cyneqdn53cr4qjcz7zkvhqtwz2mh7qfme5lpf07svngpw6cz0',
                    output_index=0,
                    data_hash=None,
                    inline_datum=None,
                    collateral=False,
                    reference_script_hash=None,
                    consumed_by_tx='00416c39a0b2ab22194a76df89689284e7ed251da204403efdc8866ebe2b92c9',
                    created_at=datetime(2025, 5, 4, 18, 15, 47, 731553),
                    amounts=[
                        TxAmountDTO(
                            id=UUID('4e16edf9-31d6-4c72-82db-56525ab6ae27'),
                            parent_id=UUID('498520d9-e1fc-4203-b25c-50d591b837db'),
                            tx_utxo_hash='5924322a9ceb1d804cca62846310d69ff0ffa175f302923316dece025e404785',
                            unit='lovelace',
                            quantity='10000000000',
                            created_at=datetime(2025, 5, 4, 18, 15, 47, 731546),
                        )
                    ]
                ),
                CardanoTxUtxoOutputDTO(
                    id=UUID('e46e138c-1667-46a6-99dd-02da97acff47'),
                    hash='5924322a9ceb1d804cca62846310d69ff0ffa175f302923316dece025e404785',
                    address='addr1qywgh46dqu7lq6mp5c6tzldpmzj6uwx335ydrpq8k7rru4q6yhkfqn5pc9f3z76e4cr64e5mf98aaeht6zwf8xl2nc9qr66sqg',
                    output_index=1,
                    data_hash=None,
                    inline_datum=None,
                    collateral=False,
                    reference_script_hash=None,
                    consumed_by_tx='a708789548eb730ab9a50d5c76004ff94aa3c53c22f32190e1933baccfa37876',
                    created_at=datetime(2025, 5, 4, 18, 15, 47, 731560),
                    amounts=[
                        TxAmountDTO(
                            id=UUID('52fcb965-3b5d-4e10-9e05-4dfa431f0c21'),
                            parent_id=UUID('e46e138c-1667-46a6-99dd-02da97acff47'),
                            tx_utxo_hash='5924322a9ceb1d804cca62846310d69ff0ffa175f302923316dece025e404785',
                            unit='lovelace',
                            quantity='16014145005',
                            created_at=datetime(2025, 5, 4, 18, 15, 47, 731546),
                        )
                    ]
                )
            ]
        ),
        CardanoTransactionUtxoDTO(
            hash='f3097e962455cfee841e9e5b242b79168d7a5934db32cb0e186b1ee213d0599e',
            created_at=datetime(2025, 5, 4, 18, 15, 47, 731583),
            inputs=[
                CardanoTxUtxoInputDTO(
                    id=UUID('c465e035-91b5-4c0d-a573-7da2f49b2bcd'),
                    hash='f3097e962455cfee841e9e5b242b79168d7a5934db32cb0e186b1ee213d0599e',
                    address='addr1q9ht7wx30g2m7nqd0slrm8gqnn0svym92rmwyvmxfndgczdr7sjujjxajf88rtyueaeppczy9vuvvxjxhnrfaufnj5xqv04xz3',
                    tx_utxo_hash='c11d56b1e27b46c77c4b1a5a6173544f062789f08230feabf9bc74a2b231b729',
                    output_index=0,
                    data_hash=None,
                    inline_datum=None,
                    reference_script_hash=None,
                    collateral=False,
                    reference=None,
                    created_at=datetime(2025, 5, 4, 18, 15, 47, 731574),
                    amounts=[
                        TxAmountDTO(
                            id=UUID('50a3d0e9-a51d-45bd-9e66-ba60638c31a1'),
                            parent_id=UUID('c465e035-91b5-4c0d-a573-7da2f49b2bcd'),
                            tx_utxo_hash='c11d56b1e27b46c77c4b1a5a6173544f062789f08230feabf9bc74a2b231b729',
                            unit='lovelace',
                            quantity='89700000',
                            created_at=datetime(2025, 5, 4, 18, 15, 47, 731546),
                        )
                    ]
                )
            ],
            outputs=[
                CardanoTxUtxoOutputDTO(
                    id=UUID('bf3ad5c5-e12f-4dd2-8dd4-adac0f1e1974'),
                    hash='f3097e962455cfee841e9e5b242b79168d7a5934db32cb0e186b1ee213d0599e',
                    address='addr1qx0ncqjs75efqm7c3q5f7y47r85e75720j78uwhfhjx3csar7sjujjxajf88rtyueaeppczy9vuvvxjxhnrfaufnj5xq67l0an',
                    output_index=0,
                    data_hash=None,
                    inline_datum=None,
                    collateral=False,
                    reference_script_hash=None,
                    consumed_by_tx='7974f8de575cb0041c9d3feaa5730fe940a0d3a095d47488372d6adb0f5672c9',
                    created_at=datetime(2025, 5, 4, 18, 15, 47, 731581),
                    amounts=[
                        TxAmountDTO(
                            id=UUID('d707457a-2f8c-415a-b18f-9bdc1568ecfd'),
                            parent_id=UUID('bf3ad5c5-e12f-4dd2-8dd4-adac0f1e1974'),
                            tx_utxo_hash='f3097e962455cfee841e9e5b242b79168d7a5934db32cb0e186b1ee213d0599e',
                            unit='lovelace',
                            quantity='89528427',
                            created_at=datetime(2025, 5, 4, 18, 15, 47, 731546),
                        )
                    ]
                )
            ]
        )
    ]

    res: dict[str, pd.DataFrame] = TransformCardanoTxUtxoDTOToDf.transform(sample_tx_utxo_dto)
    pprint(res)

    out_dir: Path = Path("cardano_tx_utxo_csv")
    out_dir.mkdir(exist_ok=True)

    for name, df in res.items():
        file_path: Path = out_dir / f"{name}.csv"
        df.to_csv(file_path, index=False)
        print(f"saved: {file_path}")