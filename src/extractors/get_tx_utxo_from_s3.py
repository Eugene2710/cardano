import io
import json
import os
from typing import Any
import boto3
from dotenv import load_dotenv
from pprint import pprint
from retry import retry

from src.models.blockfrost_models.cardano_transaction_utxo import TransactionUTxO
from src.models.database_transfer_objects.cardano_transactions_utxo_dto import CardanoTransactionUtxoDTO
from src.file_explorer.s3_file_explorer import S3Explorer


class CardanoTxUtxoS3Extractor:
    def __init__(self, s3_explorer: S3Explorer) -> None:
        self._s3_explorer: S3Explorer =s3_explorer

    @retry(
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    def get_tx_utxo_from_s3(self, s3_path: str) -> list[CardanoTransactionUtxoDTO]:
        """
        - get specified json file from s3 using S3Explorer.download_to_buffer
        - add json file to database
        """
        # download JSON file into a BytesIO buffer
        buffer: io.BytesIO = self._s3_explorer.download_to_buffer(s3_path=s3_path)
        # load JSON content into a list of dict
        tx_utxo_data: list[dict[str, Any]] = json.load(buffer)

        dtos: list[CardanoTransactionUtxoDTO] = []
        for record in tx_utxo_data:
            raw_tx_utxo: TransactionUTxO = TransactionUTxO(**record)
            dto: CardanoTransactionUtxoDTO = CardanoTransactionUtxoDTO.from_raw_cardano_tx_utxo(
                hash=raw_tx_utxo.hash,
                input=raw_tx_utxo
            )
            dtos.append(dto)

        return dtos


if __name__ == "__main__":
    load_dotenv()
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )
    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""), client=client
    )
    extractor: CardanoTxUtxoS3Extractor = CardanoTxUtxoS3Extractor(s3_explorer=s3_explorer)
    tx_utxo_res: list[CardanoTransactionUtxoDTO] = extractor.get_tx_utxo_from_s3(s3_path="cardano/transaction_utxo/raw/11292900/cardano_tx_utxo_11292900.json")
    pprint(tx_utxo_res)