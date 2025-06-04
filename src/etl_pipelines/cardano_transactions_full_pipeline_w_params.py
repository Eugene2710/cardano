import os
from dotenv import load_dotenv
from asyncio import AbstractEventLoop, new_event_loop
import click
import boto3

from src.dao.provider_to_s3_import_status_dao import ProviderToS3ImportStatusDAO
from src.dao.s3_to_db_import_status_dao import S3ToDbImportStatusDAO
from src.file_explorer.s3_file_explorer import S3Explorer
from src.extractors.get_transactions import CardanoTransactionsExtractor
from src.extractors.get_transactions_from_s3 import CardanoTransactionsS3Extractor
from src.extractors.get_tx_utxo import CardanoTxUtxoExtractor
from src.extractors.get_tx_utxo_from_s3 import CardanoTxUtxoS3Extractor
from src.transformer.transform_cardano_tx_dto_to_df import TransformCardanoTransactionsDTOToDf
from src.transformer.transform_cardano_tx_utxo_dto_to_df import TransformCardanoTxUtxoDTOToDf
from src.dao.cardano_transactions_dao import CardanoTransactionsDAO
from src.dao.cardano_tx_utxo_dao import CardanoTxUtxoDAO
from src.dao.cardano_tx_utxo_sub_dao import CardanoTxUtxoSubDAO
from src.dao.cardano_tx_utxo_input_amount_dao import CardanoTxUtxoInputAmtDAO
from database_management.cardano.cardano_tables import cardano_tx_utxo_input_table, cardano_tx_utxo_input_amount_table, cardano_tx_utxo_output_table, cardano_tx_utxo_output_amount_table
from src.etl_pipelines.cardano_transactions_to_s3_pipeline_w_param import CardanoTransactionsTOETLPipeline
from src.etl_pipelines.s3_to_db_cardano_transactions_pipeline import S3ToDBCardanoTransactionsETLPipeline
from src.etl_pipelines.cardano_tx_utxo_to_s3_pipeline_w_param import CardanoTxUtxoToETLPipeline
from src.etl_pipelines.s3_to_db_cardano_tx_utxo_pipeline import S3ToDBCardanoTxUtxoETLPipeline


class CardanoTxFullETLPipeline:
    """
    Responsible for running the
    - cardano transactions etl pipeline to extract Transactions data from Blockfrost to S3
    - cardano transactions etl pipeline to extract Transactions data from S3 to DB
    - cardano tx utxo etl pipeline to extract tx utxo data from Blockfrost to S3
    - cardano tx utxo etl pipeline to extract tx utxo data from S3 to DB
    This entire pipeline is ran with parameters "start block height" and "end block height"
    """
    def __init__(
            self,
            tx_to_s3_pipeline: CardanoTransactionsTOETLPipeline,
            tx_s3_to_db_pipeline: S3ToDBCardanoTransactionsETLPipeline,
            tx_utx_to_s3_pipeline: CardanoTxUtxoToETLPipeline,
            tx_utxo_s3_to_db_pipeline: S3ToDBCardanoTxUtxoETLPipeline,
    ) -> None:
        self._tx_to_s3_pipeline = tx_to_s3_pipeline
        self._tx_s3_to_db_pipeline = tx_s3_to_db_pipeline
        self._tx_utxo_to_s3_pipeline = tx_utx_to_s3_pipeline
        self._tx_utxo_s3_to_db_pipeline = tx_utxo_s3_to_db_pipeline

    async def run(self, start_block_height: int, end_block_height: int) -> None:
        # set batch to be 1000 - congruent to the batch limits in transactions and utxo pipelines
        batch: int = 1000
        while start_block_height <= end_block_height:
            curr_end_block: int = min(start_block_height+batch-1, end_block_height)
            await self._tx_to_s3_pipeline.run(start_block_height=start_block_height, end_block_height=curr_end_block)
            await self._tx_s3_to_db_pipeline.run()
            await self._tx_utxo_to_s3_pipeline.run(start_block_height=start_block_height, end_block_height=curr_end_block)
            await self._tx_utxo_s3_to_db_pipeline.run()
            start_block_height = curr_end_block+1


@click.command()
@click.option("--start-block", type=int, required=True, help="First block.")
@click.option("--end-block", type=int, required=True, help="Last block.")
def run(start_block: int, end_block: int) -> None:
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
    provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = ProviderToS3ImportStatusDAO(
        os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    s3_to_db_import_status_dao: S3ToDbImportStatusDAO = S3ToDbImportStatusDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    tx_extractor: CardanoTransactionsExtractor = CardanoTransactionsExtractor()
    tx_s3_extractor: CardanoTransactionsS3Extractor = CardanoTransactionsS3Extractor(
        s3_explorer=s3_explorer
    )
    tx_transformer: TransformCardanoTransactionsDTOToDf = TransformCardanoTransactionsDTOToDf()
    cardano_tx_dao: CardanoTransactionsDAO = CardanoTransactionsDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    tx_utxo_extractor: CardanoTxUtxoExtractor = CardanoTxUtxoExtractor()
    tx_utxo_s3_extractor: CardanoTxUtxoS3Extractor = CardanoTxUtxoS3Extractor(
        s3_explorer=s3_explorer
    )
    tx_utxo_transformer: TransformCardanoTxUtxoDTOToDf = TransformCardanoTxUtxoDTOToDf()
    cardano_tx_utxo_dao: CardanoTxUtxoDAO = CardanoTxUtxoDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    cardano_tx_utxo_output_dao: CardanoTxUtxoSubDAO = CardanoTxUtxoSubDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", ""),
        table=cardano_tx_utxo_output_table
    )
    cardano_tx_utxo_output_amt_dao: CardanoTxUtxoSubDAO = CardanoTxUtxoSubDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", ""),
        table=cardano_tx_utxo_output_amount_table
    )
    cardano_tx_utxo_input_dao: CardanoTxUtxoSubDAO = CardanoTxUtxoSubDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", ""),
        table=cardano_tx_utxo_input_table
    )
    cardano_tx_utxo_input_amt_dao: CardanoTxUtxoInputAmtDAO = CardanoTxUtxoInputAmtDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", ""),
        table=cardano_tx_utxo_input_amount_table
    )
    tx_to_s3_etl_pipeline: CardanoTransactionsTOETLPipeline = CardanoTransactionsTOETLPipeline(
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        table="cardano_transactions",
        s3_explorer=s3_explorer,
        extractor=tx_extractor
    )
    tx_s3_to_db_etl_pipeline: S3ToDBCardanoTransactionsETLPipeline = S3ToDBCardanoTransactionsETLPipeline(
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        table="cardano_transactions",
        s3_raw_tx_path="cardano/transactions/raw",
        extractor=tx_s3_extractor,
        transformer=tx_transformer,
        s3_transformed_tx_path="cardano/transactions/transformed",
        s3_explorer=s3_explorer,
        cardano_transactions_dao=cardano_tx_dao,
    )
    tx_utxo_to_s3_etl_pipeline: CardanoTxUtxoToETLPipeline = CardanoTxUtxoToETLPipeline(
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        table="cardano_transactions_utxo",
        s3_explorer=s3_explorer,
        extractor=tx_utxo_extractor,
    )
    tx_utxo_s3_to_db_etl_pipeline: S3ToDBCardanoTxUtxoETLPipeline = S3ToDBCardanoTxUtxoETLPipeline(
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        table="cardano_tx_utxo",
        s3_raw_tx_path="cardano/transaction_utxo/raw",
        extractor=tx_utxo_s3_extractor,
        transformer=tx_utxo_transformer,
        s3_explorer=s3_explorer,
        s3_transformed_tx_utxo_path="cardano/transaction_utxo/transformed/utxo/",
        s3_transformed_tx_utxo_input_path="cardano/transaction_utxo/transformed/utxo_input/",
        s3_transformed_tx_utxo_input_amt_path="cardano/transaction_utxo/transformed/utxo_input_amount/",
        s3_transformed_tx_utxo_output_path="cardano/transaction_utxo/transformed/utxo_output/",
        s3_transformed_tx_utxo_output_amt_path="cardano/transaction_utxo/transformed/utxo_output_amount/",
        cardano_tx_utxo_dao=cardano_tx_utxo_dao,
        cardano_tx_utxo_output_dao=cardano_tx_utxo_output_dao,
        cardano_tx_utxo_output_amt_dao=cardano_tx_utxo_output_amt_dao,
        cardano_tx_utxo_input_dao=cardano_tx_utxo_input_dao,
        cardano_tx_utxo_input_amt_dao=cardano_tx_utxo_input_amt_dao,
    )
    tx_and_utxo_pipeline: CardanoTxFullETLPipeline = CardanoTxFullETLPipeline(
        tx_to_s3_pipeline=tx_to_s3_etl_pipeline,
        tx_s3_to_db_pipeline=tx_s3_to_db_etl_pipeline,
        tx_utx_to_s3_pipeline=tx_utxo_to_s3_etl_pipeline,
        tx_utxo_s3_to_db_pipeline=tx_utxo_s3_to_db_etl_pipeline
    )
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(tx_and_utxo_pipeline.run(start_block_height=start_block, end_block_height=end_block))


if __name__ == "__main__":
    run()