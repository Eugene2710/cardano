import os
from asyncio import AbstractEventLoop, new_event_loop
import boto3
import click
from dotenv import load_dotenv

from src.dao.provider_to_s3_import_status_dao import ProviderToS3ImportStatusDAO
from src.dao.s3_to_db_import_status_dao import S3ToDbImportStatusDAO
from src.file_explorer.s3_file_explorer import S3Explorer
from src.extractors.get_block import CardanoBlockExtractor
from src.extractors.get_block_from_s3 import CardanoBlockS3Extractor
from src.extractors.get_block_transactions import CardanoBlockTransactionsExtractor
from src.extractors.get_block_transactions_from_s3 import CardanoBlockTransactionsS3Extractor
from src.transformer.transform_cardano_block_dto_to_df import TransformCardanoBlockDTOToDF
from src.transformer.transform_cardano_block_tx_dto_to_df import TransformCardanoBlockTxDTOToDf
from src.dao.cardano_block_dao import CardanoBlockDAO
from src.dao.cardano_block_transactions_dao import CardanoBlockTransactionsDAO

from cardano_blocks_to_s3_pipeline_w_param import CardanoBlocksToETLPipeline
from s3_to_db_cardano_blocks_pipeline import S3ToDBCardanoBlocksETLPipeline
from cardano_block_transactions_to_s3_pipeline_w_params import CardanoBlockTransactionsToETLPipeline
from s3_to_db_cardano_block_transactions_pipeline import S3ToDBCardanoBlockTransactionsETLPipeline


class CardanoBlocksAndBlockTxETLPipeline:
    """
    Responsible for ingesting large blocks data and block transactions)tx hash for each block) data
    from blockfrost to s3 and then to local database/postgres
    the start block height and end block height will have to be specified for this pipeline to run using click
    """
    def __init__(
        self,
        blocks_provider_to_s3_pipeline: CardanoBlocksToETLPipeline,
        blocks_s3_to_db_pipeline: S3ToDBCardanoBlocksETLPipeline,
        block_tx_provider_to_s3_pipeline: CardanoBlockTransactionsToETLPipeline,
        block_tx_s3_to_db_pipeline: S3ToDBCardanoBlockTransactionsETLPipeline
    ) -> None:
        self._blocks_provider_to_s3_pipeline = blocks_provider_to_s3_pipeline
        self._blocks_s3_to_db_pipeline = blocks_s3_to_db_pipeline
        self._block_tx_provider_to_s3_pipeline = block_tx_provider_to_s3_pipeline
        self._block_tx_s3_to_db_pipeline = block_tx_s3_to_db_pipeline

    async def run(self, start_block_height: int, end_block_height: int) -> None:
        # curr = start_block_height
        end_block_height = end_block_height
        batch: int = 2000
        while start_block_height <= end_block_height:
            # curr_start_block: int = max(start_block_height, curr_start_block)
            curr_end_block: int = min(start_block_height+batch-1, end_block_height)
            await self._blocks_provider_to_s3_pipeline.run(start_block_height=start_block_height, end_block_height=curr_end_block)
            await self._blocks_s3_to_db_pipeline.run()
            await self._block_tx_provider_to_s3_pipeline.run(start_block_height=start_block_height, end_block_height=curr_end_block)
            await self._block_tx_s3_to_db_pipeline.run()
            start_block_height = curr_end_block+1


@click.command()
@click.option("--start-block-height", type=int, required=True, help="First block.")
@click.option("--end-block-height", type=int, required=True, help="Last block.")
def run(start_block_height: int, end_block_height: int):
    load_dotenv()
    provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = ProviderToS3ImportStatusDAO(
        os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    s3_to_db_import_status_dao: S3ToDbImportStatusDAO = S3ToDbImportStatusDAO(
        os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )
    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""),
        client=client
    )
    block_extractor: CardanoBlockExtractor = CardanoBlockExtractor()
    blocks_s3_extractor: CardanoBlockS3Extractor = CardanoBlockS3Extractor(
        s3_explorer=s3_explorer
    )
    block_tx_extractor: CardanoBlockTransactionsExtractor = CardanoBlockTransactionsExtractor()
    block_tx_s3_extractor: CardanoBlockTransactionsS3Extractor = CardanoBlockTransactionsS3Extractor(
        s3_explorer=s3_explorer
    )
    blocks_transformer: TransformCardanoBlockDTOToDF = TransformCardanoBlockDTOToDF()
    block_tx_transformer: TransformCardanoBlockTxDTOToDf = TransformCardanoBlockTxDTOToDf()
    cardano_block_dao: CardanoBlockDAO = CardanoBlockDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    cardano_block_tx_dao: CardanoBlockTransactionsDAO = CardanoBlockTransactionsDAO(
        connection_string=os.getenv("ASYNC_PG_CONNECTION_STRING", "")
    )
    cardano_blocks_to_etl_pipeline: CardanoBlocksToETLPipeline = CardanoBlocksToETLPipeline(
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        table="cardano_blocks",
        s3_explorer=s3_explorer,
        extractor=block_extractor,
    )
    s3_to_db_cardano_blocks_etl_pipeline: S3ToDBCardanoBlocksETLPipeline = (
        S3ToDBCardanoBlocksETLPipeline(
            s3_to_db_import_status_dao=s3_to_db_import_status_dao,
            provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
            table="cardano_blocks",
            s3_raw_blocks_path="cardano/blocks/raw",
            extractor=blocks_s3_extractor,
            transformer=blocks_transformer,
            s3_transformed_blocks_path="cardano/blocks/transformed",
            cardano_block_dao=cardano_block_dao,
            s3_explorer=s3_explorer,
        )
    )
    cardano_block_transactions_to_etl_pipeline: CardanoBlockTransactionsToETLPipeline = CardanoBlockTransactionsToETLPipeline(
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        table="cardano_block_transactions",
        s3_explorer=s3_explorer,
        extractor=block_tx_extractor
    )
    s3_to_db_cardano_block_tx_etl_pipeline: S3ToDBCardanoBlockTransactionsETLPipeline = S3ToDBCardanoBlockTransactionsETLPipeline(
        s3_to_db_import_status_dao=s3_to_db_import_status_dao,
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        table="cardano_block_transactions",
        s3_raw_block_tx_path="cardano/block_tx/raw",
        extractor=block_tx_s3_extractor,
        transformer=block_tx_transformer,
        s3_transformed_block_tx_path="cardano/block_tx/transformed",
        cardano_block_transactions_dao=cardano_block_tx_dao,
        s3_explorer=s3_explorer,
    )
    batch_etl_pipeline: CardanoBlocksAndBlockTxETLPipeline = CardanoBlocksAndBlockTxETLPipeline(
        blocks_provider_to_s3_pipeline=cardano_blocks_to_etl_pipeline,
        blocks_s3_to_db_pipeline=s3_to_db_cardano_blocks_etl_pipeline,
        block_tx_provider_to_s3_pipeline=cardano_block_transactions_to_etl_pipeline,
        block_tx_s3_to_db_pipeline=s3_to_db_cardano_block_tx_etl_pipeline,
    )
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(batch_etl_pipeline.run(start_block_height=start_block_height, end_block_height=end_block_height))


if __name__ == "__main__":
    run()