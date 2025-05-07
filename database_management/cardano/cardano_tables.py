import uuid
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    DateTime,
    ForeignKey,
    Integer,
    UUID,
    Boolean,
    func,
    Numeric
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY

Base = declarative_base()
metadata: MetaData = MetaData()

# https://docs.blockfrost.io/#tag/cardano--blocks/GET/blocks/{hash_or_number}
cardano_block_table: Table = Table(
    "cardano_blocks",
    metadata,
    Column("time",  DateTime, nullable=False),
    Column("height", Integer, primary_key=True),
    Column("hash", String, nullable=False),  # block hash
    Column("slot", Integer, nullable=False),
    Column("epoch", Integer, nullable=True),
    Column("epoch_slot", Integer, nullable=True),
    Column("slot_leader", String, nullable=False),
    Column("size", Integer, nullable=False),
    Column("tx_count", Integer, nullable=False),
    Column("output", String, nullable=True),
    Column("fees", String, nullable=True),
    Column("block_vrf", String, nullable=True),
    Column("op_cert", String, nullable=True),
    Column("op_cert_counter", String, nullable=True),
    Column("previous_block", String, nullable=True),
    Column("next_block", String, nullable=True),
    Column("confirmations", Integer, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now() # server-side default
    ),  # date you insert the row
)

cardano_block_transactions_table: Table = Table(
    "cardano_block_transactions",
    metadata,
    Column(
        "block", String, primary_key=True
    ),  # we will be using block number, but block hash works too
    Column("tx_hash", ARRAY(String), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now()  # server-side default
    ),  # date you insert the row
)

cardano_transactions_table: Table = Table(
    "cardano_transactions",
    metadata,
    Column("hash", String, primary_key=True),  # transaction hash
    Column("block", String, nullable=False),  # block hash
    Column(
        "block_height",
        Integer,
        nullable=False,
    ),  # block number
    Column("block_time",  DateTime, nullable=False),
    Column("slot", Integer, nullable=False),
    Column("index", Integer, nullable=False),  # tx index within block
    Column("fees", String, nullable=False),
    Column("deposit", String, nullable=False),
    Column("size", Integer, nullable=False),
    Column("invalid_before", String, nullable=True),
    Column("invalid_hereafter", String, nullable=True),
    Column("utxo_count", Integer, nullable=False),
    Column("withdrawal_count", Integer, nullable=False),
    Column("mir_cert_count", Integer, nullable=False),
    Column("delegation_count", Integer, nullable=False),
    Column("stake_cert_count", Integer, nullable=False),
    Column("pool_update_count", Integer, nullable=False),
    Column("pool_retire_count", Integer, nullable=False),
    Column("asset_mint_or_burn_count", Integer, nullable=False),
    Column("redeemer_count", Integer, nullable=False),
    Column("valid_contract", Boolean, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now()  # server-side default
    ),  # date you insert the row
)

cardano_tx_output_amount_table: Table = Table(
    "cardano_tx_output_amount",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4()),
    # generates a unique id with uuid.uuid4() -> this is our own id as they did not provide it
    Column(
        "hash",
        String,
        nullable=False,
    ),  # transaction hash
    Column("unit", String, nullable=False),
    Column("quantity", Numeric(38, 0), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now()  # server-side default
    ),  # date you insert the row
)

cardano_tx_utxo_table: Table = Table(
    "cardano_tx_utxo",
    metadata,
    Column("hash", String, primary_key=True),
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now()  # server-side default
    ),  # date you insert the row
)

cardano_tx_utxo_input_table: Table = Table(
    "cardano_tx_utxo_input",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4()),
    Column(
        "hash",
        String,
        ForeignKey("cardano_transactions.hash", name="fk_utxo_input_tx"),
        nullable=False,
    ),  # transaction hash
    Column("address", String, nullable=False),  # input address
    Column(
        "tx_utxo_hash", String, nullable=False
    ),  # known as tx_hash in Blockfrost doc, but is referring to the utxo hash of the tx
    Column("output_index", Integer, nullable=False),
    Column("data_hash", String, nullable=True),
    Column("inline_datum", String, nullable=True),
    Column(
        "reference_script_hash", String, nullable=True
    ),  # used to check for protocols
    Column("collateral", Boolean, nullable=False),
    Column("reference", Boolean, nullable=True),
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now()  # server-side default
    ),  # date you insert the row
)

cardano_tx_utxo_input_amount_table: Table = Table(
    "cardano_tx_utxo_input_amount",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4()),
    Column("parent_id", UUID(as_uuid=True), ForeignKey("cardano_tx_utxo_input.id", name="fk_utxo_input_amt_parent"), nullable=False),
    Column("unit", String, nullable=False),
    Column("quantity", Numeric(38, 0), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now()  # server-side default
    ),  # date you insert the row
)

cardano_tx_utxo_output_table: Table = Table(
    "cardano_tx_utxo_output",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4()),
    Column(
        "hash",
        String,
        ForeignKey("cardano_transactions.hash", name="fk_utxo_output_tx"),
        nullable=False,
    ),
    Column("address", String, nullable=False),
    Column("output_index", Integer, nullable=False),
    Column("data_hash", String, nullable=True),
    Column("inline_datum", String, nullable=True),
    Column("collateral", Boolean, nullable=False),
    Column(
        "reference_script_hash", String, nullable=True
    ),  # used to check for protocols
    Column("consumed_by_tx", String, nullable=True),
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now()  # server-side default
    ),  # date you insert the row
)

cardano_tx_utxo_output_amount_table: Table = Table(
    "cardano_tx_utxo_output_amount",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4()),
    Column("parent_id", UUID(as_uuid=True), ForeignKey("cardano_tx_utxo_output.id", name="fk_utxo_output_amt_parent"), nullable=False),
    Column("unit", String, nullable=False),
    Column("quantity", Numeric(38, 0), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now()  # server-side default
    ),  # date you insert the row
)

s3_to_db_import_status_table: Table = Table(
    "s3_to_db_import_status",
    metadata,
    Column("table", String, primary_key=True),  # e.g. cardano_block_table
    Column("file_modified_date", DateTime, primary_key=True), # date at which the file was modified in S3
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now()  # server-side default
    ),  # date you insert the row
)

provider_to_s3_import_status_table: Table = Table(
    "provider_to_s3_import_status",
    metadata,
    Column("table", String, primary_key=True),
    Column("block_height", Integer, primary_key=True), # block number
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now()  # server-side default
    ),  # date you insert the row
)

