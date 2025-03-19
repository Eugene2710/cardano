from sqlalchemy import MetaData, Table, Column, String, DateTime, Numeric, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.functions import now

Base = declarative_base()
metadata: MetaData = MetaData()

# https://docs.blockfrost.io/#tag/cardano--blocks/GET/blocks/{hash_or_number}
cardano_block_table = Table(
    "cardano_blocks",
    metadata,
    Column("time", DateTime, nullable=False),
    Column("height", Integer, primary_key=True),
    Column("block_hash", String, nullable=False),
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
    Column("confirmations", Integer, nullable=False)
)