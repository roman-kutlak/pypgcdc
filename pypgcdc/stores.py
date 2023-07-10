import logging
from typing import Dict, Tuple, Type

import psycopg2
import psycopg2.extras
import pydantic

from pypgcdc.models import Transaction, SlotInitInfo, ChangeEvent, ReplicationMessage, TableSchema

logger = logging.getLogger(__name__)


class StateStore:
    """Store state in memory"""

    def __init__(self) -> None:
        self.txn_id = None
        self.txn_ts = None
        self.txn_lsn = None

    def handle_begin(self, txn: Transaction, msg: psycopg2.extras.ReplicationMessage) -> None:
        self.txn_id = txn.tx_id
        self.txn_ts = txn.commit_ts
        self.txn_lsn = txn.begin_lsn
        msg.cursor.send_feedback(write_lsn=txn.begin_lsn)
        print("*" * 120)

    def handle_commit(self, txn: Transaction, msg: psycopg2.extras.ReplicationMessage) -> None:
        self.txn_id = None
        self.txn_ts = None
        self.txn_lsn = None
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
        print(f"***** {txn} *****")


class ChangeStore:
    """Process changes"""

    def handle_slot_created(self, info: SlotInitInfo) -> None:
        print(info)
        logger.info(f"Processing publication {info.publication_name}")
        q1 = "begin transaction isolation level repeatable read"
        q2 = f"set transaction snapshot '{info.snapshot}'"
        q3 = f"select * from pg_publication_tables where pubname = '{info.publication_name}'"
        with psycopg2.connect(info.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(q1)
                cur.execute(q2)
                cur.execute(q3)
                result = cur.fetchall()
                for row in result:
                    logger.info(f"Including table {row[1]}.{row[2]}")

    def handle_change_event(self, event: ChangeEvent, message: ReplicationMessage) -> None:
        print(f"{message.message_id}:{event.json(indent=2)}")

    def handle_relation(self, relation: TableSchema, message: ReplicationMessage) -> None:
        print(f"{message.message_id}:{relation.json(indent=2)}")


class MetadataStore:
    def __init__(self):
        # save map of type oid to readable name
        self.pg_types: Dict[Tuple[str, int], str] = dict()
        # table schema as described in the replication message
        self.table_schemas: Dict[Tuple[str, int], TableSchema] = dict()  # map relid to table schema
        # table model for creating "row" objects
        self.table_models: Dict[Tuple[str, int], Type[pydantic.BaseModel]] = dict()
        # key only model for creating "row" that only contain the PK column changes
        self.key_models: Dict[Tuple[str, int], Type[TableSchema]] = dict()

    def add_column_type(self, database: str, type_id: int, data_type: str):
        self.pg_types[(database, type_id)] = data_type

    def column_type(self, database: str, type_id: int) -> str:
        return self.pg_types.get((database, type_id))

    def add_table_schema(self, database: str, relid: int, table_schema: TableSchema) -> None:
        self.table_schemas[(database, relid)] = table_schema

    def table_schema(self, database: str, relid: int) -> TableSchema:
        return self.table_schemas.get((database, relid))

    def add_key_model(self, database: str, relid: int, table_schema: Type[TableSchema]) -> None:
        self.key_models[(database, relid)] = table_schema

    def key_model(self, database: str, relid: int) -> Type[TableSchema]:
        return self.key_models.get((database, relid))

    def add_table_model(self, database: str, relid: int, table_model: Type[pydantic.BaseModel]) -> None:
        self.table_models[(database, relid)] = table_model

    def table_model(self, database: str, relid: int) -> Type[pydantic.BaseModel]:
        return self.table_models.get((database, relid))
