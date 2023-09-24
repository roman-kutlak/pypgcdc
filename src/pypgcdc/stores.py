"""
MIT License

Copyright (c) [2023] [Roman Kutlak]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import json
import logging
from typing import Callable, Dict, Optional, Tuple, Type

import psycopg2
import psycopg2.extras
import pydantic

from pypgcdc.models import (
    ChangeEvent,
    ReplicationMessage,
    SlotInitInfo,
    TableSchema,
    Transaction,
)

logger = logging.getLogger(__name__)


class DataStore:
    """DataStore is an example of a data consumer that is meant to handle the initial sync as well as any changes.

    The main purpose of the DataStore is to store the captured changes in the right place.
    This could be a database, a file, a message queue, etc.

    The default implementation just prints the changes to the log.

    You can override this class, or you can write your own data store class from scratch.
    If you do, don't forget to implement all public methods listed in this class.

    """

    def __init__(self, quiet=False) -> None:
        self.txn_id = None
        self.txn_ts = None
        self.txn_lsn = None
        self.commit_callback: Optional[Callable] = None
        self.quiet = quiet

    @property
    def commit_callback(self) -> Optional[Callable]:
        return self._commit_callback

    @commit_callback.setter
    def commit_callback(self, callback: Optional[Callable]) -> None:
        self._commit_callback = callback

    def handle_begin(self, txn: Transaction, message: ReplicationMessage) -> None:
        self.txn_id = txn.tx_id
        self.txn_ts = txn.commit_ts
        self.txn_lsn = txn.begin_lsn
        if not self.quiet:
            logger.info("*" * 120)
            logger.info(f"{message.message_id}:{txn.json(indent=2)}")

    def handle_commit(self, txn: Transaction, message: ReplicationMessage) -> None:
        self.txn_id = None
        self.txn_ts = None
        self.txn_lsn = None
        self.commit_callback(txn.begin_lsn)
        if not self.quiet:
            logger.info(f"{message.message_id}:{txn.json(indent=2)}")
            logger.info(f"***** {txn} *****")

    def handle_slot_created(self, info: SlotInitInfo) -> None:
        logger.info(info)
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
        if not self.quiet:
            logger.info(f"{message.message_id}:{event.json(indent=2)}")
        else:
            data = {
                "message_id": message.message_id,
                "operation": event.op,
                "key": event.key,
                "before": event.before,
                "after": event.after,
            }
            logger.info(json.dumps(data, indent=2, default=str))

    def handle_relation(self, relation: TableSchema, message: ReplicationMessage) -> None:
        if not self.quiet:
            logger.info(f"{message.message_id}:{relation.json(indent=2)}")


class MetadataStore:
    """MetadataStore is used to keep track of the table schemas and the table models."""

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
