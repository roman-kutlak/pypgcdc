"""
MIT License

Copyright (c) [2020] [Daniel Geals]

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
import logging
import multiprocessing
import time
import typing
import uuid
from collections import OrderedDict
from datetime import datetime
from multiprocessing.connection import Connection
from multiprocessing.context import Process

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import pydantic

import pypgcdc.decoders as decoders
from pypgcdc.utils import SourceDBHandler

logger = logging.getLogger(__name__)


class ReplicationMessage(pydantic.BaseModel):
    message_id: pydantic.UUID4
    data_start: int
    payload: bytes
    send_time: datetime
    data_size: int
    wal_end: int


class ColumnDefinition(pydantic.BaseModel):
    name: str
    part_of_pkey: bool
    type_id: int
    type_name: str
    optional: bool


class TableSchema(pydantic.BaseModel):
    column_definitions: typing.List[ColumnDefinition]
    db: str
    schema_name: str
    table: str
    relation_id: int


class Transaction(pydantic.BaseModel):
    tx_id: int
    begin_lsn: int
    commit_ts: datetime


class ChangeEvent(pydantic.BaseModel):
    op: str  # (ENUM of I, U, D, T)
    message_id: pydantic.UUID4
    lsn: int
    transaction: Transaction  # replication/source metadata
    table_schema: TableSchema
    before: typing.Optional[typing.Dict[str, typing.Any]]  # depends on the source table
    after: typing.Optional[typing.Dict[str, typing.Any]]


def map_tuple_to_dict(tuple_data: decoders.TupleData, relation: TableSchema) -> typing.OrderedDict[str, typing.Any]:
    """Convert tuple data to an OrderedDict with keys from relation mapped in order to tuple data"""
    output: typing.OrderedDict[str, typing.Any] = OrderedDict()
    for idx, col in enumerate(tuple_data.column_data):
        column_name = relation.column_definitions[idx].name
        output[column_name] = col.col_data
    return output


def convert_pg_type_to_py_type(pg_type_name: str) -> type:
    if pg_type_name in ("bigint", "integer", "smallint"):
        return int
    elif pg_type_name in ("timestamp with time zone", "timestamp without time zone"):
        return datetime
        # json not tested yet
    elif pg_type_name in ("json", "jsonb"):
        return pydantic.Json
    elif pg_type_name[:7] == "numeric":
        return float
    else:
        return str


class LogicalReplicationReader:
    """
    Uses pyscopg2's LogicalReplicationConnection and replication expert
        to extract the raw messages and decode binary pgoutput message
        to B, C, I, U, D or R message type.
        The message is then passed to the transform function.
    """

    def __init__(
        self,
        publication_name: str,
        slot_name: str,
        dsn: str,
        **kwargs: typing.Optional[str],
    ) -> None:
        self.dsn = dsn
        self.publication_name = publication_name
        self.slot_name = slot_name
        self.source_db_handler = SourceDBHandler(dsn=self.dsn)
        self.database = self.source_db_handler.conn.get_dsn_parameters()["dbname"]

        # transform data containers
        self.table_schemas: typing.Dict[int, TableSchema] = dict()  # map relid to table schema

        # for each relation store pydantic model applied to be before/after tuple
        # key only is the schema for before messages that only contain the PK column changes
        self.key_only_table_models: typing.Dict[int, typing.Type[TableSchema]] = dict()
        self.table_models: typing.Dict[int, typing.Type[pydantic.BaseModel]] = dict()

        # save map of type oid to readable name
        self.pg_types: typing.Dict[int, str] = dict()

        self.transaction_metadata = None

    def transform_raw(
        self, message_stream: typing.Generator[ReplicationMessage, None, None]
    ) -> typing.Generator[ChangeEvent, None, None]:
        for msg in message_stream:
            message_type = (msg.payload[:1]).decode("utf-8")
            if message_type == "R":
                yield self.process_relation(message=msg)
            elif message_type == "B":
                self.transaction_metadata = self.process_begin(message=msg)
                yield self.transaction_metadata
            # message processors below will throw an error if transaction_metadata doesn't exist
            elif message_type == "I":
                yield self.process_insert(message=msg, transaction=self.transaction_metadata)
            elif message_type == "U":
                yield self.process_update(message=msg, transaction=self.transaction_metadata)
            elif message_type == "D":
                yield self.process_delete(message=msg, transaction=self.transaction_metadata)
            elif message_type == "T":
                yield from self.process_truncate(message=msg, transaction=self.transaction_metadata)
            elif message_type == "C":
                self.transaction_metadata = None  # null out this value after commit
                yield None

    def process_relation(self, message: ReplicationMessage) -> TableSchema:
        relation_msg: decoders.Relation = decoders.Relation(message.payload)
        relation_id = relation_msg.relation_id
        column_definitions: typing.List[ColumnDefinition] = []
        for column in relation_msg.columns:
            self.pg_types[column.type_id] = self.source_db_handler.fetch_column_type(
                type_id=column.type_id, atttypmod=column.atttypmod
            )
            # pre-compute schema of the table for attaching to messages
            is_optional = self.source_db_handler.fetch_if_column_is_optional(
                table_schema=relation_msg.namespace, table_name=relation_msg.relation_name, column_name=column.name
            )
            column_definitions.append(
                ColumnDefinition(
                    name=column.name,
                    part_of_pkey=column.part_of_pkey,
                    type_id=column.type_id,
                    type_name=self.pg_types[column.type_id],
                    optional=is_optional,
                )
            )
        # in pydantic Ellipsis (...) indicates a field is required
        # this should be the type below, but it doesn't work as the kwargs for create_model with mppy
        # schema_mapping_args: typing.Dict[str, typing.Tuple[type, typing.Optional[EllipsisType]]] = {
        schema_mapping_args: typing.Dict[str, typing.Any] = {
            c.name: (convert_pg_type_to_py_type(c.type_name), None if c.optional else ...) for c in column_definitions
        }
        self.table_models[relation_id] = pydantic.create_model(
            f"DynamicSchemaModel_{relation_id}", **schema_mapping_args
        )

        # key only schema definition
        # https://www.postgresql.org/docs/12/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY
        key_only_schema_mapping_args: typing.Dict[str, typing.Any] = {
            c.name: (convert_pg_type_to_py_type(c.type_name), None if c.optional else ...)
            for c in column_definitions
            if c.part_of_pkey is True
        }
        self.key_only_table_models[relation_id] = pydantic.create_model(
            f"KeyDynamicSchemaModel_{relation_id}", **key_only_schema_mapping_args
        )
        self.table_schemas[relation_id] = TableSchema(
            db=self.database,
            schema_name=relation_msg.namespace,
            table=relation_msg.relation_name,
            column_definitions=column_definitions,
            relation_id=relation_id,
        )
        return self.table_schemas[relation_id]

    def process_begin(self, message: ReplicationMessage) -> Transaction:
        begin_msg: decoders.Begin = decoders.Begin(message.payload)
        return Transaction(tx_id=begin_msg.tx_xid, begin_lsn=begin_msg.lsn, commit_ts=begin_msg.commit_ts)

    def process_insert(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: decoders.Insert = decoders.Insert(message.payload)
        relation_id: int = decoded_msg.relation_id
        after = map_tuple_to_dict(tuple_data=decoded_msg.new_tuple, relation=self.table_schemas[relation_id])
        return ChangeEvent(
            op=decoded_msg.byte1,
            message_id=message.message_id,
            lsn=message.data_start,
            transaction=transaction,
            table_schema=self.table_schemas[relation_id],
            before=None,
            after=self.table_models[relation_id](**after),
        )

    def process_update(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: decoders.Update = decoders.Update(message.payload)
        relation_id: int = decoded_msg.relation_id
        if decoded_msg.old_tuple:
            before_raw = map_tuple_to_dict(tuple_data=decoded_msg.old_tuple, relation=self.table_schemas[relation_id])
            if decoded_msg.optional_tuple_identifier == "O":
                before_typed = self.table_models[relation_id](**before_raw)
            # if there is old tuple and not O then key only schema needed
            else:
                before_typed = self.key_only_table_models[relation_id](**before_raw)
        else:
            before_typed = None
        after = map_tuple_to_dict(tuple_data=decoded_msg.new_tuple, relation=self.table_schemas[relation_id])
        return ChangeEvent(
            op=decoded_msg.byte1,
            message_id=message.message_id,
            lsn=message.data_start,
            transaction=transaction,
            table_schema=self.table_schemas[relation_id],
            before=before_typed,
            after=self.table_models[relation_id](**after),
        )

    def process_delete(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: decoders.Delete = decoders.Delete(message.payload)
        relation_id: int = decoded_msg.relation_id
        before_raw = map_tuple_to_dict(tuple_data=decoded_msg.old_tuple, relation=self.table_schemas[relation_id])
        if decoded_msg.message_type == "O":
            # O is from REPLICA IDENTITY FULL and therefore has all columns in before message
            before_typed = self.table_models[relation_id](**before_raw)
        else:
            # message type is K and means only replica identity index is present in before tuple
            # only DEFAULT is implemented so the index can only be the primary key
            before_typed = self.key_only_table_models[relation_id](**before_raw)
        return ChangeEvent(
            op=decoded_msg.byte1,
            message_id=message.message_id,
            lsn=message.data_start,
            transaction=transaction,
            table_schema=self.table_schemas[relation_id],
            before=before_typed,
            after=None,
        )

    def process_truncate(
        self, message: ReplicationMessage, transaction: Transaction
    ) -> typing.Generator[ChangeEvent, None, None]:
        decoded_msg: decoders.Truncate = decoders.Truncate(message.payload)
        for relation_id in decoded_msg.relation_ids:
            yield ChangeEvent(
                op=decoded_msg.byte1,
                message_id=message.message_id,
                lsn=message.data_start,
                transaction=transaction,
                table_schema=self.table_schemas[relation_id],
                before=None,
                after=None,
            )

    # how to put a better type hint?
    def __iter__(self) -> "LogicalReplicationReader":
        return self

    def __next__(self) -> ChangeEvent:
        try:
            return next(self.transformed_msgs)
        except Exception as err:
            self.stop()
            raise StopIteration from err
