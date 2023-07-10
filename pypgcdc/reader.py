"""
MIT License

Copyright (c) [2020] [Daniel Geals]
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
import logging
import typing
import uuid
from collections import OrderedDict
from datetime import datetime

import psycopg2
import psycopg2.extras
import pydantic

import pypgcdc.decoders as decoders
from pypgcdc.models import TableSchema, Transaction, ChangeEvent, SlotInitInfo, ReplicationMessage, ColumnDefinition
from pypgcdc.stores import MetadataStore, StateStore, ChangeStore
from pypgcdc.utils import SourceDBHandler

logger = logging.getLogger(__name__)


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
        change_store: ChangeStore,
        state_store: StateStore,
        lsn: str = 0,
        metadata_store: MetadataStore = None,
    ) -> None:
        self.dsn = dsn
        self.publication_name = publication_name
        self.slot_name = slot_name
        self.lsn = lsn
        self.change_store = change_store
        self.state_store = state_store
        self.metadata_store = metadata_store
        self.source_db_handler = SourceDBHandler(dsn=self.dsn)
        self.database = self.source_db_handler.conn.get_dsn_parameters()["dbname"]
        self.connection = None
        self.cursor = None
        self.transaction_metadata = None

    def transform_raw(self, msg: ReplicationMessage) -> typing.Union[ChangeEvent, Transaction, TableSchema]:
        message_type = (msg.payload[:1]).decode("utf-8")
        if message_type == "R":
            return self.process_relation(message=msg)
        elif message_type == "B":
            self.transaction_metadata = self.process_begin(message=msg)
            return self.transaction_metadata
        # message processors below will throw an error if transaction_metadata doesn't exist
        elif message_type == "I":
            event = self.process_insert(message=msg, transaction=self.transaction_metadata)
            self._add_key(event)
            return event
        elif message_type == "U":
            event = self.process_update(message=msg, transaction=self.transaction_metadata)
            self._add_key(event)
            return event
        elif message_type == "D":
            event = self.process_delete(message=msg, transaction=self.transaction_metadata)
            self._add_key(event)
            return event
        elif message_type == "T":
            return self.process_truncate(message=msg, transaction=self.transaction_metadata)
        elif message_type == "C":
            txn = self.transaction_metadata.copy()
            txn.op = "C"
            self.transaction_metadata = None  # null out this value after commit
            return txn

    def _add_key(self, event: ChangeEvent) -> typing.Dict[str, typing.Any]:
        """Add a key to the event; this is either the PK or the whole row"""
        if event.before:
            key = dict(**event.before)
        else:
            key_columns = event.table_schema.get_key_columns()
            key = {col: event.after[col] for col in key_columns}
        key["database"] = self.database
        key["namespace"] = event.table_schema.namespace
        key["table"] = event.table_schema.table
        event.key = key
        return key

    def process_relation(self, message: ReplicationMessage) -> TableSchema:
        relation_msg: decoders.Relation = decoders.Relation(message.payload)
        relation_id = relation_msg.relation_id
        schema = self.metadata_store.table_schema(self.database, relation_id)
        if schema:
            return schema

        column_definitions: typing.List[ColumnDefinition] = []
        for column in relation_msg.columns:
            pg_type = self.metadata_store.column_type(self.database, column.type_id)
            if not pg_type:
                pg_type = self.source_db_handler.fetch_column_type(
                    type_id=column.type_id, atttypmod=column.atttypmod
                )
                self.metadata_store.add_column_type(self.database, column.type_id, pg_type)
            # pre-compute schema of the table for attaching to messages
            is_optional = self.source_db_handler.fetch_if_column_is_optional(
                table_schema=relation_msg.namespace, table_name=relation_msg.relation_name, column_name=column.name
            )
            column_definitions.append(
                ColumnDefinition(
                    name=column.name,
                    part_of_pkey=column.part_of_pkey,
                    type_id=column.type_id,
                    type_name=pg_type,
                    optional=is_optional,
                )
            )
        # in pydantic Ellipsis (...) indicates a field is required
        # this should be the type below, but it doesn't work as the kwargs for create_model with mppy
        # schema_mapping_args: typing.Dict[str, typing.Tuple[type, typing.Optional[EllipsisType]]] = {
        schema_mapping_args: typing.Dict[str, typing.Any] = {
            c.name: (convert_pg_type_to_py_type(c.type_name), None if c.optional else ...) for c in column_definitions
        }
        table_model = pydantic.create_model(f"DynamicSchemaModel_{relation_id}", **schema_mapping_args)
        self.metadata_store.add_table_model(self.database, relation_id, table_model)

        # key only schema definition
        # https://www.postgresql.org/docs/12/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY
        key_only_schema_mapping_args: typing.Dict[str, typing.Any] = {
            c.name: (convert_pg_type_to_py_type(c.type_name), None if c.optional else ...)
            for c in column_definitions
            if c.part_of_pkey is True
        }
        key_schema = pydantic.create_model(
            f"KeyDynamicSchemaModel_{relation_id}", **key_only_schema_mapping_args
        )
        self.metadata_store.add_key_model(self.database, relation_id, key_schema)
        
        table_schema = TableSchema(
            db=self.database,
            namespace=relation_msg.namespace,
            table=relation_msg.relation_name,
            column_definitions=column_definitions,
            relation_id=relation_id,
        )
        self.metadata_store.add_table_schema(self.database, relation_id, table_schema)
        return table_schema

    def process_begin(self, message: ReplicationMessage) -> Transaction:
        begin_msg: decoders.Begin = decoders.Begin(message.payload)
        return Transaction(op='B', tx_id=begin_msg.tx_xid, begin_lsn=begin_msg.lsn, commit_ts=begin_msg.commit_ts)

    def process_insert(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: decoders.Insert = decoders.Insert(message.payload)
        table_schema = self.metadata_store.table_schema(self.database, decoded_msg.relation_id)
        table_model = self.metadata_store.table_model(self.database, decoded_msg.relation_id)
        after = map_tuple_to_dict(tuple_data=decoded_msg.new_tuple, relation=table_schema)
        return ChangeEvent(
            op=decoded_msg.byte1,
            message_id=message.message_id,
            lsn=message.data_start,
            transaction=transaction,
            table_schema=table_schema,
            before=None,
            after=table_model(**after),
        )

    def process_update(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: decoders.Update = decoders.Update(message.payload)
        table_model = self.metadata_store.table_model(self.database, decoded_msg.relation_id)
        table_schema = self.metadata_store.table_schema(self.database, decoded_msg.relation_id)
        key_model = self.metadata_store.key_model(self.database, decoded_msg.relation_id)
        if decoded_msg.old_tuple:
            before_raw = map_tuple_to_dict(tuple_data=decoded_msg.old_tuple, relation=table_schema)
            if decoded_msg.optional_tuple_identifier == "O":
                before_typed = table_model(**before_raw)
            # if there is old tuple and not O then key only schema needed
            else:
                before_typed = key_model(**before_raw)
        else:
            before_typed = None
        after = map_tuple_to_dict(tuple_data=decoded_msg.new_tuple, relation=table_schema)
        return ChangeEvent(
            op=decoded_msg.byte1,
            message_id=message.message_id,
            lsn=message.data_start,
            transaction=transaction,
            table_schema=table_schema,
            before=before_typed,
            after=table_model(**after),
        )

    def process_delete(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: decoders.Delete = decoders.Delete(message.payload)
        table_model = self.metadata_store.table_model(self.database, decoded_msg.relation_id)
        table_schema = self.metadata_store.table_schema(self.database, decoded_msg.relation_id)
        key_model = self.metadata_store.key_model(self.database, decoded_msg.relation_id)
        before_raw = map_tuple_to_dict(tuple_data=decoded_msg.old_tuple, relation=table_schema)
        if decoded_msg.message_type == "O":
            # O is from REPLICA IDENTITY FULL and therefore has all columns in before message
            before_typed = table_model(**before_raw)
        else:
            # message type is K and means only replica identity index is present in before tuple
            # only DEFAULT is implemented so the index can only be the primary key
            before_typed = key_model(**before_raw)
        return ChangeEvent(
            op=decoded_msg.byte1,
            message_id=message.message_id,
            lsn=message.data_start,
            transaction=transaction,
            table_schema=table_schema,
            before=before_typed,
            after=None,
        )

    def process_truncate(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: decoders.Truncate = decoders.Truncate(message.payload)
        yield ChangeEvent(
            op=decoded_msg.byte1,
            message_id=message.message_id,
            lsn=message.data_start,
            transaction=transaction,
            table_schemata=[self.metadata_store.table_schema(self.database, relation_id)
                            for relation_id in decoded_msg.relation_ids],
            before=None,
            after=None,
        )

    def consume_stream(self):
        if not self.cursor:
            raise RuntimeError("Cursor not initialized; use `with` statement or call `init_cursor` first")
        logger.info(f"Starting replication from: {self.database}/{self.publication_name}/{self.slot_name}")
        self.cursor.consume_stream(self)

    def __call__(self, msg: psycopg2.extras.ReplicationMessage):
        try:
            message_id = uuid.uuid4()
            message = ReplicationMessage(
                message_id=message_id,
                data_start=msg.data_start,
                payload=msg.payload,
                send_time=msg.send_time,
                data_size=msg.data_size,
                wal_end=msg.wal_end,
            )
            change_event = self.transform_raw(message)

            if isinstance(change_event, TableSchema):
                self.change_store.handle_relation(change_event, message)
            elif change_event.op == "B":
                self.state_store.handle_begin(change_event, msg)
            elif change_event.op == "C":
                self.state_store.handle_commit(change_event, msg)
            else:
                self.change_store.handle_change_event(change_event, message)
        except Exception as err:
            raise StopIteration from err

    def __enter__(self) -> psycopg2.extras.ReplicationCursor:
        replication_options = {
            "publication_names": self.publication_name,
            "proto_version": "1",
        }
        self.connection = psycopg2.extras.LogicalReplicationConnection(self.dsn)
        self.cursor = psycopg2.extras.ReplicationCursor(self.connection)
        try:
            self.cursor.start_replication(
                slot_name=self.slot_name, options=replication_options, start_lsn=self.lsn, decode=False
            )
        except psycopg2.errors.UndefinedObject:  # noqa
            self.cursor.execute("commit;")
            self.cursor.create_replication_slot(self.slot_name, output_plugin="pgoutput")
            res = self.cursor.fetchone()
            info = SlotInitInfo(
                dsn=self.dsn,
                publication_name=self.publication_name,
                slot_name=res[0],
                flush_lsn=res[1],
                snapshot=res[2],
                plugin=res[3],
            )
            self.change_store.handle_slot_created(info)
            self.cursor.start_replication(
                slot_name=self.slot_name, options=replication_options, start_lsn=self.lsn, decode=False
            )
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

