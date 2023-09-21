import typing
from datetime import datetime
from enum import Enum

import pydantic


class ReplicationMessage(pydantic.BaseModel):
    message_id: pydantic.UUID4
    data_start: int
    payload: bytes
    send_time: datetime
    data_size: int
    wal_end: int


class OperationType(Enum):
    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"
    TRUNCATE = "T"
    BEGIN = "B"
    COMMIT = "C"
    ORIGIN = "O"
    RELATION = "R"


class ColumnDefinition(pydantic.BaseModel):
    name: str
    part_of_pkey: bool
    type_id: int
    type_name: str
    optional: bool


class TableSchema(pydantic.BaseModel):
    column_definitions: typing.List[ColumnDefinition]
    db: str
    namespace: str
    table: str
    relation_id: int

    def get_key_columns(self) -> typing.List[str]:
        return [col.name for col in self.column_definitions if col.part_of_pkey]


class Transaction(pydantic.BaseModel):
    op: str
    tx_id: int
    begin_lsn: int
    commit_lsn: typing.Optional[int]
    commit_ts: datetime


class SlotInitInfo(pydantic.BaseModel):
    dsn: str
    publication_name: str
    slot_name: str
    flush_lsn: str
    snapshot: str
    plugin: str


class ChangeEvent(pydantic.BaseModel):
    op: OperationType
    message_id: pydantic.UUID4
    lsn: int
    transaction: Transaction  # replication/source metadata
    table_schema: TableSchema
    table_schemata: typing.Optional[typing.List[TableSchema]]
    before: typing.Optional[typing.Dict[str, typing.Any]]  # depends on the source table
    after: typing.Optional[typing.Dict[str, typing.Any]]
    key: typing.Optional[typing.Dict[str, typing.Any]]

    class Config:
        use_enum_values = True
