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

from pypgcdc.decoders import (
    Begin,
    ColumnData,
    ColumnType,
    Commit,
    Delete,
    Insert,
    Origin,
    PgoutputMessage,
    Relation,
    Truncate,
    TupleData,
    Update,
)
from pypgcdc.models import (
    ChangeEvent,
    ReplicationMessage,
    SlotInitInfo,
    TableSchema,
    Transaction,
)
from pypgcdc.reader import LogicalReplicationReader
from pypgcdc.stores import DataStore, MetadataStore
from pypgcdc.utils import QueryError, SourceDBHandler

logging.getLogger("pypgcdc").addHandler(logging.NullHandler())


__all__ = [
    "PgoutputMessage",
    "Begin",
    "Commit",
    "Origin",
    "Relation",
    "TupleData",
    "Insert",
    "Update",
    "Delete",
    "Truncate",
    "ColumnData",
    "ColumnType",
    "SourceDBHandler",
    "LogicalReplicationReader",
    "QueryError",
    "Transaction",
    "SlotInitInfo",
    "ChangeEvent",
    "ReplicationMessage",
    "TableSchema",
    "DataStore",
    "MetadataStore",
]