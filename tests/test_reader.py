import logging
import os
import typing
from unittest.mock import MagicMock

import psycopg2
import psycopg2.errors as psycopg_errors
import psycopg2.extensions
import psycopg2.extras
import pytest

import pypgcdc
from pypgcdc import LogicalReplicationReader, MetadataStore, ChangeEvent, OperationType, ReplicationMessage

DSN = os.environ.get("PYPGCDC_DSN", "postgres://postgres:postgrespw@localhost:5432/unittest")
SLOT_NAME = os.environ.get("PYPGCDC_SLOT", "unittest_slot")
PUBLICATION_NAME = os.environ.get("PYPGCDC_PUBLICATION", "unittest_publication")


TEST_TABLE_DDL = """
CREATE TABLE public.integration (
        id integer primary key,
        json_data jsonb,
        amount numeric(10, 2),
        updated_at timestamptz not null,
        text_data text
);
CREATE TABLE public.control (
        id integer primary key,
        command text
);
"""
TEST_TABLE_COLUMNS = ["id", "json_data", "amount", "updated_at", "text_data"]

INSERT_STATEMENT = """
INSERT INTO public.integration (id, json_data, amount, updated_at, text_data)
VALUES (10, '{"data": 10}', 10.20, '2020-01-01 00:00:00+00', 'dummy_value');
"""

UPDATE_STATEMENT = """
UPDATE public.integration SET json_data = '{"data": 20}' WHERE id = 10;
"""

DELETE_STATEMENT = """
DELETE FROM public.integration WHERE id = 10;
"""

MESSAGE_MARKER = """
INSERT INTO public.control (id, command)
VALUES (1, 'exit');
"""


logger = logging.getLogger("tests")
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.INFO)
logger.addHandler(log_handler)


@pytest.fixture(scope="module")
def cursor() -> typing.Generator[psycopg2.extras.DictCursor, None, None]:
    connection = psycopg2.connect(DSN)
    connection.autocommit = True
    curs = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    version_query = "SELECT version();"
    curs.execute(version_query)
    result = curs.fetchone()
    logger.info(f"PG test version: {result}")
    yield curs
    curs.close()
    connection.close()


@pytest.fixture(scope="function")
def configure_db(
    cursor: psycopg2.extras.DictCursor,
) -> None:
    cursor.execute(f"DROP PUBLICATION IF EXISTS {PUBLICATION_NAME};")
    try:
        cursor.execute(f"SELECT pg_drop_replication_slot('{SLOT_NAME}');")
    except psycopg_errors.UndefinedObject as err:
        logger.warning(f"slot {SLOT_NAME} could not be dropped because it does not exist. {err}")
    cursor.execute(f"CREATE PUBLICATION {PUBLICATION_NAME} FOR ALL TABLES;")


@pytest.fixture(scope="function")
def cdc_reader(
    cursor: psycopg2.extras.DictCursor, configure_db: None
) -> typing.Generator[pypgcdc.LogicalReplicationReader, None, None]:
    meta = MetadataStore()
    data_store = MagicMock()
    reader = LogicalReplicationReader(
        dsn=DSN,
        publication_name=PUBLICATION_NAME,
        slot_name=SLOT_NAME,
        metadata_store=meta,
        data_store=data_store,
    )

    # assumes all tables are in publication
    query = f"""
    DROP TABLE IF EXISTS public.integration CASCADE;
    DROP TABLE IF EXISTS public.control CASCADE;
    {TEST_TABLE_DDL}
    """
    cursor.execute(query)

    yield reader


def test_dummy_test(cursor: psycopg2.extras.DictCursor) -> None:
    """make sure connection/cursor and DB is operational for tests"""
    cursor.execute("SELECT 1 as n;")
    result = cursor.fetchone()
    assert result["n"] == 1


def test_slot_logic(cdc_reader: pypgcdc.LogicalReplicationReader, cursor: psycopg2.extras.DictCursor) -> None:
    cursor.execute(INSERT_STATEMENT)
    with cdc_reader:
        pass
    cursor.execute(MESSAGE_MARKER)
    assert cdc_reader.data_store.handle_slot_created.call_count == 1
    cdc_reader.data_store.reset_mock()
    with cdc_reader:
        cdc_reader.consume_stream(max_count=1)
    assert cdc_reader.data_store.handle_slot_created.call_count == 0


def test_event_logic(cdc_reader: pypgcdc.LogicalReplicationReader, cursor: psycopg2.extras.DictCursor) -> None:
    cdc_reader.start_replication()
    cdc_reader.data_store.reset_mock()
    cursor.execute(INSERT_STATEMENT)
    with cdc_reader:
        cdc_reader.consume_stream(max_count=4)
    assert cdc_reader.data_store.handle_slot_created.call_count == 0
    assert cdc_reader.data_store.handle_begin.call_count == 1
    assert cdc_reader.data_store.handle_change_event.call_count == 1
    assert cdc_reader.data_store.handle_commit.call_count == 1


def test_event_logic_extended(cdc_reader: pypgcdc.LogicalReplicationReader, cursor: psycopg2.extras.DictCursor) -> None:
    cdc_reader.start_replication()
    cdc_reader.data_store.reset_mock()

    def exit_on_event(event: ChangeEvent, message: ReplicationMessage) -> None:
        if (
            event.op == OperationType.INSERT.value
            and event.key.get("table") == "control"
            and event.after.get("command") == "exit"
        ):
            raise StopIteration

    cdc_reader.data_store.handle_change_event.side_effect = exit_on_event
    cursor.execute(INSERT_STATEMENT)
    cursor.execute(UPDATE_STATEMENT)
    cursor.execute(DELETE_STATEMENT)
    cursor.execute(MESSAGE_MARKER)
    with cdc_reader:
        cdc_reader.consume_stream(max_count=16)
    assert cdc_reader.data_store.handle_slot_created.call_count == 0
    assert cdc_reader.data_store.handle_begin.call_count == 4
    assert cdc_reader.data_store.handle_change_event.call_count == 4
    assert cdc_reader.data_store.handle_commit.call_count == 3
