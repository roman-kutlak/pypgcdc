import logging
import os
import uuid

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import psycopg2.errors

from pypgcdc.reader import ReplicationMessage, LogicalReplicationReader

HOST = os.environ.get("PGHOST", "localhost")
PORT = os.environ.get("PGPORT", "5432")
DATABASE_NAME = os.environ.get("PGDATABASE", "postgres")
USER = os.environ.get("PGUSER", "postgres")
PASSWORD = os.environ.get("PGPASSWORD", "postgres")

LSN = 0

logger = logging.getLogger()


class CDCStream:
    def __init__(self, dsn: str, publication_name: str, slot_name: str) -> None:
        self.dsn = dsn
        self.publication_name = publication_name
        self.slot_name = slot_name
        self.reader = LogicalReplicationReader(publication_name, slot_name, dsn)

    def read(self) -> None:
        replication_options = {
            "publication_names": self.publication_name,
            "proto_version": "1",
        }
        with psycopg2.extras.LogicalReplicationConnection(self.dsn) as conn:
            with psycopg2.extras.ReplicationCursor(conn) as cur:
                try:
                    cur.start_replication(
                        slot_name=self.slot_name, decode=False, options=replication_options, start_lsn=LSN
                    )
                except psycopg2.errors.UndefinedObject:
                    cur.execute("commit;")
                    cur.create_replication_slot(self.slot_name, output_plugin="pgoutput")
                    res = cur.fetchone()
                    self.slot_created(res)
                    cur.start_replication(
                        slot_name=self.slot_name, decode=False, options=replication_options, start_lsn=LSN
                    )
                try:
                    logger.info(f"Starting replication from slot: '{self.slot_name}'")
                    cur.consume_stream(self.process)
                except KeyboardInterrupt:
                    logger.info(f"Stopping replication from slot: '{self.slot_name}'")
                except Exception as err:
                    logger.error(f"Error consuming stream from slot: '{self.slot_name}'. {err}")

    def slot_created(self, result):
        logger.warning(f"New slot created: {result}")
        snapshot = result[2]
        q1 = "begin transaction isolation level repeatable read"
        q2 = f"set transaction snapshot '{snapshot}'"
        q3 = f"select * from pg_publication_tables where pubname = '{self.publication_name}'"
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(q1)
                cur.execute(q2)
                cur.execute(q3)
                result = cur.fetchall()
                for row in result:
                    logger.info(f"Processing table {row.schemaname}.{row.tablename}")

    def process(self, msg: psycopg2.extras.ReplicationMessage):
        message_id = uuid.uuid4()
        message = ReplicationMessage(
            message_id=message_id,
            data_start=msg.data_start,
            payload=msg.payload,
            send_time=msg.send_time,
            data_size=msg.data_size,
            wal_end=msg.wal_end,
        )
        print((message.data_start, hex(message.data_start), message.payload))
        msgs = list(self.reader.transform_raw([message]))
        msg.cursor.send_feedback(flush_lsn=message.data_start)
        for m in msgs:
            if m:
                print(m.json(indent=2))
        print("-" * 80)


def run():
    # supported params: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    dsn = psycopg2.extensions.make_dsn(
        dsn=None,
        host=HOST,
        database=DATABASE_NAME,
        port=PORT,
        user=USER,
        password=PASSWORD,
        application_name='replicator',
    )
    logging.basicConfig(level=logging.DEBUG)
    cdc_reader = CDCStream(
        dsn=dsn,
        publication_name="test_pub",
        slot_name="test_slot3",
    )
    cdc_reader.read()


if __name__ == "__main__":
    run()
