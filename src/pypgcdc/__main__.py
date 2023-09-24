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
import contextlib
import logging
import os

from pypgcdc import DataStore, LogicalReplicationReader, MetadataStore

DSN = os.environ.get("PYPGCDC_DSN", "postgres://postgres:postgrespw@localhost:5432/test")
SLOT = os.environ.get("PYPGCDC_SLOT", "test_slot")
PUBLICATION = os.environ.get("PYPGCDC_PUBLICATION", "test_publication")
LSN = os.environ.get("PYPGCDC_LSN", 0)
QUIET = os.environ.get("PYPGCDC_VERBOSE", "False").lower() not in ("1", "true", "yes")


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("pypgcdc").info(f"Running logical replication with ")
    meta = MetadataStore()
    data = DataStore(quiet=QUIET)
    cdc_reader = LogicalReplicationReader(
        dsn=DSN,
        publication_name=PUBLICATION,
        slot_name=SLOT,
        lsn=LSN,
        metadata_store=meta,
        data_store=data,
    )
    with contextlib.suppress(KeyboardInterrupt):
        with cdc_reader:
            cdc_reader.consume_stream()


if __name__ == "__main__":
    main()
