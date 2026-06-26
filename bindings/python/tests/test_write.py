# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import tempfile

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


def _make_empty_table(warehouse):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": warehouse})
    ctx.sql("CREATE SCHEMA paimon.wdb")
    ctx.sql("CREATE TABLE paimon.wdb.t (id INT, name STRING)")
    return ctx


def _get_table(warehouse):
    return PaimonCatalog({"warehouse": warehouse}).get_table("wdb.t")


def test_write_commit_read_roundtrip():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = _make_empty_table(warehouse)
        table = _get_table(warehouse)
        batch = pa.record_batch([[1, 2, 3], ["a", "b", "c"]], names=["id", "name"])
        wb = table.new_write_builder()
        write = wb.new_write()
        write.write_arrow(batch)
        messages = write.prepare_commit()
        assert len(messages) >= 1                # cover API shape in the first test
        wb.new_commit().commit(messages)   # same wb → shared commit_user
        result = pa.Table.from_batches(
            ctx.sql("SELECT id, name FROM paimon.wdb.t")
        ).sort_by("id").to_pydict()
        assert result == {"id": [1, 2, 3], "name": ["a", "b", "c"]}
