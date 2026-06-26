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

import pickle
import tempfile

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


def _make_table_with_data(warehouse):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": warehouse})
    ctx.sql("CREATE SCHEMA paimon.rdb")
    ctx.sql("CREATE TABLE paimon.rdb.t (id INT, name STRING)")
    ctx.sql("INSERT INTO paimon.rdb.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    catalog = PaimonCatalog({"warehouse": warehouse})
    return catalog.get_table("rdb.t")


def test_read_builder_chain_exists():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        builder = table.new_read_builder()
        scan = builder.with_projection(["id"]).with_limit(2).new_scan()
        # plan() returns a Plan; deeper assertions are in later tasks.
        plan = scan.plan()
        assert plan is not None


def test_new_read_builder_plan():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        plan = table.new_read_builder().new_scan().plan()
        assert len(plan.splits()) >= 1


def test_with_projection():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        plan = table.new_read_builder().with_projection(["id"]).new_scan().plan()
        assert plan is not None


def test_with_limit():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        # limit is a planning hint; assert only that planning succeeds.
        plan = table.new_read_builder().with_limit(1).new_scan().plan()
        assert plan is not None


def test_plan_len():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        plan = table.new_read_builder().new_scan().plan()
        assert len(plan) == len(plan.splits())


def test_plan_without_filter_succeeds():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        plan = table.new_read_builder().new_scan().plan()
        assert len(plan.splits()) >= 1


def test_split_pickle_roundtrip():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        splits = table.new_read_builder().new_scan().plan().splits()
        assert len(splits) >= 1
        split = splits[0]
        restored = pickle.loads(pickle.dumps(split))
        assert restored.row_count() == split.row_count()
