# DataSplit golden fixtures

These are byte-for-byte copies of the v8 split serializations produced by
paimon-cpp's `paimon::Split::Serialize`. They are used by
`crates/paimon/src/table/split_serde.rs` integration tests as the golden
truth for cross-language wire-format compatibility.

paimon-cpp asserts that `Serialize(Deserialize(bytes)) == bytes` (see
`paimon-cpp/src/paimon/core/table/source/data_split_test.cpp`), so these
fixtures are deterministic and we can use them for byte-equality round-trip
tests on the Rust side.

| File | paimon-cpp source path | C++ test reference |
|---|---|---|
| `data_split-02_pk_dv_index_in_data_with_external` | `test/test_data/orc/pk_dv_index_in_data_with_external.db/pk_dv_index_in_data_with_external/data_splits/data_split-02` | `data_split_test.cpp::TestDeserializeVersion8WithWriteColsAndExternalPath` (lines 43–106) |
| `data_split-02_pk_dv_index_not_in_data_no_external` | `test/test_data/orc/pk_dv_index_not_in_data_no_external.db/pk_dv_index_not_in_data_no_external/data_splits/data_split-02` | `data_split_test.cpp::TestDeserializeVersion8WithWriteCols` (lines 108–170) |
| `data_split-01_append_10` | `test/test_data/orc/append_10.db/append_10/data_splits/data_split-01` | `data_split_test.cpp::TestDeserializeVersion8AppendTable` (~line 480) |
| `data_split-01_pk_table_with_total_buckets` | `test/test_data/orc/pk_table_with_total_buckets.db/pk_table_with_total_buckets/data_splits/data_split-01` | `data_split_test.cpp::TestDeserializeVersion8TotalBuckets` (~line 280) |

To regenerate (e.g. after a wire-format change in paimon-cpp), copy the
corresponding `data_splits/data_split-XX` files back into this directory.
