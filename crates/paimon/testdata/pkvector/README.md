# Primary-key vector search — end-to-end demo fixture

This directory backs the runnable end-to-end demo
`crates/paimon/examples/pk_vector_search_demo.rs`, which exercises the
primary-key vector search **read** path against a table **written by Apache
Paimon's Java writer** (not assembled in Rust).

Run the demo:

```
cargo run --example pk_vector_search_demo -p paimon
```

A clean run prints one `[PASS]` line per scenario and ends with
`=== ALL SCENARIOS PASSED ===`. The captured output is committed at
`pk_vector_demo.expected-output.txt` (same directory).

## Files

- `pk_vector_demo/` — the Java-written table directory (schema, data parquet,
  the real `ivf-flat` ANN index segment, index-manifest, snapshots). Opaque
  binary; regenerate rather than hand-edit.
- `generator/PkVectorFixtureGenerator.java` — a byte-identical copy of the
  Apache Paimon Java test that produced the table, vendored here so the fixture
  is reproducible from this repo. It also produces `pk_vector_ivf_flat` (used by
  `crates/paimon/tests/pk_vector_java_fixture_test.rs`).
- `pk_vector_demo.expected-output.txt` — the demo's printed output.

## How the table was produced

- Source: Apache Paimon Java, module `paimon-vector`, commit `c0a9dca3d`.
- Generator method: `PkVectorFixtureGenerator#generatePkVectorDemoFixture`
  (writes via the production write path, then `compact(...)` builds the real
  primary-key ANN segment).
- Command (from the Java repo root):

  ```
  mvn -pl paimon-vector test \
      -Dtest='PkVectorFixtureGenerator#generatePkVectorDemoFixture' \
      -Dgen.pkvector.fixture=true -Drun.e2e.tests=true \
      -Dspotless.check.skip=true -Dcheckstyle.skip=true
  ```

  The table is emitted to `paimon-vector/target/pkvector-fixture/pk_vector_demo`;
  copy that into `pk_vector_demo/` here.
- Table config: primary key `id`, vector column `embedding VECTOR(2, FLOAT)`,
  `ivf-flat` / metric `l2` / `nlist = 1` (single inverted list scanned in full,
  so the ANN search is exact and deterministic), `deduplicate` merge engine,
  deletion-vectors enabled (the precondition for a residual data predicate on the
  PK-vector read path). Java writes no `first_row_id` on a PK table, so
  `id == row position`.
- Fixture tree checksum: `d5001e91911ca384c20fd87e83e6e2d05a93fd6a`
  (`find pk_vector_demo -type f -exec shasum {} \; | awk '{print $1}' | sort | shasum`).

## Dataset

Six rows, `id == row position`, 2-D vectors:

| id | embedding |
|----|-----------|
| 0  | [0, 4]    |
| 1  | [8, 0]    |
| 2  | [0, 5]    |
| 3  | [7, 0]    |
| 4  | [0, 6]    |
| 5  | [9, 0]    |

Squared-L2 distance is the ground-truth metric; the read path's score is
`1 / (1 + distance)`. The dataset is chosen so every query used below has strict
distance gaps, making each top-k order unique.

## Scenarios and expected results

All scenarios open the **same** Java-written table and vary only the read.

| # | Scenario | Read-side input | Expected top-3 (ids) |
|---|----------|-----------------|----------------------|
| 0 | Fixture integrity | plain read of `(id, embedding)` | all 6 rows == dataset above |
| 1+2 | Single-query top-k, best-first | query `[10, 0]` | `[5, 1, 3]` (not id/position order); scores `[0.5, 0.2, 0.1]` |
| 3 | Residual filter | query `[10, 0]`, filter `id >= 3` | `[5, 3, 4]` (differs from unfiltered `[5, 1, 3]`) |
| 4 | Batch multi-vector | queries `[10,0]`, `[0,10]`, `[6,3]` | `[5,1,3]`, `[4,2,0]`, `[3,1,5]`; batch-of-one == single |
| 5 | Refine-factor rerank | `fields.embedding.ivf.refine-factor=4` (read-time `copy_with_options`) | `[5, 1, 3]` (exact-preserving; `nlist=1` is already exhaustive) |
| 6 | `execute_scored()` fails loud | PK-vector table | error pointing at `execute_read` |
| 7 | Concurrency | `global-index.thread-num` = 4 vs 1 (read-time) | `[5, 1, 3]` identical (concurrency changes fan-out, not the answer) |
| 8a | Wrong-case vector column | `with_vector_column("EMBEDDING")` | fails loud (vector-column matching is exact by design) |
| 8b | Wrong-case predicate column | filter on `"ID" >= 3` | resolves under the case-insensitive default → `[5, 3, 4]` |

Each scenario computes its expected top-k independently in Rust (brute-force
exact squared-L2 over the dataset) and asserts the read result matches, so the
demo is self-validating.
