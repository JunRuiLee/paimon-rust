// SPDX-License-Identifier: Apache-2.0
// C++ port of crates/paimon/examples/test.rs, using bindings/c (paimon-c).
//
// What it does (mirrors the Rust example):
//   1. Build a catalog backed by a local warehouse path
//   2. Open table `test.testrust0`
//   3. Project columns ["id", "embedding"], plan + read as Arrow batches
//   4. Print one line per batch (schema name + row count + first cell of col 0)
//
// We only depend on:
//   - bindings/c/include/paimon.h  (the cbindgen-generated header)
//   - libpaimon_c.so               (the cdylib)
// We do NOT pull in Arrow C++. Instead we reproduce the small Arrow
// C Data Interface ABI structs here, so the demo stays a single-file build.
//
// Reference: https://arrow.apache.org/docs/format/CDataInterface.html

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

extern "C" {
#include "paimon.h"
}

// ---------------------------------------------------------------------------
// Arrow C Data Interface — minimal copy of the public ABI.
//
// `paimon_arrow_batch::array` and `::schema` are heap-allocated `ArrowArray`
// and `ArrowSchema` structs whose layouts match these definitions exactly.
// We never inspect buffers here; we just call their `release` callbacks to
// hand ownership back, and free the container structs via the paimon API.
// ---------------------------------------------------------------------------
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

// Render a paimon_bytes (which is NOT null-terminated) as a std::string.
static std::string bytes_to_string(const paimon_bytes& b) {
    if (b.data == nullptr || b.len == 0) return {};
    return std::string(reinterpret_cast<const char*>(b.data), b.len);
}

// If `err` is non-null: print it, free it, return true.
// Caller should treat true as "abort the demo".
//
// Defined below, after the RAII wrappers, so it can take ownership of `err`
// via error_ptr instead of calling paimon_error_free by hand.
static bool check_err(const char* what, paimon_error* err);

// Stringify the Arrow type "format" field per the C Data Interface spec.
// Just enough for a friendly print — full coverage would be a switch on the
// 30+ format strings, which we don't need here.
static const char* arrow_format_str(const ArrowSchema* s) {
    return s && s->format ? s->format : "(unknown)";
}

// ---------------------------------------------------------------------------
// RAII wrappers
//
// Every paimon_* handle is an opaque pointer owned by Rust and freed by a
// matching paimon_*_free function. We wrap each in a std::unique_ptr with a
// custom deleter so the handle is released automatically when it goes out of
// scope — no manual teardown, and every early `return` cleans up correctly in
// reverse construction order.
//
// PAIMON_OWNED(type, freefn) defines `paimon_<type>_ptr`, a move-only smart
// pointer over `paimon_<type>*` that calls `freefn` on destruction.
// ---------------------------------------------------------------------------
#define PAIMON_OWNED(type, freefn)                          \
    struct type##_deleter {                                 \
        void operator()(paimon_##type* p) const {           \
            if (p) freefn(p);                               \
        }                                                   \
    };                                                      \
    using type##_ptr = std::unique_ptr<paimon_##type, type##_deleter>

PAIMON_OWNED(catalog, paimon_catalog_free);
PAIMON_OWNED(error, paimon_error_free);
PAIMON_OWNED(identifier, paimon_identifier_free);
PAIMON_OWNED(table, paimon_table_free);
PAIMON_OWNED(read_builder, paimon_read_builder_free);
PAIMON_OWNED(table_scan, paimon_table_scan_free);
PAIMON_OWNED(plan, paimon_plan_free);
PAIMON_OWNED(table_read, paimon_table_read_free);
PAIMON_OWNED(record_batch_reader, paimon_record_batch_reader_free);

#undef PAIMON_OWNED

// One Arrow batch (schema + array containers). Owning it requires a two-step
// teardown that the unique_ptr deleters above can't express: first invoke the
// Arrow C Data Interface `release` callback on each struct (hands buffers back
// to the producer), then free the container structs via paimon_arrow_batch_free.
class ArrowBatch {
public:
    explicit ArrowBatch(paimon_arrow_batch batch) : batch_(batch) {}
    ~ArrowBatch() {
        auto* schema = static_cast<ArrowSchema*>(batch_.schema);
        auto* array = static_cast<ArrowArray*>(batch_.array);
        if (array && array->release) array->release(array);
        if (schema && schema->release) schema->release(schema);
        paimon_arrow_batch_free(batch_);
    }

    ArrowBatch(const ArrowBatch&) = delete;
    ArrowBatch& operator=(const ArrowBatch&) = delete;

    ArrowSchema* schema() const { return static_cast<ArrowSchema*>(batch_.schema); }
    ArrowArray* array() const { return static_cast<ArrowArray*>(batch_.array); }

private:
    paimon_arrow_batch batch_;
};

// Definition of check_err (declared above): take ownership of `err` in an
// error_ptr so it is freed on every return path without a manual free.
static bool check_err(const char* what, paimon_error* err) {
    error_ptr owned(err);
    if (!owned) return false;
    std::fprintf(stderr, "%s failed: code=%d msg=%s\n", what, owned->code,
                 bytes_to_string(owned->message).c_str());
    return true;
}

int main() {
    std::printf("Hello from cpp_demo (paimon-c)\n");

    // -----------------------------------------------------------------
    // 1. Create the catalog. Paimon "factory" picks the catalog kind
    //    from the warehouse URI scheme; a bare local path → FileSystem
    //    catalog, no credentials needed.
    // -----------------------------------------------------------------
    paimon_option options[] = {
#ifdef __APPLE__
        {"warehouse", "/Users/dengfangyuan/Downloads/warehouse"},
#else
        {"warehouse", "/media/ssd2/clickhouse/dengfangyuan/test_paimon/"},
#endif
    };
    const std::size_t options_len = sizeof(options) / sizeof(options[0]);

    std::printf("Creating catalog...\n");
    paimon_result_catalog_new cat_res = paimon_catalog_create(options, options_len);
    if (check_err("paimon_catalog_create", cat_res.error)) return 1;
    catalog_ptr catalog(cat_res.catalog);

    // -----------------------------------------------------------------
    // 2. Open table `test.testrust0`
    // -----------------------------------------------------------------
    paimon_result_identifier_new id_res = paimon_identifier_new("test", "testrust0");
    if (check_err("paimon_identifier_new", id_res.error)) return 1;
    identifier_ptr ident(id_res.identifier);

    std::printf("Getting table 'test.testrust0'...\n");
    paimon_result_get_table tbl_res = paimon_catalog_get_table(catalog.get(), ident.get());
    if (check_err("paimon_catalog_get_table", tbl_res.error)) return 1;
    table_ptr table(tbl_res.table);

    // -----------------------------------------------------------------
    // 3. Build the read pipeline:
    //      ReadBuilder → projection → (Scan → Plan) + new_read → arrow stream
    //    The Rust example calls .with_projection(["id", "embedding"]).
    // -----------------------------------------------------------------
    paimon_result_read_builder rb_res = paimon_table_new_read_builder(table.get());
    if (check_err("paimon_table_new_read_builder", rb_res.error)) return 1;
    read_builder_ptr rb(rb_res.read_builder);

    // Projection columns must be a NULL-terminated array of C strings.
    const char* projection[] = {"id", "embedding", nullptr};
    if (check_err("paimon_read_builder_with_projection",
                  paimon_read_builder_with_projection(rb.get(), projection))) {
        return 1;
    }

    // Scan + plan (gives us split count + the splits to feed into the reader).
    paimon_result_table_scan scan_res = paimon_read_builder_new_scan(rb.get());
    if (check_err("paimon_read_builder_new_scan", scan_res.error)) return 1;
    table_scan_ptr scan(scan_res.scan);

    paimon_result_plan plan_res = paimon_table_scan_plan(scan.get());
    scan.reset();  // done with the scan; release it early
    if (check_err("paimon_table_scan_plan", plan_res.error)) return 1;
    plan_ptr plan(plan_res.plan);

    const std::size_t num_splits = paimon_plan_num_splits(plan.get());
    std::printf("  Number of splits: %zu\n", num_splits);
    if (num_splits == 0) {
        std::printf("No data splits found — the table may be empty.\n");
        return 0;
    }

    // -----------------------------------------------------------------
    // 4. Open the arrow stream over all splits and pull batches one by one.
    //    Mirrors the Rust `while let Some(batch) = stream.next().await` loop.
    // -----------------------------------------------------------------
    paimon_result_new_read read_res = paimon_read_builder_new_read(rb.get());
    if (check_err("paimon_read_builder_new_read", read_res.error)) return 1;
    table_read_ptr read(read_res.read);

    // offset=0, length=num_splits → consume the whole plan
    paimon_result_record_batch_reader rdr_res =
        paimon_table_read_to_arrow(read.get(), plan.get(), /*offset=*/0, /*length=*/num_splits);
    if (check_err("paimon_table_read_to_arrow", rdr_res.error)) return 1;
    record_batch_reader_ptr reader(rdr_res.reader);

    std::printf("\nReading table data...\n");
    std::size_t batch_idx = 0;
    int64_t total_rows = 0;
    for (;;) {
        paimon_result_next_batch nb = paimon_record_batch_reader_next(reader.get());
        if (check_err("paimon_record_batch_reader_next", nb.error)) break;

        // End-of-stream: both pointers are null.
        if (nb.batch.array == nullptr && nb.batch.schema == nullptr) {
            break;
        }

        // RAII: the batch's Arrow release callbacks + container free run when
        // `batch` leaves this scope, including on any early break/continue.
        ArrowBatch batch(nb.batch);
        ArrowSchema* schema = batch.schema();
        ArrowArray* array = batch.array();

        std::printf("RecordBatch #%zu: rows=%lld, columns=%lld, root_format=%s\n",
                    batch_idx,
                    static_cast<long long>(array->length),
                    static_cast<long long>(schema->n_children),
                    arrow_format_str(schema));

        // Print each column's name + type for the first batch (cheap diagnostic).
        if (batch_idx == 0) {
            for (int64_t c = 0; c < schema->n_children; ++c) {
                const ArrowSchema* child = schema->children[c];
                std::printf("    col[%lld] name=%s type=%s\n",
                            static_cast<long long>(c),
                            child && child->name ? child->name : "(null)",
                            arrow_format_str(child));
            }
        }

        total_rows += array->length;
        ++batch_idx;
    }

    std::printf("\n=== Read Summary ===\n");
    std::printf("Total batches: %zu\n", batch_idx);
    std::printf("Total rows:    %lld\n", static_cast<long long>(total_rows));

    // -----------------------------------------------------------------
    // 5. No manual teardown: every handle above is held in a RAII smart
    //    pointer and released here in reverse construction order
    //    (reader → read → plan → rb → table → ident → catalog) as the
    //    locals go out of scope.
    // -----------------------------------------------------------------
    return 0;
}
