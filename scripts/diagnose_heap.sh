#!/usr/bin/env bash
# Diagnose jemalloc heap dumps produced by examples built with
# --features jemalloc-profiling. Picks the largest .heap as the peak,
# extracts hot symbols via jeprof, and resolves them back to paimon
# source locations via addr2line / nm.
#
# Run from the repo root, after a profiled run that left jeprof.*.heap
# in the current directory.
#
# Usage:
#   ./scripts/diagnose_heap.sh [<binary_path>] [<heap_glob>]
# Examples:
#   ./scripts/diagnose_heap.sh
#   ./scripts/diagnose_heap.sh ./target/release/examples/read_local_demo 'jeprof.*.heap'

set -uo pipefail

BIN="${1:-./target/release/examples/read_local_demo}"
GLOB="${2:-jeprof.*.heap}"
OUT_DIR="heap_diag"

if [[ ! -x "$BIN" ]]; then
  echo "error: binary not executable: $BIN" >&2
  echo "build it with: cargo build -p paimon --release --features jemalloc-profiling,storage-hdfs --example read_local_demo" >&2
  exit 1
fi

# Resolve jeprof: prefer system, fallback to ~/bin/jeprof if user copied one in
JEPROF="$(command -v jeprof 2>/dev/null || true)"
[[ -z "$JEPROF" && -x "$HOME/bin/jeprof" ]] && JEPROF="$HOME/bin/jeprof"
if [[ -z "$JEPROF" ]]; then
  echo "error: jeprof not found in PATH or ~/bin/jeprof" >&2
  echo "install: yum install -y jemalloc-devel  OR  curl jeprof.in from jemalloc 5.3.0 to ~/bin/" >&2
  exit 1
fi
echo "jeprof = $JEPROF"

# Pick demangler
if command -v rustfilt >/dev/null 2>&1; then
  DEMANGLE=rustfilt
elif command -v c++filt >/dev/null 2>&1; then
  DEMANGLE='c++filt -p'
else
  DEMANGLE=cat
fi
echo "demangle = $DEMANGLE"

# Find peak heap (largest .heap) — avoids shell-globbing 1000+ files into jeprof
mapfile -t HEAPS < <(ls -S $GLOB 2>/dev/null || true)
if [[ ${#HEAPS[@]} -eq 0 ]]; then
  echo "error: no $GLOB files in $(pwd)" >&2
  exit 1
fi
PEAK="${HEAPS[0]}"
echo "peak    = $PEAK ($(stat -c%s "$PEAK" 2>/dev/null || stat -f%z "$PEAK") bytes; ${#HEAPS[@]} heaps total)"

mkdir -p "$OUT_DIR"

# --------------------------------------------------------------------
# 1) Peak inuse_space, full text — see which symbols hold 300+ GiB.
# --------------------------------------------------------------------
echo
echo "=== [1/5] peak inuse_space text -> $OUT_DIR/peak_text.txt ==="
"$JEPROF" --text --inuse_space "$BIN" "$PEAK" 2>&1 | tee "$OUT_DIR/peak_text.txt" | head -25

# --------------------------------------------------------------------
# 2) Filter to paimon:: frames only — your code, no trait noise.
# --------------------------------------------------------------------
echo
echo "=== [2/5] paimon:: frames in peak -> $OUT_DIR/peak_paimon.txt ==="
"$JEPROF" --text --inuse_space "$BIN" "$PEAK" 2>/dev/null \
  | grep -E 'paimon[_:]|::scan|::format|::manifest|::table|::reader|::arrow|::deletion_vector|::predicate|::file_index' \
  > "$OUT_DIR/peak_paimon.txt" || true
head -40 "$OUT_DIR/peak_paimon.txt"
echo "(saved $(wc -l < "$OUT_DIR/peak_paimon.txt") lines)"

# --------------------------------------------------------------------
# 3) Tree view focused on the top trait leaves we already know are hot.
# --------------------------------------------------------------------
echo
echo "=== [3/5] tree view (parents of try_fold/from_iter/poll_next) -> $OUT_DIR/peak_tree.txt ==="
"$JEPROF" --tree --inuse_space "$BIN" "$PEAK" 2>/dev/null > "$OUT_DIR/peak_tree.txt" || true
grep -B6 -E 'try_fold|from_iter|poll_next|clone' "$OUT_DIR/peak_tree.txt" 2>/dev/null \
  | head -120 || true

# --------------------------------------------------------------------
# 4) Resolve hash-suffixed hot symbols to source via addr2line.
# --------------------------------------------------------------------
echo
echo "=== [4/5] symbol -> source via addr2line -> $OUT_DIR/peak_src.txt ==="
{
  # Pick top hottest leaves from the text view (cumulative MB > 1.0),
  # demangle them, addr2line each.
  "$JEPROF" --text --inuse_space "$BIN" "$PEAK" 2>/dev/null \
    | awk 'NR>1 && $5+0 > 1.0 {print $7}' \
    | head -25 \
    | while read -r sym; do
        [[ -z "$sym" ]] && continue
        addr=$(nm "$BIN" 2>/dev/null | awk -v s="$sym" '$3==s {print "0x"$1; exit}' || true)
        if [[ -z "$addr" ]]; then
          echo "[no nm match] $sym"
          continue
        fi
        echo "--- $sym  (@$addr) ---"
        addr2line -e "$BIN" -f -i -C -p "$addr" 2>/dev/null || echo "(addr2line failed)"
        echo
      done
} > "$OUT_DIR/peak_src.txt" 2>&1 || true
head -80 "$OUT_DIR/peak_src.txt"
echo "(saved $(wc -l < "$OUT_DIR/peak_src.txt") lines)"

# --------------------------------------------------------------------
# 5) Render a single SVG for the peak (small enough that jeprof finishes).
#    Skip if dot is missing.
# --------------------------------------------------------------------
echo
echo "=== [5/5] render peak.svg ==="
if command -v dot >/dev/null 2>&1; then
  if "$JEPROF" --svg --inuse_space "$BIN" "$PEAK" > "$OUT_DIR/peak.svg" 2> "$OUT_DIR/peak.err"; then
    echo "wrote $OUT_DIR/peak.svg ($(wc -c < "$OUT_DIR/peak.svg") bytes)"
  else
    echo "jeprof --svg failed; see $OUT_DIR/peak.err"
    head -10 "$OUT_DIR/peak.err"
  fi
else
  echo "dot (graphviz) missing; skipping SVG. yum install -y graphviz to enable."
fi

echo
echo "done. open $OUT_DIR/{peak_text.txt,peak_paimon.txt,peak_tree.txt,peak_src.txt,peak.svg}"
