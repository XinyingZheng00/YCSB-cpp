#!/usr/bin/env bash
# run_benchmark.sh — full HCTree/YCSB-cpp pipeline
#
#   1. Build HCTree static library (skips if already built).
#   2. Build ycsb binary with HCTree backend (skips if already built).
#   3. Load dataset once (single-threaded, large-batch inserts).
#   4. Rename loaded DB files to cached_* so every run starts from the same state.
#   5. For each workload × thread count × run repetition:
#        - Copy cached_* files back to working DB path.
#        - Run time-bounded benchmark and append to the result file.
#        - Delete working DB files (cached_* are preserved).
#   6. Write each (workload, thread) result to:
#        /users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local/
#        1KB-dur${MAX_EXECUTION_TIME}s-1000000-workload<X>-hctree_t<N>.result
#   7. (Optional) Collect a perf flamegraph for the 8-thread workload-a run.
#
# Usage:
#   ./run_benchmark.sh [options]
#
# Options:
#   --workloads "a b c d f"   Workload letters to run (default: "a b c d f")
#   --threads   "1 2 4 8 16"  Thread counts to test (default: "1 2 4 8")
#   --runs      N             Number of repetitions per (workload, thread) pair (default: 3)
#   --flamegraph              Collect a perf flamegraph for the workload-a 8-thread run
set -euo pipefail

YCSB_DIR="$(cd "$(dirname "$0")" && pwd)"
HCTREE_BLD_DIR="${HCTREE_BLD_DIR:-$HOME/hctree/bld}"
DB_PATH="${DB_PATH:-/tmp/hctree_ycsb.db}"
CACHED_DB="$(dirname "$DB_PATH")/cached_$(basename "$DB_PATH")"
RESULTS_DIR="/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local"
PROPS="$YCSB_DIR/sqlite/hctree.properties"
YCSB_BIN="$YCSB_DIR/ycsb"

RECORD_COUNT=1000000
MAX_EXECUTION_TIME=120
WORKLOADS="a b c d f"
THREAD_COUNTS="1 2 4 8"
RUNS=2
DO_FLAMEGRAPH=0
FLAMEGRAPH_THREADS=8

# ---- Parse arguments --------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --flamegraph) DO_FLAMEGRAPH=1; shift ;;
    --threads)    THREAD_COUNTS="$2"; shift 2 ;;
    --workloads)  WORKLOADS="$2"; shift 2 ;;
    --runs)       RUNS="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Map a workload letter to its workload file.
# Workload a uses workloada_hctree (1 KB fields, 1 M records).
# All others use the standard workload files with recordcount overridden at runtime.
workload_file() {
  local wl="$1"
  case "$wl" in
    a) echo "$YCSB_DIR/workloads/workloada_hctree" ;;
    *) echo "$YCSB_DIR/workloads/workload$wl" ;;
  esac
}

mkdir -p "$RESULTS_DIR"

# ---- Step 1: Build HCTree library if needed ---------------------------------
if [ ! -f "$HCTREE_BLD_DIR/libsqlite3_hctree.a" ]; then
  echo "=== Building HCTree library ==="
  bash "$YCSB_DIR/build_hctree.sh"
else
  echo "=== HCTree library already built at $HCTREE_BLD_DIR/libsqlite3_hctree.a ==="
fi

# ---- Step 2: Build YCSB-cpp binary if needed --------------------------------
if [ ! -f "$YCSB_BIN" ] || \
   ! nm "$YCSB_BIN" 2>/dev/null | grep -q "NewSqliteDB" || \
   [ "$YCSB_DIR/sqlite/sqlite_db.cc" -nt "$YCSB_BIN" ] || \
   [ "$YCSB_DIR/sqlite/sqlite_db.h"  -nt "$YCSB_BIN" ]; then
  echo "=== Building YCSB-cpp with HCTree backend ==="
  cd "$YCSB_DIR"
  rm -f "$YCSB_BIN"
  make BIND_SQLITE=1 BIND_HCTREE=1 HCTREE_BLD_DIR="$HCTREE_BLD_DIR" -j"$(nproc)"
else
  echo "=== YCSB-cpp binary is up to date ==="
fi
cd "$YCSB_DIR"

# ---- Step 3: Load dataset ---------------------------------------------------
echo ""
echo "=== Loading $RECORD_COUNT records into $DB_PATH ==="
rm -f "$DB_PATH" "${DB_PATH}-wal" "${DB_PATH}-shm" "${DB_PATH}-pagemap" "${DB_PATH}"-log-* \
      "$CACHED_DB" "${CACHED_DB}-wal" "${CACHED_DB}-pagemap" "${CACHED_DB}"-log-*

"$YCSB_BIN" \
  -db sqlite \
  -load \
  -P "$(workload_file a)" \
  -P "$PROPS" \
  -p sqlite.dbpath="$DB_PATH" \
  -p threadcount=1 \
  -p recordcount="$RECORD_COUNT" \
  -p operationcount="$RECORD_COUNT"

echo "--- LOAD COMPLETE ---"

# ---- Step 4: Cache the loaded database --------------------------------------
# Rename the loaded files so every benchmark run can start from an identical
# initial state by copying them back.  SHM is not cached (SQLite recreates it).
echo "=== Caching loaded database as $CACHED_DB ==="
mv "$DB_PATH" "$CACHED_DB"
[ -f "${DB_PATH}-wal" ]     && mv "${DB_PATH}-wal"     "${CACHED_DB}-wal"     || true
[ -f "${DB_PATH}-pagemap" ] && mv "${DB_PATH}-pagemap" "${CACHED_DB}-pagemap" || true
for f in "${DB_PATH}"-log-*; do [ -f "$f" ] && mv "$f" "${CACHED_DB}-${f##*"${DB_PATH}-"}" || true; done
rm -f "${DB_PATH}-shm"

# ---- Step 5: Run benchmarks -------------------------------------------------
for WL in $WORKLOADS; do
  WFILE="$(workload_file "$WL")"
  WSHORT="workload${WL}"

  for T in $THREAD_COUNTS; do
    RESULT_FILE="$RESULTS_DIR/1KB-dur${MAX_EXECUTION_TIME}s-${RECORD_COUNT}-${WSHORT}-hctree_t${T}.result"

    rm -f "$RESULT_FILE"
    echo "=== Workload $WL | $T thread(s) | $RUNS run(s) — output: $RESULT_FILE ===" | tee "$RESULT_FILE"

    for RUN in $(seq 1 "$RUNS"); do
      echo "" | tee -a "$RESULT_FILE"
      echo "--- Run $RUN / $RUNS ---" | tee -a "$RESULT_FILE"

      # Restore fresh database state from cache.
      rm -f "$DB_PATH" "${DB_PATH}-wal" "${DB_PATH}-shm" "${DB_PATH}-pagemap" "${DB_PATH}"-log-*
      cp "$CACHED_DB" "$DB_PATH"
      [ -f "${CACHED_DB}-wal" ]     && cp "${CACHED_DB}-wal"     "${DB_PATH}-wal"     || true
      [ -f "${CACHED_DB}-pagemap" ] && cp "${CACHED_DB}-pagemap" "${DB_PATH}-pagemap" || true
      for f in "${CACHED_DB}"-log-*; do [ -f "$f" ] && cp "$f" "${DB_PATH}-${f##*"${CACHED_DB}-"}" || true; done

      if [ "$DO_FLAMEGRAPH" -eq 1 ] && [ "$WL" = "a" ] && [ "$T" -eq "$FLAMEGRAPH_THREADS" ] && [ "$RUN" -eq 1 ]; then
        PERF_DATA="$YCSB_DIR/perf_t${T}.data"
        echo "  (perf record enabled — output: $PERF_DATA)"
        sudo perf record -g -F 99 -o "$PERF_DATA" -- \
          "$YCSB_BIN" \
            -db sqlite \
            -run \
            -P "$WFILE" \
            -P "$PROPS" \
            -p sqlite.dbpath="$DB_PATH" \
            -p threadcount="$T" \
            -p recordcount="$RECORD_COUNT" \
            -p maxexecutiontime="$MAX_EXECUTION_TIME" \
          2>&1 | tee -a "$RESULT_FILE"
        echo "  Generating flamegraph..."
        FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-$HOME/FlameGraph}"
        if [ -d "$FLAMEGRAPH_DIR" ]; then
          sudo perf script -i "$PERF_DATA" \
            | "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" \
            | "$FLAMEGRAPH_DIR/flamegraph.pl" \
            > "$YCSB_DIR/flamegraph_t${T}.svg"
          echo "  Flamegraph: $YCSB_DIR/flamegraph_t${T}.svg"
        else
          echo "  FlameGraph tools not found at $FLAMEGRAPH_DIR — skipping SVG generation."
          echo "  Install: git clone https://github.com/brendangregg/FlameGraph $FLAMEGRAPH_DIR"
        fi
      else
        "$YCSB_BIN" \
          -db sqlite \
          -run \
          -P "$WFILE" \
          -P "$PROPS" \
          -p sqlite.dbpath="$DB_PATH" \
          -p threadcount="$T" \
          -p recordcount="$RECORD_COUNT" \
          -p maxexecutiontime="$MAX_EXECUTION_TIME" \
          2>&1 | tee -a "$RESULT_FILE"
      fi

      # Remove working database files; cached_* copies are preserved.
      rm -f "$DB_PATH" "${DB_PATH}-wal" "${DB_PATH}-shm" "${DB_PATH}-pagemap" "${DB_PATH}"-log-*

      if [ "$RUN" -lt "$RUNS" ]; then
        sleep 2
      fi
    done

    echo "--- T=$T COMPLETE ---" | tee -a "$RESULT_FILE"
    echo ""

    sleep 2
  done
done

echo ""
echo "=== All benchmarks complete. Results written to: $RESULTS_DIR ==="
echo ""
echo "Quick summary (throughput lines):"
grep -rE "Run throughput" "$RESULTS_DIR"/1KB-dur${MAX_EXECUTION_TIME}s-${RECORD_COUNT}-*-hctree_t*.result 2>/dev/null || true
