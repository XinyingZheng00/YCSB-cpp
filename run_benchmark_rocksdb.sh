#!/usr/bin/env bash
# run_benchmark_rocksdb.sh — full RocksDB/YCSB-cpp pipeline
#
#   1. Build ycsb binary with RocksDB backend (skips if already up to date).
#   2. Load dataset once (single-threaded).
#   3. Rename loaded DB directory to cached_* so every run starts from the same state.
#   4. For each workload × thread count × run repetition:
#        - Copy cached_* directory back to working DB path.
#        - Run time-bounded benchmark and append to the result file.
#        - Delete working DB directory (cached_* is preserved).
#   5. Write each (workload, thread) result to:
#        /users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local/
#        1KB-dur${MAX_EXECUTION_TIME}s-1000000-workload<X>-rocksdb_t<N>.result
#   6. (Optional) Collect a perf flamegraph for the 8-thread workload-a run.
#
# Usage:
#   ./run_benchmark_rocksdb.sh [options]
#
# Options:
#   --workloads "a b c d f"   Workload letters to run (default: "a b c d f")
#   --threads   "1 2 4 8"     Thread counts to test (default: "1 2 4 8")
#   --runs      N             Number of repetitions per (workload, thread) pair (default: 3)
#   --per-op-db               Enable per-operation DB open/close mode (rocksdb.per_op_db=true)
#   --flamegraph              Collect a perf flamegraph for the workload-a 8-thread run
set -euo pipefail

YCSB_DIR="$(cd "$(dirname "$0")" && pwd)"
DB_PATH="${DB_PATH:-/tmp/rocksdb_ycsb}"
CACHED_DB="$(dirname "$DB_PATH")/cached_$(basename "$DB_PATH")"
RESULTS_DIR="/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local"
PROPS="$YCSB_DIR/rocksdb/rocksdb.properties"
YCSB_BIN="$YCSB_DIR/ycsb"

RECORD_COUNT=1000000
MAX_EXECUTION_TIME=120
WORKLOADS="a b c d f"
THREAD_COUNTS="1 2 4 8"
RUNS=1
DO_FLAMEGRAPH=0
FLAMEGRAPH_THREADS=8
PER_OP_DB=0

# ---- Parse arguments --------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --flamegraph) DO_FLAMEGRAPH=1; shift ;;
    --per-op-db)  PER_OP_DB=1; shift ;;
    --threads)    THREAD_COUNTS="$2"; shift 2 ;;
    --workloads)  WORKLOADS="$2"; shift 2 ;;
    --runs)       RUNS="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Suffix appended to result filenames when running in per-op-db mode so the
# two modes' results don't overwrite each other.
if [ "$PER_OP_DB" -eq 1 ]; then
  MODE_SUFFIX="-per_op"
  PER_OP_ARGS="-p rocksdb.per_op_db=true"
else
  MODE_SUFFIX=""
  PER_OP_ARGS=""
fi

# Map a workload letter to its workload file.
workload_file() {
  local wl="$1"
  case "$wl" in
    a) echo "$YCSB_DIR/workloads/workloada" ;;
    *) echo "$YCSB_DIR/workloads/workload$wl" ;;
  esac
}

mkdir -p "$RESULTS_DIR"

# ---- Step 1: Build YCSB-cpp binary if needed --------------------------------
if [ ! -f "$YCSB_BIN" ] || \
   ! nm "$YCSB_BIN" 2>/dev/null | grep -q "NewRocksdbDB" || \
   [ "$YCSB_DIR/rocksdb/rocksdb_db.cc" -nt "$YCSB_BIN" ] || \
   [ "$YCSB_DIR/rocksdb/rocksdb_db.h"  -nt "$YCSB_BIN" ]; then
  echo "=== Building YCSB-cpp with RocksDB backend ==="
  cd "$YCSB_DIR"
  rm -f "$YCSB_BIN"
  make BIND_ROCKSDB=1 -j"$(nproc)"
else
  echo "=== YCSB-cpp binary is up to date ==="
fi
cd "$YCSB_DIR"

# ---- Step 2: Load dataset ---------------------------------------------------
echo ""
echo "=== Loading $RECORD_COUNT records into $DB_PATH ==="
rm -rf "$DB_PATH" "$CACHED_DB"

"$YCSB_BIN" \
  -db rocksdb \
  -load \
  -P "$(workload_file a)" \
  -P "$PROPS" \
  -p rocksdb.dbname="$DB_PATH" \
  -p rocksdb.destroy=true \
  -p threadcount=1 \
  -p recordcount="$RECORD_COUNT" \
  -p operationcount="$RECORD_COUNT"

echo "--- LOAD COMPLETE ---"

# ---- Step 3: Cache the loaded database --------------------------------------
# Rename the loaded directory so every benchmark run can start from an
# identical initial state by copying it back.
echo "=== Caching loaded database as $CACHED_DB ==="
mv "$DB_PATH" "$CACHED_DB"

# ---- Step 4: Run benchmarks -------------------------------------------------
for WL in $WORKLOADS; do
  WFILE="$(workload_file "$WL")"
  WSHORT="workload${WL}"

  for T in $THREAD_COUNTS; do
    RESULT_FILE="$RESULTS_DIR/1KB-dur${MAX_EXECUTION_TIME}s-${RECORD_COUNT}-${WSHORT}-rocksdb${MODE_SUFFIX}_t${T}.result"

    rm -f "$RESULT_FILE"
    echo "=== Workload $WL | $T thread(s) | $RUNS run(s) — output: $RESULT_FILE ===" | tee "$RESULT_FILE"

    for RUN in $(seq 1 "$RUNS"); do
      echo "" | tee -a "$RESULT_FILE"
      echo "--- Run $RUN / $RUNS ---" | tee -a "$RESULT_FILE"

      # Restore fresh database state from cache.
      # Use CoW reflinks where supported (btrfs/xfs) for a fast copy;
      # fall back to a regular recursive copy on other filesystems.
      rm -rf "$DB_PATH"
      cp -r --reflink=auto "$CACHED_DB" "$DB_PATH" 2>/dev/null || cp -r "$CACHED_DB" "$DB_PATH"

      if [ "$DO_FLAMEGRAPH" -eq 1 ] && [ "$WL" = "a" ] && [ "$T" -eq "$FLAMEGRAPH_THREADS" ] && [ "$RUN" -eq 1 ]; then
        PERF_DATA="$YCSB_DIR/perf_rocksdb_t${T}.data"
        echo "  (perf record enabled — output: $PERF_DATA)"
        sudo perf record -g -F 99 -o "$PERF_DATA" -- \
          "$YCSB_BIN" \
            -db rocksdb \
            -run \
            -P "$WFILE" \
            -P "$PROPS" \
            -p rocksdb.dbname="$DB_PATH" \
            -p threadcount="$T" \
            -p recordcount="$RECORD_COUNT" \
            -p maxexecutiontime="$MAX_EXECUTION_TIME" \
            $PER_OP_ARGS \
          2>&1 | tee -a "$RESULT_FILE"
        echo "  Generating flamegraph..."
        FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-$HOME/FlameGraph}"
        if [ -d "$FLAMEGRAPH_DIR" ]; then
          sudo perf script -i "$PERF_DATA" \
            | "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" \
            | "$FLAMEGRAPH_DIR/flamegraph.pl" \
            > "$YCSB_DIR/flamegraph_rocksdb_t${T}.svg"
          echo "  Flamegraph: $YCSB_DIR/flamegraph_rocksdb_t${T}.svg"
        else
          echo "  FlameGraph tools not found at $FLAMEGRAPH_DIR — skipping SVG generation."
          echo "  Install: git clone https://github.com/brendangregg/FlameGraph $FLAMEGRAPH_DIR"
        fi
      else
        "$YCSB_BIN" \
          -db rocksdb \
          -run \
          -P "$WFILE" \
          -P "$PROPS" \
          -p rocksdb.dbname="$DB_PATH" \
          -p threadcount="$T" \
          -p recordcount="$RECORD_COUNT" \
          -p maxexecutiontime="$MAX_EXECUTION_TIME" \
          $PER_OP_ARGS \
          2>&1 | tee -a "$RESULT_FILE"
      fi

      # Remove working database directory; cached_* copy is preserved.
      rm -rf "$DB_PATH"

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
grep -rE "Run throughput" "$RESULTS_DIR"/1KB-dur${MAX_EXECUTION_TIME}s-${RECORD_COUNT}-*-rocksdb${MODE_SUFFIX}_t*.result 2>/dev/null || true
