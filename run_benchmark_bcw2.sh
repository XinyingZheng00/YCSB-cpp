#!/usr/bin/env bash
# run_benchmark_bcw2.sh — BCW2 (BEGIN CONCURRENT + WAL2) benchmark pipeline
#
#   Identical structure to run_benchmark_hctree.sh but uses the BCW2 configuration:
#     - sqlite.hctree_mode=false  (no ?hctree=1, standard SQLite B-tree)
#     - journal_mode=WAL2
#     - journal_size_limit=16 MB
#   Result files are written alongside the HCTree results under the same
#   fig2-multi-writer-local directory, tagged with "bcw2" instead of "hctree".
#
# Usage:
#   ./run_benchmark_bcw2.sh [options]
#
# Options:
#   --workloads "a b c d f"   Workload letters to run (default: "a")
#   --threads   "1 2 4 8 16"  Thread counts to test (default: "1 2 4 8 16")
#   --runs      N             Number of repetitions per (workload, thread) pair (default: 1)
set -euo pipefail

YCSB_DIR="$(cd "$(dirname "$0")" && pwd)"
HCTREE_BLD_DIR="${HCTREE_BLD_DIR:-$HOME/hctree/bld}"
DB_PATH="${DB_PATH:-/tmp/bcw_ycsb.db}"
CACHED_DB="$(dirname "$DB_PATH")/cached_$(basename "$DB_PATH")"
RESULTS_DIR="/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local"
PROPS="$YCSB_DIR/sqlite/sqlite-bcw.properties"
YCSB_BIN="$YCSB_DIR/ycsb"

RECORD_COUNT=1000000
MAX_EXECUTION_TIME=120
WORKLOADS="a b c d f"
THREAD_COUNTS="1 2 4 8 16"
RUNS=1

# ---- Parse arguments --------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads)   THREAD_COUNTS="$2"; shift 2 ;;
    --workloads) WORKLOADS="$2"; shift 2 ;;
    --runs)      RUNS="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Map a workload letter to its workload file.
workload_file() {
  local wl="$1"
  case "$wl" in
    a) echo "$YCSB_DIR/workloads/workloada_hctree" ;;
    *) echo "$YCSB_DIR/workloads/workload$wl" ;;
  esac
}

mkdir -p "$RESULTS_DIR"

# ---- Step 1: Build HCTree library if needed ---------------------------------
# BCW mode still links against the HCTree library (same binary); it just opens
# without ?hctree=1, so no HCTree B-tree structures are activated.
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
echo "=== Loading $RECORD_COUNT records into $DB_PATH (BCW2 mode) ==="
rm -f "$DB_PATH" "${DB_PATH}-wal" "${DB_PATH}-wal2" "${DB_PATH}-shm" \
      "$CACHED_DB" "${CACHED_DB}-wal" "${CACHED_DB}-wal2"

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
echo "=== Caching loaded database as $CACHED_DB ==="
mv "$DB_PATH" "$CACHED_DB"
[ -f "${DB_PATH}-wal"  ] && mv "${DB_PATH}-wal"  "${CACHED_DB}-wal"  || true
[ -f "${DB_PATH}-wal2" ] && mv "${DB_PATH}-wal2" "${CACHED_DB}-wal2" || true
rm -f "${DB_PATH}-shm"

# ---- Step 5: Run benchmarks -------------------------------------------------
for WL in $WORKLOADS; do
  WFILE="$(workload_file "$WL")"
  WSHORT="workload${WL}"

  for T in $THREAD_COUNTS; do
    RESULT_FILE="$RESULTS_DIR/1KB-dur${MAX_EXECUTION_TIME}s-${RECORD_COUNT}-${WSHORT}-bcw2_t${T}.result"

    rm -f "$RESULT_FILE"
    echo "=== Workload $WL | $T thread(s) | $RUNS run(s) — output: $RESULT_FILE ===" | tee "$RESULT_FILE"

    for RUN in $(seq 1 "$RUNS"); do
      echo "" | tee -a "$RESULT_FILE"
      echo "--- Run $RUN / $RUNS ---" | tee -a "$RESULT_FILE"

      # Restore fresh database state from cache.
      rm -f "$DB_PATH" "${DB_PATH}-wal" "${DB_PATH}-wal2" "${DB_PATH}-shm"
      cp "$CACHED_DB" "$DB_PATH"
      [ -f "${CACHED_DB}-wal"  ] && cp "${CACHED_DB}-wal"  "${DB_PATH}-wal"  || true
      [ -f "${CACHED_DB}-wal2" ] && cp "${CACHED_DB}-wal2" "${DB_PATH}-wal2" || true

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

      # Remove working database files; cached_* copies are preserved.
      rm -f "$DB_PATH" "${DB_PATH}-wal" "${DB_PATH}-wal2" "${DB_PATH}-shm"

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
echo "=== All BCW2 benchmarks complete. Results written to: $RESULTS_DIR ==="
echo ""
echo "Quick summary (throughput lines):"
grep -rE "Run throughput" "$RESULTS_DIR"/1KB-dur${MAX_EXECUTION_TIME}s-${RECORD_COUNT}-*-bcw2_t*.result 2>/dev/null || true
