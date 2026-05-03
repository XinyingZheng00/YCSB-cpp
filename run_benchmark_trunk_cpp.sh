#!/usr/bin/env bash
# run_benchmark_trunk_cpp.sh — Traditional SQLite (cpp binding, plain WAL + BEGIN) benchmark pipeline
#
#   Identical structure to run_benchmark_hctree.sh but uses the BCW2 configuration:
#     - sqlite.hctree_mode=false  (no ?hctree=1, standard SQLite B-tree)
#     - journal_mode=WAL2
#     - journal_size_limit=16 MB
#   Result files are written alongside the HCTree results under the same
#   fig2-multi-writer-local directory, tagged with "bcw2" instead of "hctree".
#
# Usage:
#   ./run_benchmark_trunk_cpp.sh [options]
#
# Options:
#   --workloads "a b c d f"   Workload letters to run (default: "a")
#   --threads   "1 2 4 8 16"  Thread counts to test (default: "1 2 4 8 16")
#   --runs      N             Number of repetitions per (workload, thread) pair (default: 1)
set -euo pipefail

YCSB_DIR="$(cd "$(dirname "$0")" && pwd)"
# BCW2 mode requires a SQLite library built from the begin-concurrent-pnu-wal2
# branch, NOT the hctree branch. The hctree branch lacks the WAL2/BEGIN
# CONCURRENT patches, which is why earlier bcw2 runs silently fell back to
# journal_mode=delete. Build via: bash $YCSB_DIR/build_bcw2.sh
BCW2_BLD_DIR="${BCW2_BLD_DIR:-$HOME/bcw2/bld}"
DB_PATH="${DB_PATH:-/tank/ycsb_data/trunkcpp_ycsb/trunkcpp_ycsb.db}"
mkdir -p "$(dirname "$DB_PATH")"
CACHED_DB="$(dirname "$DB_PATH")/cached_$(basename "$DB_PATH")"
RESULTS_DIR="/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local"
PROPS="$YCSB_DIR/sqlite/sqlite-trunk.properties"
YCSB_BIN="$YCSB_DIR/ycsb"

RECORD_COUNT=1000000
MAX_EXECUTION_TIME=60
WORKLOADS="a b c d f"
THREAD_COUNTS="1"
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

# ---- Step 1: Build SQLite library if needed ---------------------------------
# hctree-bedrock branch: contains hctree storage engine + WAL2 + BEGIN
# CONCURRENT in a single library. Used for BOTH bcw2 and hctree benchmarks.
if [ ! -f "$BCW2_BLD_DIR/libsqlite3_hctree.a" ]; then
  echo "=== Building SQLite library (hctree-bedrock) ==="
  bash "$YCSB_DIR/build_bcw2.sh"
else
  echo "=== SQLite library already built at $BCW2_BLD_DIR/libsqlite3_hctree.a ==="
fi

# ---- Step 2: Build YCSB-cpp binary if needed --------------------------------
# Single binary shared by run_benchmark_trunk_cpp.sh and run_benchmark_hctree.sh.
# Mode is selected at runtime by the properties file (sqlite-trunk.properties
# vs hctree.properties), not by which binary is invoked.
if [ ! -f "$YCSB_BIN" ] || \
   ! nm "$YCSB_BIN" 2>/dev/null | grep -q "NewSqliteDB" || \
   [ "$YCSB_DIR/sqlite/sqlite_db.cc" -nt "$YCSB_BIN" ] || \
   [ "$YCSB_DIR/sqlite/sqlite_db.h"  -nt "$YCSB_BIN" ] || \
   [ "$BCW2_BLD_DIR/libsqlite3_hctree.a" -nt "$YCSB_BIN" ]; then
  echo "=== Building YCSB-cpp ==="
  cd "$YCSB_DIR"
  rm -f "$YCSB_BIN"
  make clean >/dev/null 2>&1 || true
  make BIND_SQLITE=1 BIND_HCTREE=1 HCTREE_BLD_DIR="$BCW2_BLD_DIR" -j"$(nproc)"
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
    RESULT_FILE="$RESULTS_DIR/1KB-dur${MAX_EXECUTION_TIME}s-${RECORD_COUNT}-${WSHORT}-trunkcpp_t${T}.result"

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
echo "=== All trunk-cpp benchmarks complete. Results written to: $RESULTS_DIR ==="
echo ""
echo "Quick summary (throughput lines):"
grep -rE "Run throughput" "$RESULTS_DIR"/1KB-dur${MAX_EXECUTION_TIME}s-${RECORD_COUNT}-*-trunkcpp_t*.result 2>/dev/null || true
