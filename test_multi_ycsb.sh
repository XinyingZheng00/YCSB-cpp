#!/bin/bash
#
# test_multiprocess_ycsb.sh
# Run multiple YCSB processes concurrently to test SQLite multi-process writer support
#

set -e

# ./test_multi_ycsb.sh $NUMBER_OF_WRITERS $DB_NAME $PAGE_SIZE $TEST_TYPE $(RECORD_COUNT/$NUMBER_OF_WRITERS) > multiple-${TEST_TYPE}-result/$DB_NAME-$PAGE_SIZE-$NUMBER_OF_WRITERS-$RECORD_COUNT.txt
            
NUM_WRITERS=${1:-4}
DB_NAME=${2:-sqlite}
DB_PAGE_SIZE=${3:-4096}
TEST_TYPE=${4:-multithread}
RECORD_COUNT=${5:-1000000}


PROPERTIES_FILE="sqlite/sqlite.properties"
DB_PATH="/tmp/test_ycsb_multi.db"

mkdir -p /tmp/ycsb_test_props

echo "=========================================="
echo "${TEST_TYPE} YCSB ${DB_NAME} Test"
echo "=========================================="
echo "Database: $DB_PATH"
echo "Number of processes: $NUM_WRITERS"
if [ "$TEST_TYPE" == "multithread" ]; then
    echo "Total records: $RECORD_COUNT"
elif [ "$TEST_TYPE" == "multiprocess" ]; then
    echo "Total records: $((NUM_WRITERS * RECORD_COUNT))"
fi
echo "=========================================="
echo ""

for i in $(seq 0 $((NUM_WRITERS - 1))); do
    PROP_FILE="/tmp/ycsb_test_props/props_$i.properties"
    cp "$PROPERTIES_FILE" "$PROP_FILE"
    echo "" >> "$PROP_FILE"
    if [ "$DB_NAME" == "sqlite" ]; then
        echo "sqlite.journal_mode=WAL" >> "$PROP_FILE"
        echo "sqlite.dbpath=$DB_PATH" >> "$PROP_FILE"
        echo "sqlite.busy_timeout=5000" >> "$PROP_FILE"
        echo "sqlite.transaction_batch_size=1000" >> "$PROP_FILE"
        echo "sqlite.page_size=$DB_PAGE_SIZE" >> "$PROP_FILE"
    elif [ "$DB_NAME" == "sqlite_concurrent" ]; then
        echo "sqlite.journal_mode=WAL" >> "$PROP_FILE"
        echo "sqlite.dbpath=$DB_PATH" >> "$PROP_FILE"
        echo "sqlite.busy_timeout=5000" >> "$PROP_FILE"
        echo "sqlite.begin_concurrent_transactions=true" >> "$PROP_FILE"
        echo "sqlite.transaction_batch_size=1000" >> "$PROP_FILE"
        echo "sqlite.page_size=$DB_PAGE_SIZE" >> "$PROP_FILE"
    elif [ "$DB_NAME" == "hctree" ]; then
        # echo "sqlite.journal_mode=WAL" >> "$PROP_FILE" //Setting journal_mode to WAL is not supported by hctree
        echo "sqlite.dbpath=$DB_PATH" >> "$PROP_FILE"
        # echo "sqlite.busy_timeout=5000" >> "$PROP_FILE"
        echo "sqlite.page_size=$DB_PAGE_SIZE" >> "$PROP_FILE"
        echo "sqlite.begin_concurrent_transactions=true" >> "$PROP_FILE"
        echo "sqlite.transaction_batch_size=10" >> "$PROP_FILE"
    elif [ "$DB_NAME" == "fileio" ]; then
        echo "fileio.filepath=$DB_PATH" >> "$PROP_FILE"
    fi
done

# run the load phase first, if .bak files exist, skip the load phase
echo "=========================================="
echo "Running YCSB load phase"
echo "=========================================="
if [ ! -f "$DB_PATH".bak ]; then
    rm -f "$DB_PATH"*
    ./ycsb -load -db sqlite \
        -P "workloads/workload" \
        -P "$PROP_FILE" \
        -p recordcount=$RECORD_COUNT \
        -p num_partitions=31680 \
        -p use_partition_prefix=true \
        -p threadcount=16 \
        -p status.interval=1 \
        -s 2>&1
    # Checkpoint WAL to merge it into main database before backup
    # This ensures the backup has a consistent state and preserves journal_mode
    if [ "$DB_NAME" == "sqlite" ] || [ "$DB_NAME" == "sqlite_concurrent" ] || [ "$DB_NAME" == "hctree" ]; then
        sqlite3 "$DB_PATH" "PRAGMA wal_checkpoint(FULL);" 2>/dev/null || true
    fi
    cp "$DB_PATH" "$DB_PATH".bak
    cp "$DB_PATH"-pagemap "$DB_PATH"-pagemap.bak 2>/dev/null || true
fi 

# Verify data after load phase
echo ""
echo "Verifying data integrity after load phase..."
if [ "$DB_NAME" == "sqlite" ] || [ "$DB_NAME" == "sqlite_concurrent" ] || [ "$DB_NAME" == "hctree" ]; then
    ACTUAL_LOAD=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM usertable;" 2>/dev/null || echo "0")
elif [ "$DB_NAME" == "fileio" ]; then
    ACTUAL_LOAD=$(cat "$DB_PATH" 2>/dev/null | wc -l || echo "0")
fi

if [ "$ACTUAL_LOAD" -eq "$RECORD_COUNT" ]; then
    echo "✓ Load phase data integrity verified: $ACTUAL_LOAD records (expected $RECORD_COUNT)"
else
    echo "✗ Load phase data integrity check failed: $ACTUAL_LOAD records (expected $RECORD_COUNT)"
    echo "  Missing: $((RECORD_COUNT - ACTUAL_LOAD)) records"
fi
echo ""

rm -f "$DB_PATH" "$DB_PATH"-wal "$DB_PATH"-shm "$DB_PATH"-pagemap
cp "$DB_PATH".bak "$DB_PATH"
cp "$DB_PATH"-pagemap.bak "$DB_PATH"-pagemap
echo "=========================================="
echo "Running YCSB run phase"
echo "=========================================="

PIDS=()
START_TIME=$(date +%s.%N)

# if [ "$TEST_TYPE" == "multithread" ]; then
#     ./ycsb -run -db sqlite \
#         -P "workloads/workload" \
#         -P "$PROP_FILE" \
#         -p operationcount=$RECORD_COUNT \
#         -p threadcount=$NUM_WRITERS \
#         -p use_partition_prefix=true \
#         -p status.interval=1 \
#         -s 2>&1

if [ "$TEST_TYPE" == "multithread" ]; then
  FUTEX_OUT="/tmp/futex.${DB_NAME}.${DB_PAGE_SIZE}.${NUM_WRITERS}w.${RECORD_COUNT}.sum"
  YCSB_OUT="/tmp/ycsb.${DB_NAME}.${DB_PAGE_SIZE}.${NUM_WRITERS}w.${RECORD_COUNT}.log"

  # Start YCSB in background so we can attach strace
  ./ycsb -run -db sqlite \
      -P "workloads/workload" \
      -P "$PROP_FILE" \
      -p operationcount=$RECORD_COUNT \
      -p threadcount=$NUM_WRITERS \
      -p use_partition_prefix=true \
      -p status.interval=1 \
      -s 2>&1 | tee "$YCSB_OUT" &
  YCSB_PID=$!

  echo "[INFO] Started ycsb pid=$YCSB_PID, collecting futex stats for 10s -> $FUTEX_OUT"

  # Collect futex stats for 10 seconds while YCSB runs
  sudo strace -f -tt -p "$YCSB_PID" -e trace=futex -o "$FUTEX_OUT" -- sleep 10 || true

  echo "========== FUTEX SUMMARY ($NUM_WRITERS writers) =========="
  cat "$FUTEX_OUT" || true
  echo "========================================================="

  # Wait for YCSB to finish
  wait "$YCSB_PID"

elif [ "$TEST_TYPE" == "multiprocess" ]; then
    for i in $(seq 0 $((NUM_WRITERS - 1))); do
        # Run YCSB load in background
        (
            echo "[Process $i] Starting at $(date +%H:%M:%S.%N)"
            ./ycsb -run -db sqlite \
                -P "workloads/workload" \
                -P "$PROP_FILE" \
                -p operationcount=$RECORD_COUNT \
                -p num_partitions=$NUM_WRITERS \
                -p status.interval=1 \
                -s 2>&1 | sed "s/^/[Process $i] /"
            echo "[Process $i] Completed at $(date +%H:%M:%S.%N)"
        ) &
        PIDS+=($!)
    done
    echo "Waiting for all $NUM_WRITERS processes to complete..."
    for pid in "${PIDS[@]}"; do
        wait $pid
        echo "Process $pid finished"
    done
fi

END_TIME=$(date +%s.%N)
DURATION=$(echo "$END_TIME - $START_TIME" | bc)

if [ "$TEST_TYPE" == "multiprocess" ]; then
    TOTAL_OPS=$((NUM_WRITERS * RECORD_COUNT))
else
    TOTAL_OPS=$RECORD_COUNT
fi

AGGREGATED_THROUGHPUT=$(echo "scale=2; $TOTAL_OPS / $DURATION" | bc)

echo ""
echo "=========================================="
echo "All processes completed!"
echo "Total time: ${DURATION} seconds"
echo ""
echo "Aggregated Performance:"
echo "  Total operations: $TOTAL_OPS"
echo "  Aggregated throughput: $AGGREGATED_THROUGHPUT ops/sec"
echo "=========================================="

# Verify data
echo ""
echo "Verifying data integrity..."
if [ "$TEST_TYPE" == "multiprocess" ]; then
    EXPECTED=$((NUM_WRITERS * RECORD_COUNT))
else
    EXPECTED=$RECORD_COUNT
fi

if [ "$DB_NAME" == "sqlite" ] || [ "$DB_NAME" == "sqlite_concurrent" ] || [ "$DB_NAME" == "hctree" ]; then
    ACTUAL=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM usertable;")
elif [ "$DB_NAME" == "fileio" ]; then
    ACTUAL=$(cat "$DB_PATH" | wc -l)
fi

echo ""
if [ "$ACTUAL" -eq "$EXPECTED" ]; then
    echo "✓ Data integrity verified: $ACTUAL records (expected $EXPECTED)"
else
    echo "✗ Data integrity check failed: $ACTUAL records (expected $EXPECTED)"
    echo "  Missing: $((EXPECTED - ACTUAL)) records"
fi
