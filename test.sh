# Configuration
RECORD_COUNT=10000000
# accept command line argument for test type
TEST_TYPE=${1:-multithread} # multiprocess or multithread

# 1,2,4,8,16
# run the test repeatedly for 10 times
for j in {1}; do
    echo "Running test $j"
    rm -rf multiple-${TEST_TYPE}-result-${j}/*
    # the number of partition should be a multiple of the number of writers
    # for i in {1,2,4,8,16,32,48,64,72,80,88,96}; do
    for i in {1,2,4,8,16,32,48,64,72,80,88,96}; do
        NUMBER_OF_WRITERS=$i
        for DB_NAME in hctree; do # sqlite_concurrent hctree fileio
            for PAGE_SIZE in 1024; do
                echo "Testing $DB_NAME with $NUMBER_OF_WRITERS writers and page size $PAGE_SIZE and test type $TEST_TYPE"

                if [ "$DB_NAME" == "sqlite" ]; then
                    unset LD_LIBRARY_PATH
                    unset LD_RUN_PATH
                    export PATH=/usr/bin:$PATH
                    which sqlite3

                elif [ "$DB_NAME" == "sqlite_concurrent" ]; then
                    export PATH=$HOME/sqlite-concurrent/install/bin:$PATH
                    export LD_LIBRARY_PATH=$HOME/sqlite-concurrent/install/lib
                    export LD_RUN_PATH=$HOME/sqlite-concurrent/install/lib
                    which sqlite3

                elif [ "$DB_NAME" == "hctree" ]; then
                    export PATH=$HOME/hctree/install/bin:$PATH
                    export LD_LIBRARY_PATH=$HOME/hctree/install/lib
                    export LD_RUN_PATH=$HOME/hctree/install/lib
                    which sqlite3
                fi
                
                mkdir -p multiple-${TEST_TYPE}-result-${j}/
                if [ "$TEST_TYPE" == "multiprocess" ]; then
                    RECORDS_PER_PROCESS=$((RECORD_COUNT / NUMBER_OF_WRITERS))
                    ./test_multi_ycsb.sh $NUMBER_OF_WRITERS $DB_NAME $PAGE_SIZE $TEST_TYPE $RECORDS_PER_PROCESS > multiple-${TEST_TYPE}-result-${j}/$DB_NAME-$PAGE_SIZE-$NUMBER_OF_WRITERS-$RECORD_COUNT.txt
                else
                    ./test_multi_ycsb.sh $NUMBER_OF_WRITERS $DB_NAME $PAGE_SIZE $TEST_TYPE $RECORD_COUNT > multiple-${TEST_TYPE}-result-${j}/$DB_NAME-$PAGE_SIZE-$NUMBER_OF_WRITERS-$RECORD_COUNT.txt
                fi
            done
        done
    done
done