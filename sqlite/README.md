sudo apt-get update && sudo apt-get install -y libsqlite3-dev zlib1g-dev sqlite3 tcl tcl-dev fossil
make BIND_SQLITE=1
//./ycsb -load -db sqlite -P workloads/workloada -P sqlite/sqlite.properties -s -p recordcount=1000
// the database file is /tmp/ycsb-sqlite.db

sudo apt install fossil
// prepare sqlite-concurrent
fossil clone https://www.sqlite.org/src sqlite.fossil
mkdir sqlite-concurrent/ && cd sqlite-concurrent/
fossil open ../sqlite.fossil
fossil update begin-concurrent
mkdir build install
cd build && ../configure --prefix=/users/Xinying/sqlite-concurrent/install && make -j$(nproc) && sudo make install -j$(nproc)
// fossil branch list --all
// make distclean 2>/dev/null || true && rm -f config.cache 

// prepare hctree
fossil clone https://www.sqlite.org/hctree hctree.fossil
mkdir hctree && cd hctree
fossil open ../hctree.fossil
fossil up hctree
rm -rf build install && mkdir build install \
&& cd build && ../configure CFLAGS="-DSQLITE_SHARED_MAPPING=1 \
                    -DSQLITE_DEFAULT_MEMSTATUS=0 \
                    -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS=1" --prefix=/users/Xinying/hctree/install \
&& make -j$(nproc) \
&& sudo make install -j$(nproc)

// prepare fileio
make BIND_FILEIO=1 

// sync updates regularly
rsync -avz --delete Xinying@clnode281.clemson.cloudlab.us:/users/Xinying/YCSB-cpp/ YCSB-cpp

extended_error_code = 517 means:
SQLITE_BUSY_SNAPSHOT
This is not a normal lock wait. It’s an optimistic-concurrency conflict.