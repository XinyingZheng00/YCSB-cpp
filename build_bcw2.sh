#!/usr/bin/env bash
# build_bcw2.sh — builds the hctree-bedrock SQLite branch as a static library
# for the BCW2 benchmark. Output is named libsqlite3_hctree.a so it can be
# picked up by the existing BIND_HCTREE Makefile branch unchanged.
#
# Output: ~/bcw2/bld/libsqlite3_hctree.a  and  ~/bcw2/bld/sqlite3.h
#
# Why hctree-bedrock: the plain hctree branch (used by build_hctree.sh) lacks
# the WAL2 and BEGIN CONCURRENT patches, so `PRAGMA journal_mode=wal2` silently
# falls back to the default rollback journal. hctree-bedrock is the merge of
# bedrock (which has wal2 + begin-concurrent) into the hctree line, so it
# carries ALL THREE features (hctree storage engine, WAL2, BEGIN CONCURRENT)
# at SQLite 3.54.0 (2026-04-22). The older begin-concurrent-pnu-wal2 branch
# is also viable but pins to SQLite 3.41.0 (Feb 2023).
#
# Initial setup (one-time):
#   cd $HOME && fossil clone https://www.sqlite.org/hctree hctree.fossil
#   mkdir -p bcw2 && cd bcw2 && fossil open ../hctree.fossil hctree-bedrock
set -euo pipefail

BCW2_DIR="${BCW2_DIR:-$HOME/bcw2}"
BUILD_DIR="$BCW2_DIR/bld"

# Same defines as build_hctree.sh — SHARED_MAPPING and the two stat-disabling
# flags are required by the cpp binding's SHARED_MAPPING-aware paths.
BCW2_DEFINES="-DSQLITE_SHARED_MAPPING=1 \
  -DSQLITE_DEFAULT_MEMSTATUS=0 \
  -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS=1 \
  -DSQLITE_THREADSAFE=1"

BCW2_CFLAGS="-fno-omit-frame-pointer -g -O2 $BCW2_DEFINES"

echo "=== [1/4] Installing build dependencies ==="
sudo apt-get install -y gcc make tcl tcl-dev >/dev/null

echo "=== [2/4] Configuring BCW2 in $BUILD_DIR ==="
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

if [ ! -f Makefile ]; then
  # hctree-bedrock uses jimsh-based configure; --enable-all enables RTREE,
  # FTS3/4/5, SESSION, GEOPOLY, MATH_FUNCTIONS, PERCENTILE, etc.
  ../configure --enable-all CFLAGS='-fno-omit-frame-pointer -g -O2'
fi

echo "=== [3/4] Generating amalgamation (sqlite3.c / sqlite3.h) ==="
if [ ! -f sqlite3.c ] || [ ! -f sqlite3.h ]; then
  make sqlite3.c sqlite3.h -j"$(nproc)"
fi

echo "=== [4/4] Compiling libsqlite3_hctree.a (BCW2 sources) ==="
# Note: archive is named libsqlite3_hctree.a to keep the YCSB-cpp Makefile's
# BIND_HCTREE branch working unchanged. The actual sources are bcw2.
# shellcheck disable=SC2086
gcc $BCW2_CFLAGS -fPIC -c sqlite3.c -o sqlite3_bcw2.o
ar rcs libsqlite3_hctree.a sqlite3_bcw2.o

echo ""
echo "Done."
echo "  Static library : $BUILD_DIR/libsqlite3_hctree.a (BCW2 sources)"
echo "  Header         : $BUILD_DIR/sqlite3.h"
echo ""
echo "Verifying WAL2 / BEGIN CONCURRENT symbols are present:"
nm "$BUILD_DIR/libsqlite3_hctree.a" 2>/dev/null | grep -ciE 'wal2|begin_concurrent' \
  | xargs -I{} echo "  matched symbols: {}"
