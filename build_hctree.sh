#!/usr/bin/env bash
# build_hctree.sh — builds HCTree's sqlite3 amalgamation as a static library
# Output: ~/hctree/bld/libsqlite3_hctree.a  and  ~/hctree/bld/sqlite3.h

# This is a bash safety header — it sets three shell options that make scripts fail loudly and predictably instead of silently continuing on errors.
# Breaking down each flag:

# -e — Exit immediately if any command returns a non-zero exit code (i.e., fails). Without this, bash keeps running even after errors.
# -u — Treat unset/undefined variables as errors and exit. Without this, referencing an undefined variable just gives an empty string silently.
# -o pipefail — If any command in a pipeline fails (e.g., cmd1 | cmd2), the whole pipeline returns a failure. Without this, a pipeline only fails if the last command fails, hiding errors in earlier commands.
set -euo pipefail

# sudo apt install fossil -y
# cd $HOME
# fossil clone https://www.sqlite.org/hctree hctree.fossil
# mkdir -p hctree && cd hctree
# fossil open ../hctree.fossil
# fossil up hctree

HCTREE_DIR="${HCTREE_DIR:-$HOME/hctree}"
BUILD_DIR="$HCTREE_DIR/bld"

# ---- Required compile-time defines (as specified by HCTree) ----------------
HCTREE_DEFINES="-DSQLITE_SHARED_MAPPING=1 \
  -DSQLITE_DEFAULT_MEMSTATUS=0 \
  -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS=1 \
  -DSQLITE_THREADSAFE=1"

HCTREE_CFLAGS="-fno-omit-frame-pointer -g -O2 $HCTREE_DEFINES"

echo "=== [1/4] Installing build dependencies ==="
sudo apt-get install -y gcc make tcl tcl-dev >/dev/null

echo "=== [2/4] Configuring HCTree in $BUILD_DIR ==="
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

if [ ! -f Makefile ]; then
  # --all enables unix-excl VFS and other extensions in the amalgamation
  ../configure --all CFLAGS='-fno-omit-frame-pointer -g -O2'
fi

echo "=== [3/4] Generating amalgamation (sqlite3.c / sqlite3.h) ==="
# mksqlite3c.tcl needs tclsh; the amalgamation does NOT embed #defines —
# those are passed as -D flags at compile time below.
if [ ! -f sqlite3.c ] || [ ! -f sqlite3.h ]; then
  make sqlite3.c sqlite3.h -j"$(nproc)"
fi

echo "=== [4/4] Compiling libsqlite3_hctree.a ==="
# Compile the amalgamation with all required HCTree flags.
# -fPIC makes the .a usable in both static and future shared-lib contexts.
# shellcheck disable=SC2086
gcc $HCTREE_CFLAGS -fPIC -c sqlite3.c -o sqlite3_hctree.o
ar rcs libsqlite3_hctree.a sqlite3_hctree.o

echo ""
echo "Done."
echo "  Static library : $BUILD_DIR/libsqlite3_hctree.a"
echo "  Header         : $BUILD_DIR/sqlite3.h"
echo ""
echo "Next: build YCSB-cpp with HCTree backend:"
echo "  cd ~/YCSB-cpp && make BIND_SQLITE=1 BIND_HCTREE=1 -j\$(nproc)"
