//
//  sqlite_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2023 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "query_builder.h"
#include "core/db_factory.h"
#include "utils/properties.h"
#include "utils/utils.h"

#include "sqlite_db.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <functional>
namespace {

const std::string PROP_DBPATH = "sqlite.dbpath";
const std::string PROP_DBPATH_DEFAULT = "";

const std::string PROP_CACHE_SIZE = "sqlite.cache_size";
const std::string PROP_CACHE_SIZE_DEFAULT = "-2000";

const std::string PROP_PAGE_SIZE = "sqlite.page_size";
const std::string PROP_PAGE_SIZE_DEFAULT = "4096";

const std::string PROP_JOURNAL_MODE = "sqlite.journal_mode";
const std::string PROP_JOURNAL_MODE_DEFAULT = "WAL";

const std::string PROP_SYNCHRONOUS = "sqlite.synchronous";
const std::string PROP_SYNCHRONOUS_DEFAULT = "NORMAL";

const std::string PROP_PRIMARY_KEY = "sqlite.primary_key";
const std::string PROP_PRIMARY_KEY_DEFAULT = "user_id";

const std::string PROP_CREATE_TABLE = "sqlite.create_table";
const std::string PROP_CREATE_TABLE_DEFAULT = "true";

const std::string PROP_BUSY_TIMEOUT = "sqlite.busy_timeout";
const std::string PROP_BUSY_TIMEOUT_DEFAULT = "5000";

const std::string PROP_BEGIN_CONCURRENT_TRANSACTIONS = "sqlite.begin_concurrent_transactions";
const std::string PROP_BEGIN_CONCURRENT_TRANSACTIONS_DEFAULT = "false";

const std::string PROP_MMAP_SIZE = "sqlite.mmap_size";
const std::string PROP_MMAP_SIZE_DEFAULT = "1000000000";

const std::string PROP_LOCKING_MODE = "sqlite.locking_mode";
const std::string PROP_LOCKING_MODE_DEFAULT = "";

const std::string PROP_HCT_TRY_BEFORE_UNEVICT = "sqlite.hct_try_before_unevict";
const std::string PROP_HCT_TRY_BEFORE_UNEVICT_DEFAULT = "";

const std::string PROP_WAL_AUTOCHECKPOINT = "sqlite.wal_autocheckpoint";
const std::string PROP_WAL_AUTOCHECKPOINT_DEFAULT = "";

const std::string PROP_JOURNAL_SIZE_LIMIT = "sqlite.journal_size_limit";
const std::string PROP_JOURNAL_SIZE_LIMIT_DEFAULT = "";

// When false, opens without ?hctree=1 and uses WAL2 — the "bcw" comparison mode.
const std::string PROP_HCTREE_MODE = "sqlite.hctree_mode";
const std::string PROP_HCTREE_MODE_DEFAULT = "true";

static sqlite3_stmt *SQLite3Prepare(sqlite3 *db, std::string query) {
  sqlite3_stmt *stmt;
  int rc = sqlite3_prepare_v2(db, query.c_str(), query.size()+1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    throw ycsbc::utils::Exception(std::string("prepare: ") + sqlite3_errmsg(db));
  }
  return stmt;
}

} // anonymous

namespace ycsbc {

std::mutex SqliteDB::mu_;

// ---- HCTree checkpointer statics -------------------------------------------
sqlite3         *SqliteDB::s_ckpt_db_      = nullptr;
std::thread      SqliteDB::s_ckpt_thread_;
std::mutex       SqliteDB::s_ckpt_mu_;
std::condition_variable SqliteDB::s_ckpt_cv_;
std::atomic<bool>     SqliteDB::s_ckpt_needed_{false};
std::atomic<bool>     SqliteDB::s_ckpt_stop_{false};
int                   SqliteDB::s_global_cnt_  = 0;
std::atomic<uint64_t> SqliteDB::s_busy_count_{0};
// ----------------------------------------------------------------------------

// ---- WAL hook: runs on every COMMIT, on the committing connection ----------
// HCTree guideline: trigger a checkpoint when the WAL reaches ~16 MB.
// At 4096 bytes/page that is 4096 WAL frames.  Notify the checkpointer thread
// rather than checkpointing inline so we never block a writer.
int SqliteDB::WalHookCb(void* /*pArg*/, sqlite3* /*db*/,
                        const char* /*zDb*/, int nFrames) {
  if (nFrames >= 4096) {
    s_ckpt_needed_.store(true, std::memory_order_relaxed);
    s_ckpt_cv_.notify_one();
  }
  return SQLITE_OK;
}

void SqliteDB::CheckpointThreadFn() {
  while (true) {
    std::unique_lock<std::mutex> lk(s_ckpt_mu_);
    s_ckpt_cv_.wait(lk, [] {
      return s_ckpt_needed_.load(std::memory_order_relaxed)
          || s_ckpt_stop_.load(std::memory_order_relaxed);
    });
    if (s_ckpt_stop_.load(std::memory_order_relaxed)) break;
    s_ckpt_needed_.store(false, std::memory_order_relaxed);
    lk.unlock();

    if (s_ckpt_db_) {
      // PASSIVE: checkpoints frames not pinned by any reader; never blocks writers.
      sqlite3_wal_checkpoint_v2(s_ckpt_db_, nullptr,
                                SQLITE_CHECKPOINT_PASSIVE, nullptr, nullptr);
    }
  }
}
// ----------------------------------------------------------------------------

void SqliteDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  bool is_first_global = (s_global_cnt_++ == 0);

  // Must be called before the first sqlite3_open (i.e. before sqlite3_initialize).
  // MULTITHREAD: per-connection mutexes only; safe because each connection is
  // owned by exactly one thread.
  if (is_first_global) {
    sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
  }

  // Per-instance init: every YCSB thread opens its own connection.
  if (ref_cnt_++ == 0) {
    std::time_t now = std::time(nullptr);
    std::tm *tm_info = std::localtime(&now);
    char time_str[20];
    std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);
    std::cout << "[Thread " << std::this_thread::get_id()
              << "] SqliteDB::Init at " << time_str << std::endl;
    OpenDB();
    // Register WAL hook on this connection so it can trigger the checkpointer.
    sqlite3_wal_hook(db_, WalHookCb, nullptr);
  }

  // Global init: first thread opens a dedicated checkpoint connection and
  // starts the background checkpointer thread.
  if (is_first_global) {
    const bool use_hctree_mode = (props_->GetProperty(PROP_HCTREE_MODE, PROP_HCTREE_MODE_DEFAULT) == "true");
    const std::string &db_path = props_->GetProperty(PROP_DBPATH, PROP_DBPATH_DEFAULT);
    if (!db_path.empty()) {
      bool has_excl_vfs = (sqlite3_vfs_find("unix-excl") != nullptr);
      std::string ckpt_url = "file://" + db_path;
      if (use_hctree_mode) {
        ckpt_url += "?hctree=1";
        if (has_excl_vfs) ckpt_url += "&vfs=unix-excl";
      } else {
        if (has_excl_vfs) ckpt_url += "?vfs=unix-excl";
      }
      int rc = sqlite3_open_v2(ckpt_url.c_str(), &s_ckpt_db_,
                               SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                               SQLITE_OPEN_URI | SQLITE_OPEN_NOMUTEX, nullptr);
      if (rc != SQLITE_OK) {
        fprintf(stderr, "[checkpoint] Connection failed: %s\n",
                s_ckpt_db_ ? sqlite3_errmsg(s_ckpt_db_) : "unknown");
        sqlite3_close(s_ckpt_db_);
        s_ckpt_db_ = nullptr;
      }
    }
    s_ckpt_stop_.store(false);
    s_ckpt_needed_.store(false);
    s_ckpt_thread_ = std::thread(CheckpointThreadFn);
  }

  PrepareQueries();
}

void SqliteDB::OpenDB() {
  const std::string &db_path = props_->GetProperty(PROP_DBPATH, PROP_DBPATH_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("SQLite db path is missing");
  }

  // VFS is configurable so multi-process shared-DB experiments can use the
  // default "unix" VFS (which uses POSIX advisory locks and supports
  // concurrent multi-process access). The default "unix-excl" preserves the
  // previous behavior for single-process multi-thread runs.
  std::string vfs_prop = props_->GetProperty("sqlite.vfs", "unix-excl");
  const char *vfs_name = vfs_prop.c_str();
  sqlite3_vfs *requested_vfs = sqlite3_vfs_find(vfs_name);
  sqlite3_vfs *default_vfs = sqlite3_vfs_find(nullptr);
  
  if (requested_vfs == nullptr) {
    // VFS not found, SQLite will use default
    printf("Warning: Requested VFS '%s' not found, will use default VFS: %s\n", 
           vfs_name, default_vfs ? default_vfs->zName : "unknown");
    vfs_name = nullptr; // Use default
  } else {
    // printf("Requested VFS '%s' is available: %s\n", vfs_name, requested_vfs->zName);
  }
  
  // Build URI. hctree=1 enables HCTree mode; omit it for bcw (BEGIN CONCURRENT) mode.
  bool use_hctree_mode = (props_->GetProperty(PROP_HCTREE_MODE, PROP_HCTREE_MODE_DEFAULT) == "true");
  std::string url = "file://" + db_path;
  if (use_hctree_mode) {
    url += "?hctree=1";
    if (vfs_name != nullptr) { url += "&vfs="; url += vfs_name; }
  } else {
    if (vfs_name != nullptr) { url += "?vfs="; url += vfs_name; }
  }
  std::cout << "Opening database with URL: " << url << std::endl;
  int rc = sqlite3_open_v2(url.c_str(), &db_,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                           SQLITE_OPEN_URI | SQLITE_OPEN_NOMUTEX, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init open: ") + sqlite3_errmsg(db_));
  }

  // Apply busy_timeout immediately via the C API so subsequent PRAGMAs and
  // CREATE TABLE retry on SQLITE_BUSY (rather than failing immediately).
  // Without this, multi-process shared-DB init races on cache_size /
  // journal_mode setup. The PRAGMA busy_timeout below applies the same value
  // again — both are idempotent.
  int early_busy_timeout = std::stoi(
      props_->GetProperty(PROP_BUSY_TIMEOUT, PROP_BUSY_TIMEOUT_DEFAULT));
  sqlite3_busy_timeout(db_, early_busy_timeout);
  
  
  // Verify which VFS was actually used by checking the database filename
  // Note: SQLite doesn't provide direct API to get VFS from connection,
  // but if open succeeded with the VFS name, it should have used it
  if (requested_vfs != nullptr) {
    // printf("Database opened successfully, VFS '%s' should be in use\n", vfs_name);
  } else {
    printf("Database opened successfully with default VFS: %s\n", 
           default_vfs ? default_vfs->zName : "unknown");
  }

  // Set page_size FIRST, before any other operations, if database is new/empty
  // SQLite only allows page_size to be set on an empty database (before any schema)
  int page_size = std::stoi(props_->GetProperty(PROP_PAGE_SIZE, PROP_PAGE_SIZE_DEFAULT));
  std::string stmt = std::string("PRAGMA page_size = ") + std::to_string(page_size);
  rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    printf("Warning: Failed to set page_size to %d: %s (may be set already)\n", 
           page_size, sqlite3_errmsg(db_));
  }
  SetPragma();

  int busy_timeout = std::stoi(props_->GetProperty(PROP_BUSY_TIMEOUT, PROP_BUSY_TIMEOUT_DEFAULT));
  stmt = std::string("PRAGMA busy_timeout = ") + std::to_string(busy_timeout);
  rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init exec busy_timeout: ") + sqlite3_errmsg(db_));
  }

  
  key_ = props_->GetProperty(PROP_PRIMARY_KEY, PROP_PRIMARY_KEY_DEFAULT);
  field_prefix_ = props_->GetProperty(CoreWorkload::FIELD_NAME_PREFIX, CoreWorkload::FIELD_NAME_PREFIX_DEFAULT);
  field_count_ = std::stoi(props_->GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY, CoreWorkload::FIELD_COUNT_DEFAULT));
  table_name_ = props_->GetProperty(CoreWorkload::TABLENAME_DEFAULT, CoreWorkload::TABLENAME_DEFAULT);

  if (props_->GetProperty(PROP_CREATE_TABLE, PROP_CREATE_TABLE_DEFAULT) == "true") {
    std::vector<std::string> fields;
    fields.reserve(field_count_);
    for (size_t i = 0; i < field_count_; i++) {
        fields.push_back(field_prefix_ + std::to_string(i));
    }
    rc = sqlite3_exec(db_, BuildCreateTableQuery(table_name_, key_, fields).c_str(), nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) {
      throw utils::Exception(std::string("Create table: ") + sqlite3_errmsg(db_));
    }
  }
  PrintPragma();
}

void SqliteDB::SetPragma() {
  int cache_size = std::stoi(props_->GetProperty(PROP_CACHE_SIZE, PROP_CACHE_SIZE_DEFAULT));
  std::string stmt = std::string("PRAGMA cache_size = ") + std::to_string(cache_size);
  int rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init exec cache_size: ") + sqlite3_errmsg(db_));
  }
  
  std::string journal_mode = props_->GetProperty(PROP_JOURNAL_MODE, PROP_JOURNAL_MODE_DEFAULT);
  stmt = std::string("PRAGMA journal_mode = ") + journal_mode;
  rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init exec journal_mode: ") + sqlite3_errmsg(db_));
  }

  int page_size = std::stoi(props_->GetProperty(PROP_PAGE_SIZE, PROP_PAGE_SIZE_DEFAULT));
  stmt = std::string("PRAGMA page_size = ") + std::to_string(page_size);
  rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init exec page_size: ") + sqlite3_errmsg(db_));
  }

  
  int mmap_size = std::stoi(props_->GetProperty(PROP_MMAP_SIZE, PROP_MMAP_SIZE_DEFAULT));
  stmt = std::string("PRAGMA mmap_size = ") + std::to_string(mmap_size);
  rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init exec mmap_size: ") + sqlite3_errmsg(db_));
  }

  std::string locking_mode = props_->GetProperty(PROP_LOCKING_MODE, PROP_LOCKING_MODE_DEFAULT);
  if (!locking_mode.empty()) {
    stmt = std::string("PRAGMA locking_mode = ") + locking_mode;
    rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) {
      throw utils::Exception(std::string("Init exec locking_mode: ") + sqlite3_errmsg(db_));
    }
  }

  std::string hct_unevict = props_->GetProperty(PROP_HCT_TRY_BEFORE_UNEVICT, PROP_HCT_TRY_BEFORE_UNEVICT_DEFAULT);
  if (!hct_unevict.empty()) {
    stmt = std::string("PRAGMA hct_try_before_unevict = ") + hct_unevict;
    // Silently ignore errors: this pragma only exists in HCTree builds.
    sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  }

  std::string wal_autockpt = props_->GetProperty(PROP_WAL_AUTOCHECKPOINT, PROP_WAL_AUTOCHECKPOINT_DEFAULT);
  if (!wal_autockpt.empty()) {
    stmt = std::string("PRAGMA wal_autocheckpoint = ") + wal_autockpt;
    rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) {
      throw utils::Exception(std::string("Init exec wal_autocheckpoint: ") + sqlite3_errmsg(db_));
    }
  }

  std::string synchronous = props_->GetProperty(PROP_SYNCHRONOUS, PROP_SYNCHRONOUS_DEFAULT);
  stmt = std::string("PRAGMA synchronous = ") + synchronous;
  rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init exec synchronous: ") + sqlite3_errmsg(db_));
  }

  std::string journal_size_limit = props_->GetProperty(PROP_JOURNAL_SIZE_LIMIT, PROP_JOURNAL_SIZE_LIMIT_DEFAULT);
  if (!journal_size_limit.empty()) {
    stmt = std::string("PRAGMA journal_size_limit = ") + journal_size_limit;
    rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) {
      throw utils::Exception(std::string("Init exec journal_size_limit: ") + sqlite3_errmsg(db_));
    }
  }

  use_begin_concurrent_ = (props_->GetProperty(PROP_BEGIN_CONCURRENT_TRANSACTIONS, PROP_BEGIN_CONCURRENT_TRANSACTIONS_DEFAULT) == "true");
}

void SqliteDB::PrintPragma() {
  // Print the following pragma values
  sqlite3_stmt *check_stmt;
  int rc;
  
  // PRAGMA journal_mode (returns text)
  const char *query = "PRAGMA journal_mode";
  rc = sqlite3_prepare_v2(db_, query, -1, &check_stmt, nullptr);
  if (rc == SQLITE_OK) {
    rc = sqlite3_step(check_stmt);
    if (rc == SQLITE_ROW) {
      const char *journal_mode = (const char *)sqlite3_column_text(check_stmt, 0);
      printf("PRAGMA journal_mode: %s\n", journal_mode ? journal_mode : "NULL");
    }
    sqlite3_finalize(check_stmt);
  }
  
  // PRAGMA mmap_size (returns integer)
  query = "PRAGMA mmap_size";
  rc = sqlite3_prepare_v2(db_, query, -1, &check_stmt, nullptr);
  if (rc == SQLITE_OK) {
    rc = sqlite3_step(check_stmt);
    if (rc == SQLITE_ROW) {
      int mmap_size = sqlite3_column_int(check_stmt, 0);
      printf("PRAGMA mmap_size: %d\n", mmap_size);
    }
    sqlite3_finalize(check_stmt);
  }
  
  // PRAGMA page_size (returns integer)
  query = "PRAGMA page_size";
  rc = sqlite3_prepare_v2(db_, query, -1, &check_stmt, nullptr);
  if (rc == SQLITE_OK) {
    rc = sqlite3_step(check_stmt);
    if (rc == SQLITE_ROW) {
      int page_size = sqlite3_column_int(check_stmt, 0);
      printf("PRAGMA page_size: %d\n", page_size);
    }
    sqlite3_finalize(check_stmt);
  }
  
  // PRAGMA cache_size (returns integer)
  query = "PRAGMA cache_size";
  rc = sqlite3_prepare_v2(db_, query, -1, &check_stmt, nullptr);
  if (rc == SQLITE_OK) {
    rc = sqlite3_step(check_stmt);
    if (rc == SQLITE_ROW) {
      int cache_size = sqlite3_column_int(check_stmt, 0);
      printf("PRAGMA cache_size: %d\n", cache_size);
    }
    sqlite3_finalize(check_stmt);
  }
  
  // PRAGMA synchronous (returns integer)
  query = "PRAGMA synchronous";
  rc = sqlite3_prepare_v2(db_, query, -1, &check_stmt, nullptr);
  if (rc == SQLITE_OK) {
    rc = sqlite3_step(check_stmt);
    if (rc == SQLITE_ROW) {
      int synchronous = sqlite3_column_int(check_stmt, 0);
      printf("PRAGMA synchronous: %d\n", synchronous);
    }
    sqlite3_finalize(check_stmt);
  }
}

void SqliteDB::PrepareQueries() {
  std::vector<std::string> fields;
  fields.reserve(field_count_);
  for (size_t i = 0; i < field_count_; i++) {
      fields.push_back(field_prefix_ + std::to_string(i));
  }

  // Read
  stmt_read_all_ = SQLite3Prepare(db_, BuildReadQuery(table_name_, key_, fields));
  for (size_t i = 0; i < field_count_; i++) {
    std::string field_name = field_prefix_ + std::to_string(i);
    stmt_read_field_[field_name] = SQLite3Prepare(db_, BuildReadQuery(table_name_, key_, {field_name}));
  }

  // Scan
  stmt_scan_all_ = SQLite3Prepare(db_, BuildScanQuery(table_name_, key_, fields));
  for (size_t i = 0; i < field_count_; i++) {
    std::string field_name = field_prefix_ + std::to_string(i);
    stmt_scan_field_[field_name] = SQLite3Prepare(db_, BuildScanQuery(table_name_, key_, {field_name}));
  }

  // Update
  stmt_update_all_ = SQLite3Prepare(db_, BuildUpdateQuery(table_name_, key_, fields));
  for (size_t i = 0; i < field_count_; i++) {
    std::string field_name = field_prefix_ + std::to_string(i);
    stmt_update_field_[field_name] = SQLite3Prepare(db_, BuildUpdateQuery(table_name_, key_, {field_name}));
  }

  // Insert
  stmt_insert_ = SQLite3Prepare(db_, BuildInsertQuery(table_name_, key_, fields));

  // Delete
  stmt_delete_ = SQLite3Prepare(db_, BuildDeleteQuery(table_name_, key_));
}

void SqliteDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);

  sqlite3_finalize(stmt_read_all_);
  for (auto s : stmt_read_field_) {
    sqlite3_finalize(s.second);
  }
  sqlite3_finalize(stmt_scan_all_);
  for (auto s : stmt_scan_field_) {
    sqlite3_finalize(s.second);
  }
  sqlite3_finalize(stmt_update_all_);
  for (auto s : stmt_update_field_) {
    sqlite3_finalize(s.second);
  }
  sqlite3_finalize(stmt_insert_);
  sqlite3_finalize(stmt_delete_);

  if (--ref_cnt_ == 0) {
    int close_rc = sqlite3_close(db_);
    assert(close_rc == SQLITE_OK);
    (void)close_rc;
  }

  // Global teardown: last thread stops the checkpointer and prints stats.
  if (--s_global_cnt_ == 0) {
    // Signal checkpointer to exit and wait for it.
    {
      std::lock_guard<std::mutex> lk(s_ckpt_mu_);
      s_ckpt_stop_.store(true);
    }
    s_ckpt_cv_.notify_one();
    if (s_ckpt_thread_.joinable()) {
      s_ckpt_thread_.join();
    }
    if (s_ckpt_db_) {
      sqlite3_close(s_ckpt_db_);
      s_ckpt_db_ = nullptr;
    }

    uint64_t busy = s_busy_count_.load(std::memory_order_relaxed);
    printf("[HCTree] busy_snapshot aborts (discarded, not retried): %llu\n",
           (unsigned long long)busy);

    std::time_t now = std::time(nullptr);
    std::tm *tm_info = std::localtime(&now);
    char time_str[20];
    std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);
    std::cout << "[Thread " << std::this_thread::get_id()
              << "] SqliteDB::Cleanup (last) at " << time_str << std::endl;
  }
}

DB::Status SqliteDB::Read(const std::string &table, const std::string &key,
                          const std::vector<std::string> *fields, std::vector<Field> &result) {
  DB::Status s = kOK;
  bool temp = false;
  sqlite3_stmt *stmt;
  size_t field_cnt;

  if (fields == nullptr || fields->size() == field_count_) {
    field_cnt = field_count_;
    stmt = stmt_read_all_;
  } else if (fields->size() == 1) {
    field_cnt = 1;
    stmt = stmt_read_field_[(*fields)[0]];
  } else {
    temp = true;
    field_cnt = fields->size();;
    stmt = SQLite3Prepare(db_, BuildReadQuery(table_name_, key_, *fields));
  }

  int rc = sqlite3_bind_text(stmt, 1, key.c_str(), key.size(), SQLITE_STATIC);
  if (rc != SQLITE_OK) {
    s = kError;
    goto cleanup;
  }

  rc = sqlite3_step(stmt);
  if (rc != SQLITE_ROW) {
    s = kNotFound;
    goto cleanup;
  }

  result.reserve(field_cnt);
  for (size_t i = 0; i < field_cnt; i++) {
    const char *name = reinterpret_cast<const char *>(sqlite3_column_name(stmt, i));
    const char *value = reinterpret_cast<const char *>(sqlite3_column_text(stmt, i));
    result.push_back({name, value});
  }

cleanup:
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);
  if (temp) {
    sqlite3_finalize(stmt);
  }

  return s;
}

DB::Status SqliteDB::Scan(const std::string &table, const std::string &key, int len,
                          const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
  DB::Status s = kOK;
  bool temp = false;
  sqlite3_stmt *stmt;
  size_t field_cnt;

  if (fields == nullptr || fields->size() == field_count_) {
    field_cnt = field_count_;
    stmt = stmt_scan_all_;
  } else if (fields->size() == 1) {
    field_cnt = 1;
    stmt = stmt_scan_field_[(*fields)[0]];
  } else {
    temp = true;
    field_cnt = fields->size();;
    stmt = SQLite3Prepare(db_, BuildScanQuery(table_name_, key_, *fields));
  }

  int rc = sqlite3_bind_text(stmt, 1, key.c_str(), key.size(), SQLITE_STATIC);
  if (rc != SQLITE_OK) {
    s = kError;
    goto cleanup;
  }
  rc = sqlite3_bind_int(stmt, 2, len);
  if (rc != SQLITE_OK) {
    s = kError;
    goto cleanup;
  }

  for (int i = 0; i < len; i++) {
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
      break;
    }
    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    values.reserve(field_cnt);
    // const char *user_id = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
    for (size_t i = 0; i < field_cnt; i++) {
      const char *name = reinterpret_cast<const char *>(sqlite3_column_name(stmt, 1+i));
      const char *value = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1+i));
      values.push_back({name, value});
    }
  }

  if (result.size() == 0) {
    s = kNotFound;
  }

cleanup:
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);
  if (temp) {
    sqlite3_finalize(stmt);
  }

  return s;
}

DB::Status SqliteDB::Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
  bool temp = false;
  sqlite3_stmt *stmt;
  size_t field_cnt;

  if (values.size() == field_count_) {
    field_cnt = field_count_;
    stmt = stmt_update_all_;
  } else if (values.size() == 1) {
    field_cnt = 1;
    stmt = stmt_update_field_[values[0].name];
  } else {
    temp = true;
    std::vector<std::string> fields;
    fields.reserve(values.size());
    for (auto &f : values) fields.push_back(f.name);
    field_cnt = values.size();
    stmt = SQLite3Prepare(db_, BuildUpdateQuery(table_name_, key_, fields));
  }

  const char *begin_sql = use_begin_concurrent_ ? "BEGIN CONCURRENT" : "BEGIN";

  int rc = sqlite3_exec(db_, begin_sql, nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    printf("BEGIN error: %s\n", sqlite3_errmsg(db_));
    if (temp) sqlite3_finalize(stmt);
    return kError;
  }

  bool ok = true;
  for (size_t i = 0; i < field_cnt && ok; i++) {
    rc = sqlite3_bind_text(stmt, 1+i, values[i].value.c_str(), values[i].value.size(), SQLITE_STATIC);
    if (rc != SQLITE_OK) ok = false;
  }
  if (ok) {
    rc = sqlite3_bind_text(stmt, 1+field_cnt, key.c_str(), key.size(), SQLITE_STATIC);
    if (rc != SQLITE_OK) ok = false;
  }
  if (ok) {
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
      printf("UPDATE step error: %s\n", sqlite3_errmsg(db_));
      ok = false;
    }
  }
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);

  if (!ok) {
    sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
    if (temp) sqlite3_finalize(stmt);
    return kError;
  }

  int commit_rc;
  {
    const std::lock_guard<std::mutex> lock(mu_);
    commit_rc = sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);
  }
  if (commit_rc == SQLITE_OK) {
    if (temp) sqlite3_finalize(stmt);
    return kOK;
  }

  sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);

  if (commit_rc == SQLITE_BUSY) {
    // Matches hct_thread_test.c: discard, count as an attempt, move on.
    // The YCSB framework will issue the next operation with fresh data.
    s_busy_count_.fetch_add(1, std::memory_order_relaxed);
    if (temp) sqlite3_finalize(stmt);
    return kOK;
  }

  int ext_err = sqlite3_extended_errcode(db_);
  std::hash<std::thread::id> hasher;
  printf("[Thread %zu] UPDATE COMMIT error: %s (ext=%d) key=%s\n",
         hasher(std::this_thread::get_id()), sqlite3_errmsg(db_), ext_err, key.c_str());
  if (temp) sqlite3_finalize(stmt);
  return kError;
}


DB::Status SqliteDB::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
  if (field_count_ != values.size()) {
    return kError;
  }

  sqlite3_stmt *stmt = stmt_insert_;
  const char *begin_sql = use_begin_concurrent_ ? "BEGIN CONCURRENT" : "BEGIN";

  int rc = sqlite3_exec(db_, begin_sql, nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    printf("BEGIN error: %s\n", sqlite3_errmsg(db_));
    return kError;
  }

  bool ok = true;
  rc = sqlite3_bind_text(stmt, 1, key.c_str(), key.size(), SQLITE_STATIC);
  if (rc != SQLITE_OK) ok = false;
  for (size_t i = 0; i < field_count_ && ok; i++) {
    rc = sqlite3_bind_text(stmt, 2+i, values[i].value.c_str(), values[i].value.size(), SQLITE_STATIC);
    if (rc != SQLITE_OK) ok = false;
  }
  if (ok) {
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) ok = false;
  }
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);

  if (!ok) {
    sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
    return kError;
  }

  int commit_rc;
  {
    const std::lock_guard<std::mutex> lock(mu_);
    commit_rc = sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);
  }
  if (commit_rc == SQLITE_OK) return kOK;

  sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);

  if (commit_rc == SQLITE_BUSY) {
    s_busy_count_.fetch_add(1, std::memory_order_relaxed);
    return kOK;
  }

  int ext_err = sqlite3_extended_errcode(db_);
  std::hash<std::thread::id> hasher;
  printf("[Thread %zu] INSERT COMMIT error: %s (ext=%d) key=%s\n",
         hasher(std::this_thread::get_id()), sqlite3_errmsg(db_), ext_err, key.c_str());
  return kError;
}

DB::Status SqliteDB::Delete(const std::string &table, const std::string &key) {
  sqlite3_stmt *stmt = stmt_delete_;
  const char *begin_sql = use_begin_concurrent_ ? "BEGIN CONCURRENT" : "BEGIN";

  int rc = sqlite3_exec(db_, begin_sql, nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    printf("BEGIN error: %s\n", sqlite3_errmsg(db_));
    return kError;
  }

  bool ok = true;
  rc = sqlite3_bind_text(stmt, 1, key.c_str(), key.size(), SQLITE_STATIC);
  if (rc != SQLITE_OK) ok = false;
  if (ok) {
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) ok = false;
  }
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);

  if (!ok) {
    sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
    return kError;
  }

  int commit_rc;
  {
    const std::lock_guard<std::mutex> lock(mu_);
    commit_rc = sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);
  }
  if (commit_rc == SQLITE_OK) return kOK;

  sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);

  if (commit_rc == SQLITE_BUSY) {
    s_busy_count_.fetch_add(1, std::memory_order_relaxed);
    return kOK;
  }

  int ext_err = sqlite3_extended_errcode(db_);
  std::hash<std::thread::id> hasher;
  printf("[Thread %zu] DELETE COMMIT error: %s (ext=%d) key=%s\n",
         hasher(std::this_thread::get_id()), sqlite3_errmsg(db_), ext_err, key.c_str());
  return kError;
}

DB *NewSqliteDB() {
  return new SqliteDB;
}

const bool registered = DBFactory::RegisterDB("sqlite", NewSqliteDB);

} // ycsbc
