//
//  sqlite_db.h
//  YCSB-cpp
//
//  Copyright (c) 2023 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_SQLITE_DB_H_
#define YCSB_C_SQLITE_DB_H_

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "core/db.h"

#include <sqlite3.h>

namespace ycsbc {

class SqliteDB : public DB {
 public:
  SqliteDB() {}
  ~SqliteDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result);

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values);

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values);

  Status Delete(const std::string &table, const std::string &key);

 private:
  void OpenDB();
  void SetPragma();
  void PrintPragma();
  void PrepareQueries();

  static std::mutex mu_;

  // ----------- HCTree checkpointer (global, shared across all instances) ---
  // Dedicated connection used only for sqlite3_wal_checkpoint_v2 calls.
  static sqlite3 *s_ckpt_db_;
  // Background thread that runs checkpoints when the WAL grows past ~16 MB.
  static std::thread s_ckpt_thread_;
  // Separate mutex/cv so the checkpointer never contends with commit mu_.
  static std::mutex s_ckpt_mu_;
  static std::condition_variable s_ckpt_cv_;
  static std::atomic<bool> s_ckpt_needed_;
  static std::atomic<bool> s_ckpt_stop_;
  // Reference count across ALL SqliteDB instances (not per-instance).
  static int s_global_cnt_;
  // Counts COMMIT attempts that returned SQLITE_BUSY and were discarded
  // (matches nBusy in hct_thread_test.c — one increment per aborted txn,
  // no retry; the operation is counted as an attempt and the caller moves on).
  static std::atomic<uint64_t> s_busy_count_;

 public:
  static uint64_t BusyCount() { return s_busy_count_.load(std::memory_order_relaxed); }

  // WAL hook: fires after each COMMIT; schedules a checkpoint when WAL >= 16 MB.
  static int WalHookCb(void *pArg, sqlite3 *db, const char *zDb, int nFrames);
  static void CheckpointThreadFn();
  // --------------------------------------------------------------------------
  

  sqlite3 *db_;
  int ref_cnt_;
  // std::mutex mu_;

  std::string key_;
  std::string field_prefix_;
  size_t field_count_;
  std::string table_name_;
  bool use_begin_concurrent_;
  
  sqlite3_stmt *stmt_read_all_;
  sqlite3_stmt *stmt_scan_all_;
  sqlite3_stmt *stmt_update_all_;
  sqlite3_stmt *stmt_insert_;
  sqlite3_stmt *stmt_delete_;
  std::unordered_map<std::string, sqlite3_stmt *> stmt_read_field_;
  std::unordered_map<std::string, sqlite3_stmt *> stmt_scan_field_;
  std::unordered_map<std::string, sqlite3_stmt *> stmt_update_field_;
};

DB *NewSqliteDB();

} // ycsbc

#endif // YCSB_C_SQLITE_DB_H_
