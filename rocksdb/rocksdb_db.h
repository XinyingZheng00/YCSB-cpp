//
//  rocksdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_ROCKSDB_DB_H_
#define YCSB_C_ROCKSDB_DB_H_

#include <string>
#include <mutex>
#include <atomic>

#include "core/db.h"
#include "utils/properties.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace ycsbc {

class RocksdbDB : public DB {
 public:
  RocksdbDB() : per_op_db_enabled_(false), per_op_max_retries_(0),
                per_op_retry_base_us_(0), local_db_(nullptr) {}
  ~RocksdbDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    return (this->*(method_read_))(table, key, fields, result);
  }

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return (this->*(method_scan_))(table, key, len, fields, result);
  }

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_update_))(table, key, values);
  }

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_insert_))(table, key, values);
  }

  Status Delete(const std::string &table, const std::string &key) {
    return (this->*(method_delete_))(table, key);
  }

 private:
  enum RocksFormat {
    kSingleRow,
  };
  RocksFormat format_;

  void GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                  std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs);
  static void SerializeRow(const std::vector<Field> &values, std::string &data);
  static void DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                   const std::vector<std::string> &fields);
  static void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                   const std::vector<std::string> &fields);
  static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
  static void DeserializeRow(std::vector<Field> &values, const std::string &data);

  // Core single-row operations — use ActiveDB() for the handle.
  Status ReadSingle(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields, std::vector<Field> &result);
  Status ScanSingle(const std::string &table, const std::string &key, int len,
                    const std::vector<std::string> *fields,
                    std::vector<std::vector<Field>> &result);
  Status UpdateSingle(const std::string &table, const std::string &key,
                      std::vector<Field> &values);
  Status MergeSingle(const std::string &table, const std::string &key,
                     std::vector<Field> &values);
  Status InsertSingle(const std::string &table, const std::string &key,
                      std::vector<Field> &values);
  Status DeleteSingle(const std::string &table, const std::string &key);

  // Per-op wrappers: open DB, delegate to *Single, close DB.
  // Used when rocksdb.per_op_db=true.
  Status ReadPerOp(const std::string &table, const std::string &key,
                   const std::vector<std::string> *fields, std::vector<Field> &result);
  Status ScanPerOp(const std::string &table, const std::string &key, int len,
                   const std::vector<std::string> *fields,
                   std::vector<std::vector<Field>> &result);
  Status UpdatePerOp(const std::string &table, const std::string &key,
                     std::vector<Field> &values);
  Status MergePerOp(const std::string &table, const std::string &key,
                    std::vector<Field> &values);
  Status InsertPerOp(const std::string &table, const std::string &key,
                     std::vector<Field> &values);
  Status DeletePerOp(const std::string &table, const std::string &key);

  // Open/close a per-operation DB handle into local_db_.
  void OpenPerOpDB();
  void ClosePerOpDB();

  // Returns local_db_ when in per-op mode, otherwise the shared db_.
  rocksdb::DB *ActiveDB() const { return local_db_ ? local_db_ : db_; }

  Status (RocksdbDB::*method_read_)(const std::string &, const std:: string &,
                                    const std::vector<std::string> *, std::vector<Field> &);
  Status (RocksdbDB::*method_scan_)(const std::string &, const std::string &,
                                    int, const std::vector<std::string> *,
                                    std::vector<std::vector<Field>> &);
  Status (RocksdbDB::*method_update_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (RocksdbDB::*method_insert_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (RocksdbDB::*method_delete_)(const std::string &, const std::string &);

  int fieldcount_;

  // Per-op mode: per-thread state, initialized in Init(), used in *PerOp methods.
  bool per_op_db_enabled_;
  std::string per_op_db_path_;
  rocksdb::Options per_op_opt_;
  std::vector<rocksdb::ColumnFamilyDescriptor> per_op_cf_descs_;
  int per_op_max_retries_;
  int per_op_retry_base_us_;
  rocksdb::DB *local_db_;
  std::vector<rocksdb::ColumnFamilyHandle *> local_cf_handles_;

  // Shared state for the default (persistent-handle) mode.
  static std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  static rocksdb::DB *db_;
  static int ref_cnt_;
  static std::mutex mu_;
  static rocksdb::WriteOptions wopt_;

  // Shared options template built once under mu_ for per-op mode.
  static rocksdb::Options shared_opt_;
  static std::vector<rocksdb::ColumnFamilyDescriptor> shared_cf_descs_;
  static bool per_op_opts_ready_;
  static std::atomic<uint64_t> total_lock_retries_;
};

DB *NewRocksdbDB();

} // ycsbc

#endif // YCSB_C_ROCKSDB_DB_H_
