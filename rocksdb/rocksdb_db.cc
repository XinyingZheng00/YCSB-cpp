//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "rocksdb_db.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"

#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/write_batch.h>

#include <unistd.h>
#include <cinttypes>
#include <cstdio>
#include <algorithm>

namespace {
  const std::string PROP_NAME = "rocksdb.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_FORMAT = "rocksdb.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_MERGEUPDATE = "rocksdb.mergeupdate";
  const std::string PROP_MERGEUPDATE_DEFAULT = "false";

  const std::string PROP_DESTROY = "rocksdb.destroy";
  const std::string PROP_DESTROY_DEFAULT = "false";

  const std::string PROP_COMPRESSION = "rocksdb.compression";
  const std::string PROP_COMPRESSION_DEFAULT = "no";

  const std::string PROP_MAX_BG_JOBS = "rocksdb.max_background_jobs";
  const std::string PROP_MAX_BG_JOBS_DEFAULT = "0";

  const std::string PROP_TARGET_FILE_SIZE_BASE = "rocksdb.target_file_size_base";
  const std::string PROP_TARGET_FILE_SIZE_BASE_DEFAULT = "0";

  const std::string PROP_TARGET_FILE_SIZE_MULT = "rocksdb.target_file_size_multiplier";
  const std::string PROP_TARGET_FILE_SIZE_MULT_DEFAULT = "0";

  const std::string PROP_MAX_BYTES_FOR_LEVEL_BASE = "rocksdb.max_bytes_for_level_base";
  const std::string PROP_MAX_BYTES_FOR_LEVEL_BASE_DEFAULT = "0";

  const std::string PROP_WRITE_BUFFER_SIZE = "rocksdb.write_buffer_size";
  const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "0";

  const std::string PROP_MAX_WRITE_BUFFER = "rocksdb.max_write_buffer_number";
  const std::string PROP_MAX_WRITE_BUFFER_DEFAULT = "0";

  const std::string PROP_COMPACTION_PRI = "rocksdb.compaction_pri";
  const std::string PROP_COMPACTION_PRI_DEFAULT = "-1";

  const std::string PROP_MAX_OPEN_FILES = "rocksdb.max_open_files";
  const std::string PROP_MAX_OPEN_FILES_DEFAULT = "-1";

  const std::string PROP_L0_COMPACTION_TRIGGER = "rocksdb.level0_file_num_compaction_trigger";
  const std::string PROP_L0_COMPACTION_TRIGGER_DEFAULT = "0";

  const std::string PROP_L0_SLOWDOWN_TRIGGER = "rocksdb.level0_slowdown_writes_trigger";
  const std::string PROP_L0_SLOWDOWN_TRIGGER_DEFAULT = "0";

  const std::string PROP_L0_STOP_TRIGGER = "rocksdb.level0_stop_writes_trigger";
  const std::string PROP_L0_STOP_TRIGGER_DEFAULT = "0";

  const std::string PROP_USE_DIRECT_WRITE = "rocksdb.use_direct_io_for_flush_compaction";
  const std::string PROP_USE_DIRECT_WRITE_DEFAULT = "false";

  const std::string PROP_USE_DIRECT_READ = "rocksdb.use_direct_reads";
  const std::string PROP_USE_DIRECT_READ_DEFAULT = "false";

  const std::string PROP_USE_MMAP_WRITE = "rocksdb.allow_mmap_writes";
  const std::string PROP_USE_MMAP_WRITE_DEFAULT = "false";

  const std::string PROP_USE_MMAP_READ = "rocksdb.allow_mmap_reads";
  const std::string PROP_USE_MMAP_READ_DEFAULT = "false";

  const std::string PROP_CACHE_SIZE = "rocksdb.cache_size";
  const std::string PROP_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_COMPRESSED_CACHE_SIZE = "rocksdb.compressed_cache_size";
  const std::string PROP_COMPRESSED_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_BLOOM_BITS = "rocksdb.bloom_bits";
  const std::string PROP_BLOOM_BITS_DEFAULT = "0";

  const std::string PROP_INCREASE_PARALLELISM = "rocksdb.increase_parallelism";
  const std::string PROP_INCREASE_PARALLELISM_DEFAULT = "false";

  const std::string PROP_OPTIMIZE_LEVELCOMP = "rocksdb.optimize_level_style_compaction";
  const std::string PROP_OPTIMIZE_LEVELCOMP_DEFAULT = "false";

  const std::string PROP_OPTIONS_FILE = "rocksdb.optionsfile";
  const std::string PROP_OPTIONS_FILE_DEFAULT = "";

  const std::string PROP_ENV_URI = "rocksdb.env_uri";
  const std::string PROP_ENV_URI_DEFAULT = "";

  const std::string PROP_FS_URI = "rocksdb.fs_uri";
  const std::string PROP_FS_URI_DEFAULT = "";

  const std::string PROP_SYNC = "rocksdb.sync";
  const std::string PROP_SYNC_DEFAULT = "false";

  // rocksdb.per_op_db=true  — open+close the DB around every single operation.
  // Enables multi-thread benchmarking against a shared on-disk DB path without
  // keeping a persistent handle.  Much slower than the default shared-handle
  // mode; that overhead is the quantity being measured.
  const std::string PROP_PER_OP_DB = "rocksdb.per_op_db";
  const std::string PROP_PER_OP_DB_DEFAULT = "false";

  // Maximum number of times to retry DB::Open when the LOCK file is held by
  // another thread.  Each retry uses exponential backoff starting at
  // rocksdb.per_op_db_retry_base_us microseconds, capped at 64 ms.
  const std::string PROP_PER_OP_DB_MAX_RETRIES = "rocksdb.per_op_db_max_retries";
  const std::string PROP_PER_OP_DB_MAX_RETRIES_DEFAULT = "100";

  // Initial sleep duration (µs) before the first retry on lock contention.
  const std::string PROP_PER_OP_DB_RETRY_BASE_US = "rocksdb.per_op_db_retry_base_us";
  const std::string PROP_PER_OP_DB_RETRY_BASE_US_DEFAULT = "1000";

  static std::shared_ptr<rocksdb::Env> env_guard;
  static std::shared_ptr<rocksdb::Cache> block_cache;
#if ROCKSDB_MAJOR < 8
  static std::shared_ptr<rocksdb::Cache> block_cache_compressed;
#endif
} // anonymous

namespace ycsbc {

std::vector<rocksdb::ColumnFamilyHandle *> RocksdbDB::cf_handles_;
rocksdb::DB *RocksdbDB::db_ = nullptr;
int RocksdbDB::ref_cnt_ = 0;
std::mutex RocksdbDB::mu_;
rocksdb::WriteOptions RocksdbDB::wopt_;

rocksdb::Options RocksdbDB::shared_opt_;
std::vector<rocksdb::ColumnFamilyDescriptor> RocksdbDB::shared_cf_descs_;
bool RocksdbDB::per_op_opts_ready_ = false;
std::atomic<uint64_t> RocksdbDB::total_lock_retries_{0};

void RocksdbDB::Init() {
// merge operator disabled by default due to link error
#ifdef USE_MERGEUPDATE
  class YCSBUpdateMerge : public rocksdb::AssociativeMergeOperator {
   public:
    virtual bool Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value,
                       const rocksdb::Slice &value, std::string *new_value,
                       rocksdb::Logger *logger) const override {
      assert(existing_value);

      std::vector<Field> values;
      const char *p = existing_value->data();
      const char *lim = p + existing_value->size();
      DeserializeRow(values, p, lim);

      std::vector<Field> new_values;
      p = value.data();
      lim = p + value.size();
      DeserializeRow(new_values, p, lim);

      for (Field &new_field : new_values) {
        bool found = false;
        for (Field &field : values) {
          if (field.name == new_field.name) {
            found = true;
            field.value = new_field.value;
            break;
          }
        }
        if (!found) {
          values.push_back(new_field);
        }
      }

      SerializeRow(values, *new_value);
      return true;
    }

    virtual const char *Name() const override {
      return "YCSBUpdateMerge";
    }
  };
#endif
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  const std::string format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  if (format == "single") {
    format_ = kSingleRow;
    if (props.GetProperty(PROP_PER_OP_DB, PROP_PER_OP_DB_DEFAULT) == "true") {
      per_op_db_enabled_ = true;
      method_read_   = &RocksdbDB::ReadPerOp;
      method_scan_   = &RocksdbDB::ScanPerOp;
      method_update_ = &RocksdbDB::UpdatePerOp;
      method_insert_ = &RocksdbDB::InsertPerOp;
      method_delete_ = &RocksdbDB::DeletePerOp;
#ifdef USE_MERGEUPDATE
      if (props.GetProperty(PROP_MERGEUPDATE, PROP_MERGEUPDATE_DEFAULT) == "true") {
        method_update_ = &RocksdbDB::MergePerOp;
      }
#endif
    } else {
      method_read_   = &RocksdbDB::ReadSingle;
      method_scan_   = &RocksdbDB::ScanSingle;
      method_update_ = &RocksdbDB::UpdateSingle;
      method_insert_ = &RocksdbDB::InsertSingle;
      method_delete_ = &RocksdbDB::DeleteSingle;
#ifdef USE_MERGEUPDATE
      if (props.GetProperty(PROP_MERGEUPDATE, PROP_MERGEUPDATE_DEFAULT) == "true") {
        method_update_ = &RocksdbDB::MergeSingle;
      }
#endif
    }
  } else {
    throw utils::Exception("unknown format");
  }
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  ref_cnt_++;

  const std::string &db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("RocksDB db path is missing");
  }

  if (per_op_db_enabled_) {
    per_op_db_path_ = db_path;
    per_op_max_retries_ = std::stoi(
        props.GetProperty(PROP_PER_OP_DB_MAX_RETRIES, PROP_PER_OP_DB_MAX_RETRIES_DEFAULT));
    per_op_retry_base_us_ = std::stoi(
        props.GetProperty(PROP_PER_OP_DB_RETRY_BASE_US, PROP_PER_OP_DB_RETRY_BASE_US_DEFAULT));

    // Build the options template exactly once (first thread); subsequent
    // threads copy the already-constructed shared_opt_ / shared_cf_descs_.
    if (!per_op_opts_ready_) {
      shared_opt_.create_if_missing = true;
      GetOptions(props, &shared_opt_, &shared_cf_descs_);
#ifdef USE_MERGEUPDATE
      shared_opt_.merge_operator.reset(new YCSBUpdateMerge);
#endif
      if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
        rocksdb::Status s = rocksdb::DestroyDB(db_path, shared_opt_);
        if (!s.ok()) {
          throw utils::Exception(std::string("RocksDB DestroyDB: ") + s.ToString());
        }
      }
      per_op_opts_ready_ = true;
    }

    // Each thread gets its own copy; shared_ptr members (table_factory,
    // merge_operator, block_cache, env) are reference-counted and safe to share.
    per_op_opt_ = shared_opt_;
    per_op_cf_descs_ = shared_cf_descs_;
    return;  // no persistent DB handle in this mode
  }

  if (db_) {
    return;
  }

  rocksdb::Options opt;
  opt.create_if_missing = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
  GetOptions(props, &opt, &cf_descs);
#ifdef USE_MERGEUPDATE
  opt.merge_operator.reset(new YCSBUpdateMerge);
#endif

  rocksdb::Status s;
  if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
    s = rocksdb::DestroyDB(db_path, opt);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB DestroyDB: ") + s.ToString());
    }
  }
  if (cf_descs.empty()) {
    s = rocksdb::DB::Open(opt, db_path, &db_);
  } else {
    s = rocksdb::DB::Open(opt, db_path, cf_descs, &cf_handles_, &db_);
  }
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Open: ") + s.ToString());
  }
}

void RocksdbDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  uint64_t retries = total_lock_retries_.load(std::memory_order_relaxed);
  if (retries > 0) {
    fprintf(stderr,
            "RocksDB per-op mode: %" PRIu64 " total lock-contention retries across all threads\n",
            retries);
  }
  per_op_opts_ready_ = false;
  for (size_t i = 0; i < cf_handles_.size(); i++) {
    if (cf_handles_[i] != nullptr) {
      db_->DestroyColumnFamilyHandle(cf_handles_[i]);
      cf_handles_[i] = nullptr;
    }
  }
  if (db_) {
    delete db_;
    db_ = nullptr;
  }
}

// ---------------------------------------------------------------------------
// Per-op DB open / close
// ---------------------------------------------------------------------------

// Returns true if a failed DB::Open status represents LOCK-file contention
// from another holder (vs. a genuine error we should surface immediately).
static bool IsLockContention(const rocksdb::Status &s) {
  if (!s.IsIOError()) return false;
  const std::string msg = s.ToString();
  // RocksDB lock-error messages contain the word "LOCK" (the filename) or
  // "lock hold by current process".
  return msg.find("LOCK") != std::string::npos ||
         msg.find("lock hold") != std::string::npos;
}

void RocksdbDB::OpenPerOpDB() {
  int sleep_us = per_op_retry_base_us_;
  for (int attempt = 0; ; attempt++) {
    rocksdb::Status s;
    if (per_op_cf_descs_.empty()) {
      s = rocksdb::DB::Open(per_op_opt_, per_op_db_path_, &local_db_);
    } else {
      s = rocksdb::DB::Open(per_op_opt_, per_op_db_path_,
                            per_op_cf_descs_, &local_cf_handles_, &local_db_);
    }
    if (s.ok()) {
      return;
    }
    if (IsLockContention(s) && attempt < per_op_max_retries_) {
      total_lock_retries_.fetch_add(1, std::memory_order_relaxed);
      usleep(static_cast<useconds_t>(sleep_us));
      sleep_us = std::min(sleep_us * 2, 64000);  // exponential backoff, cap 64 ms
      continue;
    }
    throw utils::Exception(std::string("RocksDB Open (per-op): ") + s.ToString());
  }
}

void RocksdbDB::ClosePerOpDB() {
  if (!local_db_) return;
  for (auto *h : local_cf_handles_) {
    local_db_->DestroyColumnFamilyHandle(h);
  }
  local_cf_handles_.clear();
  rocksdb::Status s = local_db_->Close();
  if (!s.ok()) {
    fprintf(stderr, "RocksDB Close (per-op): %s\n", s.ToString().c_str());
  }
  delete local_db_;
  local_db_ = nullptr;
}

// ---------------------------------------------------------------------------
// Per-op dispatch wrappers: open → delegate to *Single → close.
// Exception safety: ClosePerOpDB is called in both the success and the throw
// paths so the DB handle is never leaked.
// ---------------------------------------------------------------------------

#define PER_OP_WRAP(call)          \
  OpenPerOpDB();                   \
  try {                            \
    auto s = (call);               \
    ClosePerOpDB();                \
    return s;                      \
  } catch (...) {                  \
    ClosePerOpDB();                \
    throw;                         \
  }

DB::Status RocksdbDB::ReadPerOp(const std::string &table, const std::string &key,
                                const std::vector<std::string> *fields,
                                std::vector<Field> &result) {
  PER_OP_WRAP(ReadSingle(table, key, fields, result));
}

DB::Status RocksdbDB::ScanPerOp(const std::string &table, const std::string &key, int len,
                                const std::vector<std::string> *fields,
                                std::vector<std::vector<Field>> &result) {
  PER_OP_WRAP(ScanSingle(table, key, len, fields, result));
}

DB::Status RocksdbDB::UpdatePerOp(const std::string &table, const std::string &key,
                                  std::vector<Field> &values) {
  PER_OP_WRAP(UpdateSingle(table, key, values));
}

DB::Status RocksdbDB::MergePerOp(const std::string &table, const std::string &key,
                                 std::vector<Field> &values) {
  PER_OP_WRAP(MergeSingle(table, key, values));
}

DB::Status RocksdbDB::InsertPerOp(const std::string &table, const std::string &key,
                                  std::vector<Field> &values) {
  PER_OP_WRAP(InsertSingle(table, key, values));
}

DB::Status RocksdbDB::DeletePerOp(const std::string &table, const std::string &key) {
  PER_OP_WRAP(DeleteSingle(table, key));
}

#undef PER_OP_WRAP

// ---------------------------------------------------------------------------
// Options parsing
// ---------------------------------------------------------------------------

void RocksdbDB::GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                           std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs) {
  std::string env_uri = props.GetProperty(PROP_ENV_URI, PROP_ENV_URI_DEFAULT);
  std::string fs_uri = props.GetProperty(PROP_FS_URI, PROP_FS_URI_DEFAULT);
  rocksdb::Env* env =  rocksdb::Env::Default();;
  if (!env_uri.empty() || !fs_uri.empty()) {
#if ROCKSDB_MAJOR > 6 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR >= 22)
    rocksdb::Status s = rocksdb::Env::CreateFromUri(rocksdb::ConfigOptions(),
                                                    env_uri, fs_uri, &env, &env_guard);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB CreateFromUri: ") + s.ToString());
    }
#else
    // RocksDB < 6.22 does not have Env::CreateFromUri; fs_uri is unsupported.
    if (!fs_uri.empty()) {
      throw utils::Exception("rocksdb.fs_uri requires RocksDB >= 6.22");
    }
    rocksdb::Status s = rocksdb::Env::LoadEnv(env_uri, &env, &env_guard);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB LoadEnv: ") + s.ToString());
    }
#endif
    opt->env = env;
  }

  const std::string options_file = props.GetProperty(PROP_OPTIONS_FILE, PROP_OPTIONS_FILE_DEFAULT);
  if (options_file != "") {
    rocksdb::ConfigOptions config_options;
    config_options.ignore_unknown_options = false;
    config_options.input_strings_escaped = true;
    config_options.env = env;
    rocksdb::Status s = rocksdb::LoadOptionsFromFile(config_options, options_file, opt, cf_descs);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB LoadOptionsFromFile: ") + s.ToString());
    }
  } else {
    const std::string compression_type = props.GetProperty(PROP_COMPRESSION,
                                                           PROP_COMPRESSION_DEFAULT);
    if (compression_type == "no") {
      opt->compression = rocksdb::kNoCompression;
    } else if (compression_type == "snappy") {
      opt->compression = rocksdb::kSnappyCompression;
    } else if (compression_type == "zlib") {
      opt->compression = rocksdb::kZlibCompression;
    } else if (compression_type == "bzip2") {
      opt->compression = rocksdb::kBZip2Compression;
    } else if (compression_type == "lz4") {
      opt->compression = rocksdb::kLZ4Compression;
    } else if (compression_type == "lz4hc") {
      opt->compression = rocksdb::kLZ4HCCompression;
    } else if (compression_type == "xpress") {
      opt->compression = rocksdb::kXpressCompression;
    } else if (compression_type == "zstd") {
      opt->compression = rocksdb::kZSTD;
    } else {
      throw utils::Exception("Unknown compression type");
    }

    int val = std::stoi(props.GetProperty(PROP_MAX_BG_JOBS, PROP_MAX_BG_JOBS_DEFAULT));
    if (val != 0) {
      opt->max_background_jobs = val;
    }
    val = std::stoi(props.GetProperty(PROP_TARGET_FILE_SIZE_BASE, PROP_TARGET_FILE_SIZE_BASE_DEFAULT));
    if (val != 0) {
      opt->target_file_size_base = val;
    }
    val = std::stoi(props.GetProperty(PROP_TARGET_FILE_SIZE_MULT, PROP_TARGET_FILE_SIZE_MULT_DEFAULT));
    if (val != 0) {
      opt->target_file_size_multiplier = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_BYTES_FOR_LEVEL_BASE, PROP_MAX_BYTES_FOR_LEVEL_BASE_DEFAULT));
    if (val != 0) {
      opt->max_bytes_for_level_base = val;
    }
    val = std::stoi(props.GetProperty(PROP_WRITE_BUFFER_SIZE, PROP_WRITE_BUFFER_SIZE_DEFAULT));
    if (val != 0) {
      opt->write_buffer_size = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_WRITE_BUFFER, PROP_MAX_WRITE_BUFFER_DEFAULT));
    if (val != 0) {
      opt->max_write_buffer_number = val;
    }
    val = std::stoi(props.GetProperty(PROP_COMPACTION_PRI, PROP_COMPACTION_PRI_DEFAULT));
    if (val != -1) {
      opt->compaction_pri = static_cast<rocksdb::CompactionPri>(val);
    }
    val = std::stoi(props.GetProperty(PROP_MAX_OPEN_FILES, PROP_MAX_OPEN_FILES_DEFAULT));
    if (val != 0) {
      opt->max_open_files = val;
    }

    val = std::stoi(props.GetProperty(PROP_L0_COMPACTION_TRIGGER, PROP_L0_COMPACTION_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_file_num_compaction_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_L0_SLOWDOWN_TRIGGER, PROP_L0_SLOWDOWN_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_slowdown_writes_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_L0_STOP_TRIGGER, PROP_L0_STOP_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_stop_writes_trigger = val;
    }

    if (props.GetProperty(PROP_USE_DIRECT_WRITE, PROP_USE_DIRECT_WRITE_DEFAULT) == "true") {
      opt->use_direct_io_for_flush_and_compaction = true;
    }
    if (props.GetProperty(PROP_USE_DIRECT_READ, PROP_USE_DIRECT_READ_DEFAULT) == "true") {
      opt->use_direct_reads = true;
    }
    if (props.GetProperty(PROP_USE_MMAP_WRITE, PROP_USE_MMAP_WRITE_DEFAULT) == "true") {
      opt->allow_mmap_writes = true;
    }
    if (props.GetProperty(PROP_USE_MMAP_READ, PROP_USE_MMAP_READ_DEFAULT) == "true") {
      opt->allow_mmap_reads = true;
    }

    rocksdb::BlockBasedTableOptions table_options;
    size_t cache_size = std::stoul(props.GetProperty(PROP_CACHE_SIZE, PROP_CACHE_SIZE_DEFAULT));
    if (cache_size > 0) {
      block_cache = rocksdb::NewLRUCache(cache_size);
      table_options.block_cache = block_cache;
    }
#if ROCKSDB_MAJOR < 8
    size_t compressed_cache_size = std::stoul(props.GetProperty(PROP_COMPRESSED_CACHE_SIZE,
                                                                PROP_COMPRESSED_CACHE_SIZE_DEFAULT));
    if (compressed_cache_size > 0) {
      block_cache_compressed = rocksdb::NewLRUCache(compressed_cache_size);
      table_options.block_cache_compressed = block_cache_compressed;
    }
#endif
    int bloom_bits = std::stoul(props.GetProperty(PROP_BLOOM_BITS, PROP_BLOOM_BITS_DEFAULT));
    if (bloom_bits > 0) {
      table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloom_bits));
    }
    opt->table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    if (props.GetProperty(PROP_INCREASE_PARALLELISM, PROP_INCREASE_PARALLELISM_DEFAULT) == "true") {
      opt->IncreaseParallelism();
    }
    if (props.GetProperty(PROP_OPTIMIZE_LEVELCOMP, PROP_OPTIMIZE_LEVELCOMP_DEFAULT) == "true") {
      opt->OptimizeLevelStyleCompaction();
    }
    if (props.GetProperty(PROP_SYNC, PROP_SYNC_DEFAULT) == "true") {
      wopt_.sync = true;
    }
  }
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

void RocksdbDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void RocksdbDB::DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                     const std::vector<std::string> &fields) {
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      values.push_back({field, value});
      filter_iter++;
    }
  }
  assert(values.size() == fields.size());
}

void RocksdbDB::DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                     const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRowFilter(values, p, lim, fields);
}

void RocksdbDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values.push_back({field, value});
  }
}

void RocksdbDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}

// ---------------------------------------------------------------------------
// Core single-row operations — all use ActiveDB() so they work in both modes.
// ---------------------------------------------------------------------------

DB::Status RocksdbDB::ReadSingle(const std::string &table, const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  std::string data;
  rocksdb::Status s = ActiveDB()->Get(rocksdb::ReadOptions(), key, &data);
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Get: ") + s.ToString());
  }
  if (fields != nullptr) {
    DeserializeRowFilter(result, data, *fields);
  } else {
    DeserializeRow(result, data);
    assert(result.size() == static_cast<size_t>(fieldcount_));
  }
  return kOK;
}

DB::Status RocksdbDB::ScanSingle(const std::string &table, const std::string &key, int len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {
  rocksdb::Iterator *db_iter = ActiveDB()->NewIterator(rocksdb::ReadOptions());
  db_iter->Seek(key);
  for (int i = 0; db_iter->Valid() && i < len; i++) {
    std::string data = db_iter->value().ToString();
    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, data, *fields);
    } else {
      DeserializeRow(values, data);
      assert(values.size() == static_cast<size_t>(fieldcount_));
    }
    db_iter->Next();
  }
  delete db_iter;
  return kOK;
}

DB::Status RocksdbDB::UpdateSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  std::string data;
  rocksdb::Status s = ActiveDB()->Get(rocksdb::ReadOptions(), key, &data);
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Get: ") + s.ToString());
  }
  std::vector<Field> current_values;
  DeserializeRow(current_values, data);
  assert(current_values.size() == static_cast<size_t>(fieldcount_));
  for (Field &new_field : values) {
    bool found MAYBE_UNUSED = false;
    for (Field &cur_field : current_values) {
      if (cur_field.name == new_field.name) {
        found = true;
        cur_field.value = new_field.value;
        break;
      }
    }
    assert(found);
  }

  data.clear();
  SerializeRow(current_values, data);
  s = ActiveDB()->Put(wopt_, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Put: ") + s.ToString());
  }
  return kOK;
}

DB::Status RocksdbDB::MergeSingle(const std::string &table, const std::string &key,
                                  std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
  rocksdb::Status s = ActiveDB()->Merge(wopt_, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Merge: ") + s.ToString());
  }
  return kOK;
}

DB::Status RocksdbDB::InsertSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
  rocksdb::Status s = ActiveDB()->Put(wopt_, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Put: ") + s.ToString());
  }
  return kOK;
}

DB::Status RocksdbDB::DeleteSingle(const std::string &table, const std::string &key) {
  rocksdb::Status s = ActiveDB()->Delete(wopt_, key);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Delete: ") + s.ToString());
  }
  return kOK;
}

DB *NewRocksdbDB() {
  return new RocksdbDB;
}

const bool registered = DBFactory::RegisterDB("rocksdb", NewRocksdbDB);

} // ycsbc
