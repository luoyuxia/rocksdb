//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include "rocksdb/sst_file_reader.h"

#include "db/arena_wrapped_db_iter.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "file/random_access_file_reader.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "table/get_context.h"
#include "table/table_builder.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

struct SstFileReader::Rep {
  Options options;
  EnvOptions soptions;
  ImmutableOptions ioptions;
  MutableCFOptions moptions;

  autovector<std::unique_ptr<TableReader>> table_readers;
  std::unique_ptr<TableReader> table_reader;

  Rep(const Options& opts)
      : options(opts),
        soptions(options),
        ioptions(options),
        moptions(ColumnFamilyOptions(options)) {}
};

SstFileReader::SstFileReader(const Options& options) : rep_(new Rep(options)) {}

SstFileReader::~SstFileReader() = default;

Status SstFileReader::Open(const std::vector<std::string>& file_paths) {
  auto r = rep_.get();
  r->table_readers.resize(file_paths.size());
  Status s;
  const auto& fs = r->options.env->GetFileSystem();
  for(unsigned long i = 0; i < file_paths.size(); i++) {
    const auto& file_path = file_paths[i];
    FileOptions fopts(r->soptions);
    uint64_t file_size = 0;
    std::unique_ptr<FSRandomAccessFile> file;
    std::unique_ptr<RandomAccessFileReader> file_reader;
    s = fs->GetFileSize(file_path, fopts.io_options, &file_size, nullptr);
    if (s.ok()) {
      s = fs->NewRandomAccessFile(file_path, fopts, &file, nullptr);
    }
    if (s.ok()) {
      file_reader.reset(new RandomAccessFileReader(std::move(file), file_path));
    }

    if (s.ok()) {
      TableReaderOptions t_opt(r->ioptions, r->moptions.prefix_extractor,
                               r->soptions, r->ioptions.internal_comparator,
                               r->moptions.block_protection_bytes_per_key);
      // Allow open file with global sequence number for backward compatibility.
      t_opt.largest_seqno = kMaxSequenceNumber;
      s = r->options.table_factory->NewTableReader(t_opt, std::move(file_reader),
                                                   file_size, &r->table_readers[i]);
    }
  }
  return s;
}

//Status SstFileReader::Open(const std::string& file_path) {
//  std::vector<std::string> file_paths;
//  file_paths.push_back(file_path);
//  return Open(file_paths);
//}

Status SstFileReader::Open(const std::string& file_path) {
  auto r = rep_.get();
  Status s;
  uint64_t file_size = 0;
  std::unique_ptr<FSRandomAccessFile> file;
  std::unique_ptr<RandomAccessFileReader> file_reader;
  FileOptions fopts(r->soptions);
  const auto& fs = r->options.env->GetFileSystem();

  s = fs->GetFileSize(file_path, fopts.io_options, &file_size, nullptr);
  if (s.ok()) {
    s = fs->NewRandomAccessFile(file_path, fopts, &file, nullptr);
  }
  if (s.ok()) {
    file_reader.reset(new RandomAccessFileReader(std::move(file), file_path));
  }
  if (s.ok()) {
    TableReaderOptions t_opt(r->ioptions, r->moptions.prefix_extractor,
                             r->soptions, r->ioptions.internal_comparator,
                             r->moptions.block_protection_bytes_per_key);
    // Allow open file with global sequence number for backward compatibility.
    t_opt.largest_seqno = kMaxSequenceNumber;
    s = r->options.table_factory->NewTableReader(t_opt, std::move(file_reader),
                                                 file_size, &r->table_reader);
  }
  return s;
}

//Status SstFileReader::Open(const std::string& file_path) {
//  auto r = rep_.get();
//  Status s;
//  uint64_t file_size = 0;
//  std::unique_ptr<FSRandomAccessFile> file;
//  std::unique_ptr<RandomAccessFileReader> file_reader;
//  FileOptions fopts(r->soptions);
//  const auto& fs = r->options.env->GetFileSystem();
//  r->table_readers.resize(1);
//
//  s = fs->GetFileSize(file_path, fopts.io_options, &file_size, nullptr);
//  if (s.ok()) {
//    s = fs->NewRandomAccessFile(file_path, fopts, &file, nullptr);
//  }
//  if (s.ok()) {
//    file_reader.reset(new RandomAccessFileReader(std::move(file), file_path));
//  }
//  if (s.ok()) {
//    TableReaderOptions t_opt(r->ioptions, r->moptions.prefix_extractor,
//                             r->soptions, r->ioptions.internal_comparator,
//                             r->moptions.block_protection_bytes_per_key);
//    // Allow open file with global sequence number for backward compatibility.
//    t_opt.largest_seqno = kMaxSequenceNumber;
//    s = r->options.table_factory->NewTableReader(t_opt, std::move(file_reader),
//                                                 file_size, &r->table_readers[0]);
//  }
//  return s;
//}

//Iterator* SstFileReader::NewIterator(const ReadOptions& roptions) {
//  assert(roptions.io_activity == Env::IOActivity::kUnknown);
//  auto r = rep_.get();
//  auto sequence = roptions.snapshot != nullptr
//                      ? roptions.snapshot->GetSequenceNumber()
//                      : kMaxSequenceNumber;
//  auto* res = new ArenaWrappedDBIter();
//  res->Init(r->options.env, roptions, r->ioptions, r->moptions,
//            nullptr /* version */, sequence,
//            r->moptions.max_sequential_skip_in_iterations,
//            0 /* version_number */, nullptr /* read_callback */,
//            nullptr /* db_impl */, nullptr /* cfd */,
//            true /* expose_blob_index */, false /* allow_refresh */);
//
//  // in here, we construct a merge iterator builder
//  MergeIteratorBuilder merge_iter_builder(
//      &r->ioptions.internal_comparator,
//      res->GetArena(),
//      false,
//      roptions.iterate_upper_bound);
//  for(unsigned long i = 0; i < r->table_readers.size(); i++) {
//    // in here, we will need to construct different iterators
//    // for different sst files
//    auto internal_iter = r->table_readers[i]->NewIterator(
//        res->GetReadOptions(), r->moptions.prefix_extractor.get(),
//        res->GetArena(), false /* skip_filters */,
//        TableReaderCaller::kSSTFileReader);
//    // in here, add the iterators to the merge iterator builder
//    merge_iter_builder.AddIterator(internal_iter);
//  }
//  auto final_iter = merge_iter_builder.Finish(
//      roptions.ignore_range_deletions ? nullptr : res);
//
//  res->SetIterUnderDBIter(final_iter);
//  return res;
//}

Iterator* SstFileReader::NewIterator(const ReadOptions& roptions) {
  assert(roptions.io_activity == Env::IOActivity::kUnknown);
  auto r = rep_.get();
  auto sequence = roptions.snapshot != nullptr
                      ? roptions.snapshot->GetSequenceNumber()
                      : kMaxSequenceNumber;
  ArenaWrappedDBIter* res = new ArenaWrappedDBIter();
  res->Init(r->options.env, roptions, r->ioptions, r->moptions,
            nullptr /* version */, sequence,
            r->moptions.max_sequential_skip_in_iterations,
            0 /* version_number */, nullptr /* read_callback */,
            nullptr /* db_impl */, nullptr /* cfd */,
            true /* expose_blob_index */, false /* allow_refresh */);
  auto internal_iter = r->table_reader->NewIterator(
      res->GetReadOptions(), r->moptions.prefix_extractor.get(),
      res->GetArena(), false /* skip_filters */,
      TableReaderCaller::kSSTFileReader);
  res->SetIterUnderDBIter(internal_iter);
  return res;
}

std::shared_ptr<const TableProperties> SstFileReader::GetTableProperties()
    const {
  return rep_->table_reader->GetTableProperties();
}

Status SstFileReader::VerifyChecksum(const ReadOptions& read_options) {
  assert(read_options.io_activity == Env::IOActivity::kUnknown);
  return rep_->table_reader->VerifyChecksum(read_options,
                                            TableReaderCaller::kSSTFileReader);
}

}  // namespace ROCKSDB_NAMESPACE
