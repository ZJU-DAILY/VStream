//
// Created by shb on 23-9-12.
//

#pragma once

#include "../vectorbackend/vectorbackend_namespace.h"
#include "db/version_edit.h"

namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE {
class VectorSearchFilePicker {
 public:
  VectorSearchFilePicker(autovector<LevelFilesBrief>* file_levels,
                         unsigned int num_levels)
      : num_levels_(num_levels),
        curr_level_(static_cast<unsigned int>(-1)),
        level_files_brief_(file_levels) {
    search_ended_ = !PrepareNextLevel();
  }

  void PrepareNextLevelForSearch() { search_ended_ = !PrepareNextLevel(); }

  ROCKSDB_NAMESPACE::FileMetaData* GetNextFile() {
    while (!search_ended_) {  // Loops over different levels.
      if (curr_index_in_curr_level_ < curr_file_level_->num_files) {
        return curr_file_level_->files[curr_index_in_curr_level_++]
            .file_metadata;
      }
      // Start searching next level.
      search_ended_ = !PrepareNextLevel();
    }
    return nullptr;
  }

  bool IsSearchEnded() { return search_ended_; }

  // getter for current file level
  // for GET_HIT_L0, GET_HIT_L1 & GET_HIT_L2_AND_UP counts
  unsigned int GetHitFileLevel() { return curr_level_; }

  unsigned int GetNumFilesInCurrentLevel() {
    if (curr_file_level_ == nullptr) {
      return 0;
    }
    return curr_file_level_->num_files;
  }

  void Reset() {
    curr_level_ = static_cast<unsigned int>(-1);
    curr_index_in_curr_level_ = 0;
    search_ended_ = !PrepareNextLevel();
  }

 private:
  unsigned int num_levels_;
  unsigned int curr_level_;
  autovector<LevelFilesBrief>* level_files_brief_;
  unsigned int curr_index_in_curr_level_{0};
  bool search_ended_;
  LevelFilesBrief* curr_file_level_{nullptr};

  // Setup local variables to search next level.
  // Returns false if there are no more levels to search.
  bool PrepareNextLevel() {
    curr_level_++;
    while (curr_level_ < num_levels_) {
      curr_file_level_ = &(*level_files_brief_)[curr_level_];
      if (curr_file_level_->num_files == 0) {
        curr_level_++;
        continue;
      }
      curr_index_in_curr_level_ = 0;
      return true;
    }
    return false;
  }
};
}  // namespace ROCKSDB_NAMESPACE::VECTORBACKEND_NAMESPACE