//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
// page id是传出参数
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t free_frame;
  if (GetAvailableFrame(&free_frame)) {
    *page_id = AllocatePage();
    page_table_->Insert(*page_id, free_frame);
    pages_[free_frame].ResetMemory();
    pages_[free_frame].is_dirty_ = false;
    pages_[free_frame].page_id_ = *page_id;
    pages_[free_frame].pin_count_ = 1;
    replacer_->SetEvictable(free_frame, false);
    replacer_->RecordAccess(free_frame);
    return &pages_[free_frame];
  }

  return nullptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t free_frame;
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  if (page_table_->Find(page_id, free_frame)) {
    pages_[free_frame].pin_count_++;

    replacer_->RecordAccess(free_frame);
    replacer_->SetEvictable(free_frame, false);  // alread has  also SetEvictable
    return &pages_[free_frame];
  }
  if (!GetAvailableFrame(&free_frame)) {
    return nullptr;
  }

  page_table_->Insert(page_id, free_frame);
  pages_[free_frame].is_dirty_ = false;
  pages_[free_frame].page_id_ = page_id;
  pages_[free_frame].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[free_frame].data_);

  replacer_->SetEvictable(free_frame, false);
  replacer_->RecordAccess(free_frame);

  return &pages_[free_frame];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame;
  // LOG_INFO("UnpinPgImp  page_id   %d", page_id);
  if (page_id != INVALID_PAGE_ID && page_table_->Find(page_id, frame)) {
    if (pages_[frame].pin_count_ == 0) {
      return false;
    }
    pages_[frame].pin_count_--;
    if (pages_[frame].pin_count_ == 0) {
      replacer_->SetEvictable(frame, true);
    }
    pages_[frame].is_dirty_ |= is_dirty;
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  std::vector<bool> use_frame(pool_size_, true);
  // disk_manager_->WritePage(page_id,pages_;
  frame_id_t frame;
  if (page_id != INVALID_PAGE_ID && page_table_->Find(page_id, frame)) {
    disk_manager_->WritePage(pages_[frame].page_id_, pages_[frame].data_);
    pages_[frame].is_dirty_ = false;
    return true;
  }

  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::vector<bool> use_frame(pool_size_, true);
  for (auto it : free_list_) {
    use_frame[static_cast<int>(it)] = false;
  }
  for (int i = 0; i < static_cast<int>(pool_size_); i++) {
    if (use_frame[i]) {
      if (!FlushPgImp(pages_[i].page_id_)) {
        throw std::invalid_argument(std::string("std::fliush error") + std::to_string(pages_[i].page_id_));
      }
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame;

  if (!page_table_->Find(page_id, frame)) {
    return true;
  }
  if (pages_[frame].pin_count_ > 0) {
    return false;
  }
  replacer_->Remove(frame);
  page_table_->Remove(page_id);  // page table  page_id ->  frame    remove page id
  free_list_.emplace_back(frame);

  pages_[frame].ResetMemory();
  pages_[frame].is_dirty_ = false;
  pages_[frame].page_id_ = INVALID_PAGE_ID;
  pages_[frame].pin_count_ = 0;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  // std::scoped_lock<std::mutex> lock(latch_);
  return next_page_id_++;
}

auto BufferPoolManagerInstance::GetAvailableFrame(frame_id_t *out_frame_id) -> bool {
  frame_id_t fid;

  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    *out_frame_id = fid;
    return true;
  }
  if (replacer_->Evict(&fid)) {
    if (pages_[fid].is_dirty_) {
      disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
      pages_[fid].is_dirty_ = false;
    }
    page_table_->Remove(pages_[fid].page_id_);
    *out_frame_id = fid;
    return true;
  }
  return false;
}

}  // namespace bustub
