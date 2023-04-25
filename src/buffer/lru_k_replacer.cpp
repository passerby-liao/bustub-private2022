//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/logger.h"
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (cache_evicnum_ + history_evicnum_ == 0) {
    return false;
  }
  if (history_evicnum_ != 0) {
    for (auto it = history_list_.rbegin(); it != history_list_.rend(); it++) {
      // LOG_INFO("frame id %d:  evictable%d", *it, mp_[*it].evictable_);
      if (mp_[*it].evictable_) {
        *frame_id = *it;
        history_list_.erase(mp_[*it].pos_);
        history_evicnum_--;
        break;
      }
    }
  } else {
    for (auto it = cache_list_.rbegin(); it != cache_list_.rend(); it++) {
      if (mp_[*it].evictable_) {
        *frame_id = *it;
        cache_list_.erase(mp_[*it].pos_);
        cache_evicnum_--;
        break;
      }
    }
  }
  mp_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    return;
  }

  size_t now_count = ++mp_[frame_id].hit_count_;
  if (now_count == 1) {
    history_list_.emplace_front(frame_id);
    ++curr_size_;
    mp_[frame_id].pos_ = history_list_.begin();
  } else if (now_count == k_) {
    if (mp_[frame_id].evictable_) {
      history_evicnum_--;
      cache_evicnum_++;
    }
    cache_list_.push_front(frame_id);
    history_list_.erase(mp_[frame_id].pos_);
    mp_[frame_id].pos_ = cache_list_.begin();
  } else if (now_count > k_) {
    cache_list_.erase(mp_[frame_id].pos_);
    cache_list_.emplace_front(frame_id);
    mp_[frame_id].pos_ = cache_list_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (mp_.find(frame_id) == mp_.end() || frame_id > static_cast<int>(replacer_size_) ||
      mp_[frame_id].evictable_ == set_evictable) {
    return;
  }
  int change;
  if (mp_[frame_id].evictable_) {
    change = -1;
  } else {
    change = 1;
  }
  mp_[frame_id].evictable_ = set_evictable;
  if (mp_[frame_id].hit_count_ >= k_) {
    cache_evicnum_ += change;
  } else {
    history_evicnum_ += change;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (mp_.find(frame_id) == mp_.end() || frame_id > static_cast<int>(replacer_size_)) {
    return;
  }
  // can not evict  exception
  if (!mp_[frame_id].evictable_) {
    throw std::exception();
  }

  if (mp_[frame_id].hit_count_ < k_) {
    history_evicnum_--;
    history_list_.erase(mp_[frame_id].pos_);
  } else {
    cache_evicnum_--;
    cache_list_.erase(mp_[frame_id].pos_);
  }
  mp_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return history_evicnum_ + cache_evicnum_; }

}  // namespace bustub
