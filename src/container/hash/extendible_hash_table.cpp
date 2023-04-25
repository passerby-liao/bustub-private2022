//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include "common/logger.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  auto bucket = std::make_shared<Bucket>(bucket_size_, 0);
  dir_.push_back(bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;  // key取hash  取最低global位的数
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // LOG_INFO("extendible  find  %d",(*key))
  std::scoped_lock<std::mutex> lock(latch_);
  auto dir_index = IndexOf(key);
  auto res_bucket = dir_[dir_index];
  return res_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto dir_index = IndexOf(key);
  auto res_bucket = dir_[dir_index];
  return res_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto dir_index = IndexOf(key);

  auto res_bucket = dir_[dir_index];
  if (res_bucket->Insert(key, value)) {
    return;
  }
  // LOG_INFO("# dir_index: %ld", dir_index);
  // LOG_INFO("# GetGlobalDepth: %d ,GetLocalDepth: %d ", GetGlobalDepth() ,GetLocalDepth(dir_index));
  while (dir_[dir_index]->IsFull()) {
    int local_depth = dir_[dir_index]->GetDepth();
    if (local_depth == global_depth_) {
      ++global_depth_;
      int length = dir_.size();
      // vector is not thread safe
      dir_.resize(length << 1);
      for (int i = 0; i < length; i++) {
        dir_[i + length] = dir_[i];
      }
    }
    int mask = 1 << GetLocalDepthInternal(dir_index);
    num_buckets_++;
    auto bucket0 = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(dir_index) + 1);
    auto bucket1 = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(dir_index) + 1);

    for (auto &[k, v] : res_bucket->GetItems()) {
      auto hashkey = std::hash<K>()(k);
      if ((hashkey & mask) != 0) {
        bucket1->Insert(k, v);
      } else {
        bucket0->Insert(k, v);
      }
    }
    // 从前一位的大小开始 从变化位往后+ 所以+mask
    for (size_t i = dir_index & (mask - 1); i < dir_.size(); i += mask) {
      if ((i & mask) != 0) {
        dir_[i] = bucket1;
      } else {
        dir_[i] = bucket0;
      }
    }
    dir_index = IndexOf(key);
  }
  dir_[dir_index]->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // LOG_INFO("bucket find lst key:%d",key);
  for (auto &[k, v] : list_) {
    // LOG_INFO("lst:%d",k);
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if ((*it).first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if ((*it).first == key) {
      it->second = value;
      return true;
    }
  }
  if (this->IsFull()) {
    return false;
  }
  // LOG_INFO("insert success");
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
