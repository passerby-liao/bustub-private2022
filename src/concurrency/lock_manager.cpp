//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  //  READ_UNCOMMITTED
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  // READ_COMMITTED
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  // REPEATABLE_READ
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    // table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // IS -> [S,X,IX,SIX] NOT NEED CHECK
      // X ->abort
      if (request->lock_mode_ == LockMode::EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      //  S -> [X, SIX]  IX -> [X, SIX]
      if (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
        if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          lock_request_queue->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }
      // SIX -> [X]
      if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        if (lock_mode != LockMode::EXCLUSIVE) {
          lock_request_queue->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }

      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteTableLockSet(txn, request, false);

      auto upgrade_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

      auto it = lock_request_queue->request_queue_.begin();
      for (; it != lock_request_queue->request_queue_.end(); it++) {
        if (!(*it)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(it, upgrade_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(upgrade_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_request->granted_ = true;
      InsertOrDeleteTableLockSet(txn, upgrade_request, true);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  auto insert_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(insert_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(insert_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(insert_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  insert_request->granted_ = true;
  InsertOrDeleteTableLockSet(txn, insert_request, true);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto x_row_lock_set = txn->GetSharedRowLockSet();
  auto s_row_lock_set = txn->GetExclusiveRowLockSet();

  // table id -> set
  // no set    or    has set&& set is empty
  if (!(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty()) ||
      !(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;

  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId() && request->granted_) {
      lock_request_queue->request_queue_.remove(request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           request->lock_mode_ == LockMode::EXCLUSIVE)) {
        {
          if (txn->GetState() == TransactionState::GROWING) {
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      InsertOrDeleteTableLockSet(txn, request, false);
      return true;
    }
  }
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }

  //  READ_UNCOMMITTED
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  // READ_COMMITTED
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  // REPEATABLE_READ
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  // LOG_INFO("enter request loop");
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // IS -> [S,X,IX,SIX] NOT NEED CHECK
      // X ->abort
      if (request->lock_mode_ == LockMode::EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      //  S -> [X, SIX]  IX -> [X, SIX]
      if (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
        if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          lock_request_queue->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }
      // SIX -> [X]
      if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        if (lock_mode != LockMode::EXCLUSIVE) {
          lock_request_queue->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }

      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteRowLockSet(txn, request, false);

      auto upgrade_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

      auto it = lock_request_queue->request_queue_.begin();
      for (; it != lock_request_queue->request_queue_.end(); it++) {
        if (!(*it)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(it, upgrade_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(upgrade_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_request->granted_ = true;
      InsertOrDeleteRowLockSet(txn, upgrade_request, true);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  auto insert_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(insert_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(insert_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(insert_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  insert_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn, insert_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (txn->GetTransactionId() == request->txn_id_ && request->granted_) {
      lock_request_queue->request_queue_.remove(request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           request->lock_mode_ == LockMode::EXCLUSIVE)) {
        {
          if (txn->GetState() == TransactionState::GROWING) {
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      InsertOrDeleteRowLockSet(txn, request, false);
      return true;
    }
  }
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  txn_set_.insert(t1);
  txn_set_.insert(t2);
  waits_for_[t1].emplace_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (it != waits_for_[t1].end()) {
    waits_for_[t1].erase(it);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  for (auto start_id : txn_set_) {
    if (Dfs(start_id)) {
      *txn_id = *active_set_.rbegin();
      active_set_.clear();
      vis_map_.clear();
      return true;
    }
  }
  vis_map_.clear();
  active_set_.clear();
  return false;
}

auto LockManager::Dfs(txn_id_t txn_id) -> bool {
  if (vis_map_.find(txn_id) != vis_map_.end() && vis_map_[txn_id] == 1) {
    return true;
  }
  if (vis_map_.find(txn_id) != vis_map_.end() && vis_map_[txn_id] == -1) {
    return false;
  }
  active_set_.insert(txn_id);
  vis_map_[txn_id] = 1;
  auto &next_vector = waits_for_[txn_id];
  std::sort(next_vector.begin(), next_vector.end());
  for (auto to_tid : next_vector) {
    if (txn_id != to_tid && Dfs(to_tid)) {
      return true;
    }
  }
  vis_map_[txn_id] = -1;
  active_set_.erase(txn_id);
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto const &p : waits_for_) {
    auto t1 = p.first;
    for (auto t2 : p.second) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::DeleteNode(txn_id_t tid) {
  waits_for_.erase(tid);
  for (auto tmp_tid : txn_set_) {
    if (tmp_tid != tid) {
      RemoveEdge(tmp_tid, tid);
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();

      for (auto &table_it : table_lock_map_) {
        auto oid = table_it.first;
        table_it.second->latch_.lock();
        auto request_queue = table_it.second->request_queue_;
        std::unordered_set<txn_id_t> granted_set;
        for (auto const &request : request_queue) {
          if (request->granted_) {
            granted_set.emplace(request->txn_id_);
          } else {
            auto tx1 = request->txn_id_;

            for (auto tx2 : granted_set) {
              map_txn_oid_.emplace(tx1, oid);
              AddEdge(tx1, tx2);
            }
          }
        }
        table_it.second->latch_.unlock();
      }

      // row

      for (auto &row_it : row_lock_map_) {
        auto rid = row_it.first;
        row_it.second->latch_.lock();
        auto request_queue = row_it.second->request_queue_;
        std::unordered_set<txn_id_t> granted_set;
        for (auto const &request : request_queue) {
          if (request->granted_) {
            granted_set.emplace(request->txn_id_);
          } else {
            auto tx1 = request->txn_id_;

            for (auto tx2 : granted_set) {
              map_txn_rid_.emplace(tx1, rid);
              AddEdge(tx1, tx2);
            }
          }
        }
        row_it.second->latch_.unlock();
      }

      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        DeleteNode(txn_id);
        if (map_txn_oid_.count(txn_id) > 0) {
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.lock();
          table_lock_map_[map_txn_oid_[txn_id]]->cv_.notify_all();
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.unlock();
        }

        if (map_txn_rid_.count(txn_id) > 0) {
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.lock();
          row_lock_map_[map_txn_rid_[txn_id]]->cv_.notify_all();
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.unlock();
        }
      }

      waits_for_.clear();
      txn_set_.clear();
      map_txn_oid_.clear();
      map_txn_rid_.clear();
    }
  }
}

void LockManager::InsertOrDeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                             bool insert) {
  switch (lock_request->lock_mode_) {
    case LockMode::EXCLUSIVE:
      if (insert) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;

    case LockMode::INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (insert) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED:
      if (insert) {
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}

void LockManager::InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                           bool insert) {
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  switch (lock_request->lock_mode_) {
    case LockMode::EXCLUSIVE:
      if (insert) {
        InsertRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::SHARED:
      if (insert) {
        InsertRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::INTENTION_SHARED:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (auto const &it : lock_request_queue->request_queue_) {
    if (it->granted_) {
      switch (it->lock_mode_) {
        case LockMode::EXCLUSIVE:
          return false;
          break;

        case LockMode::INTENTION_EXCLUSIVE:
          if (lock_request->lock_mode_ == LockMode::SHARED ||
              lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_SHARED:
          if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED:
          if (lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
              lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lock_request->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    } else if (lock_request.get() != it.get()) {
      return false;
    } else {
      return true;
    }
  }
  return false;
}
}  // namespace bustub
