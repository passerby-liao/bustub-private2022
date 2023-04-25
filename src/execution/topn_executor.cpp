#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_->Init();
  auto cmp = [order_bys = plan_->GetOrderBy(), schema = child_->GetOutputSchema()](const Tuple &tuple_a,
                                                                                   const Tuple &tuple_b) {
    for (const auto &order_key : order_bys) {
      switch (order_key.first) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                    .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                           .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                    .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                           .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
            return false;
          }
          break;
      }
    }
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);

  Tuple tuple{};
  RID rid{};
  while (child_->Next(&tuple, &rid)) {
    pq.push(tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }
  while (!pq.empty()) {
    res_tuples_.push(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (res_tuples_.empty()) {
    return false;
  }
  *tuple = res_tuples_.top();
  *rid = tuple->GetRid();
  res_tuples_.pop();
  return true;
}

}  // namespace bustub
