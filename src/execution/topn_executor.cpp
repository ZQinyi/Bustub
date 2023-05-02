#include "execution/executors/topn_executor.h"
#include <cstddef>
#include <queue>
#include <vector>
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_->Init();

  auto cmp = [order_bys = plan_->order_bys_, schema = child_->GetOutputSchema()](const Tuple &tuple_a,
                                                                                 const Tuple &tuple_b) {
    for (const auto &order_by : order_bys) {
      switch (order_by.first) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (static_cast<bool>(order_by.second->Evaluate(&tuple_a, schema)
                                    .CompareLessThan(order_by.second->Evaluate(&tuple_b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_by.second->Evaluate(&tuple_a, schema)
                                           .CompareGreaterThan(order_by.second->Evaluate(&tuple_b, schema)))) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (static_cast<bool>(order_by.second->Evaluate(&tuple_a, schema)
                                    .CompareGreaterThan(order_by.second->Evaluate(&tuple_b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_by.second->Evaluate(&tuple_a, schema)
                                           .CompareLessThan(order_by.second->Evaluate(&tuple_b, schema)))) {
            return false;
          }
          break;
      }
    }
    return false;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
  Tuple emit_tuple{};
  RID rid;

  while (child_->Next(&emit_tuple, &rid)) {
    pq.push(emit_tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }

  while (!pq.empty()) {
    child_tuples_.push(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!child_tuples_.empty()) {
    *tuple = child_tuples_.top();
    *rid = tuple->GetRid();
    child_tuples_.pop();
    return true;
  }
  return false;
}

}  // namespace bustub
