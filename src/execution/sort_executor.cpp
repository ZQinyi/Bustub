#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_->Init();
  Tuple emit_tuple{};
  RID rid{};

  while (child_->Next(&emit_tuple, &rid)) {
    child_tuples_.push_back(emit_tuple);
  }

  std::sort(
      child_tuples_.begin(), child_tuples_.end(),
      [order_bys = plan_->order_bys_, schema = child_->GetOutputSchema()](const Tuple &tuple_a, const Tuple &tuple_b) {
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
      });

  child_iter_ = child_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_iter_ == child_tuples_.end()) {
    return false;
  }
  *tuple = *child_iter_;
  *rid = tuple->GetRid();
  ++child_iter_;
  return true;
}

}  // namespace bustub
