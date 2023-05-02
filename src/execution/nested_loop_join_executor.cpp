//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID left_rid{};
  // rti == -1 -> left_executor_ to get new left tuple
  // find matched right tuple -> rti >= 0 return;
  // rti >= 0, do not pull new left tuple，if find matched right tuple then repeat
  // do not find the duplicated -> rti = -1;
  while (right_tuple_idx_ >= 0 || left_executor_->Next(&left_tuple_, &left_rid)) {
    std::vector<Value> val{};
    for (uint32_t index = (right_tuple_idx_ < 0 ? 0 : right_tuple_idx_); index < right_tuples_.size(); index++) {
      if (Matched(&left_tuple_, &right_tuples_[index])) {
        // 加入此match tuple对应的所有列
        for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          val.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          val.push_back(right_tuples_[index].GetValue(&right_executor_->GetOutputSchema(), idx));
        }
        *tuple = Tuple(val, &GetOutputSchema());
        right_tuple_idx_ = index + 1;
        return true;
      }
    }
    // 无right tuple与当前left tuple匹配 采用left join
    if (right_tuple_idx_ == -1 && plan_->GetJoinType() == JoinType::LEFT) {
      for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        val.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        val.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
      }
      *tuple = Tuple(val, &GetOutputSchema());
      return true;
    }
    right_tuple_idx_ = -1;
  }
  return false;
}

auto NestedLoopJoinExecutor::Matched(Tuple *left_tuple, Tuple *right_tuple) const -> bool {
  auto result = plan_->predicate_->EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                                                right_executor_->GetOutputSchema());
  return !result.IsNull() && result.GetAs<bool>();
}

}  // namespace bustub
