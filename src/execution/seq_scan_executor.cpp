//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // ask plan_ for the table_id
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid());
}

void SeqScanExecutor::Init() { table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_ == table_info_->table_->End()) {
    return false;
  }
  *rid = table_iter_->GetRid();
  *tuple = *table_iter_;
  ++table_iter_;
  return true;
}

}  // namespace bustub
