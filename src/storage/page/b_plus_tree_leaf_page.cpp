//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/*
 * Helper method to find and return the key/Item associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  assert(GetSize() > 0);
  int left = 0;
  int right = GetSize() - 1;
  while (left < right) {  // find the last key in array <= input
    int mid = (right - left) / 2 + left;
    if (comparator(array_[mid].first, key) > 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  int t = comparator(array_[left].first, key);
  if (t > 0) {
    return left - 1;
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType & {
  assert(index >= 0 && index < GetSize());
  return array_[index];
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const
    -> bool {
  int idx = KeyIndex(key, comparator);
  if (idx < GetSize() && comparator(array_[idx].first, key) == 0) {
    *value = array_[idx].second;
    return true;
  }
  return false;
}

/*****************************************************************************
 * Insert
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  // Situation1: The original leaf_page is empty
  if (GetSize() == 0) {
    IncreaseSize(1);
    array_[0].first = key;
    array_[0].second = value;
    return GetSize();
  }

  // Situation2: The original leaf_page has at least one key
  int idx = KeyIndex(key, comparator) + 1;  // first larger than key
  assert(idx >= 0);
  IncreaseSize(1);
  int cur_size = GetSize();
  for (int i = cur_size - 1; i > idx; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[idx].first = key;
  array_[idx].second = value;
  return cur_size;
}

// Split
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  assert(recipient != nullptr);
  int maxsize = GetMaxSize();

  assert(GetSize() == maxsize);
  // copy last half
  int copy_idx = (GetMaxSize() + 1) / 2;
  for (int i = copy_idx; i < maxsize; i++) {
    recipient->array_[i - copy_idx].first = array_[i].first;
    recipient->array_[i - copy_idx].second = array_[i].second;
  }
  // set next_page_id
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
  // set size
  SetSize(copy_idx);
  recipient->SetSize(maxsize - copy_idx);
}

/*****************************************************************************
 * Remove
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int {
  auto idx = KeyIndex(key, comparator);
  // Make sure that the given key is existed in this leaf
  if (idx == GetSize() || comparator(array_[idx].first, key) != 0) {
    return GetSize();
  }
  std::move(array_ + idx + 1, array_ + GetSize(), array_ + idx);
  IncreaseSize(-1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  // std::copy(iterator source_first, iterator source_end, iterator target_start);
  std::copy(items, items + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array_, GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveMiddleTo(BPlusTreeLeafPage *recipient) {
  auto increment = GetSize() - GetMinSize();
  auto deduce = GetMinSize() - GetSize();
  auto orisize = recipient->GetSize();
  recipient->IncreaseSize(increment);
  // 内移 左闭右开
  std::move(recipient->array_, recipient->array_ + orisize, recipient->array_ + increment);
  // 外移
  std::move(array_ + GetMinSize(), array_ + GetSize(), recipient->array_);
  IncreaseSize(deduce);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAheadTo(BPlusTreeLeafPage *recipient) {
  auto increment = GetSize() - GetMinSize();
  auto deduce = GetMinSize() - GetSize();
  recipient->CopyNFrom(array_, increment);
  std::move(array_ + increment, array_ + GetSize(), array_);
  IncreaseSize(deduce);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
