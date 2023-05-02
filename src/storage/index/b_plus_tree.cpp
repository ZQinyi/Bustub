#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "common/rwlatch.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
/*****************************************************************************
 * INITIALIZATION
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * Get the target leaf page
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Operation operation, Transaction *transaction, bool leftMost,
                                  bool rightMost) -> Page * {
  assert(root_page_id_ != INVALID_PAGE_ID);
  if (IsEmpty()) {
    return nullptr;
  }
  // 获取指向root_page的指针
  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  // 将page指针映射到B+树
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (operation == Operation::SEARCH) {
    // 读锁 锁住child page时 即解锁其parent锁
    root_page_id_latch_.RUnlock();
    page->RLatch();
  } else {
    // 插入/删除上写锁
    page->WLatch();
    if (operation == Operation::DELETE && node->GetSize() > 2) {
      ReleaseAllAncestor(transaction);
    }
    if (operation == Operation::INSERT && node->IsLeafPage() && node->GetSize() < node->GetMaxSize() - 1) {
      ReleaseAllAncestor(transaction);
    }
    if (operation == Operation::INSERT && !node->IsLeafPage() && node->GetSize() < node->GetMaxSize()) {
      ReleaseAllAncestor(transaction);
    }
  }
  while (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_node_id;
    if (leftMost) {
      child_node_id = internal_node->ValueAt(0);
    } else if (rightMost) {
      child_node_id = internal_node->ValueAt(internal_node->GetSize() - 1);
    } else {
      child_node_id = internal_node->Lookup(key, comparator_);
    }
    assert(child_node_id > 0);
    auto child_page = buffer_pool_manager_->FetchPage(child_node_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if (operation == Operation::SEARCH) {
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else if (operation == Operation::INSERT) {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);

      // Child node is safe, release all locks on ancestors
      if (child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize() - 1) {
        ReleaseAllAncestor(transaction);
      }
      if (!child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize()) {
        ReleaseAllAncestor(transaction);
      }
    } else if (operation == Operation::DELETE) {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);

      // child node is safe, release all locks on ancestors
      if (child_node->GetSize() > child_node->GetMinSize()) {
        ReleaseAllAncestor(transaction);
      }
    }
    page = child_page;
    node = child_node;
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllAncestor(Transaction *transaction) {
  while (!transaction->GetPageSet()->empty()) {
    auto page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      this->root_page_id_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // One of the corner case is that when insert and delete, the member variable root_page_id will also be updated.
  // It is your responsibility to protect from concurrent update of this shared variable
  // hint: add an abstract layer in B+ tree index, use std::mutex to protect this variable
  root_page_id_latch_.RLock();

  // 返回指向leaf_page的指针
  auto *leaf_page = FindLeafPage(key, Operation::SEARCH, transaction, false, false);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  ValueType v;
  // look_up函数给V赋值 并返回true；else返回false
  bool if_existed = leaf_node->Lookup(key, &v, comparator_);

  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

  if (if_existed) {
    (*result).push_back(v);
  }
  return if_existed;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_page_id_latch_.WLock();
  // nullptr代表root_page_id_latch
  transaction->AddIntoPageSet(nullptr);
  if (IsEmpty()) {
    StartNewTree(key, value);
    ReleaseAllAncestor(transaction);
    return true;
  }
  return InsertToLeaf(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto *newpage = buffer_pool_manager_->NewPage(&root_page_id_);
  assert(newpage != nullptr);
  auto *rootpage = reinterpret_cast<BPlusTreePage *>(newpage->GetData());
  auto *root_page = reinterpret_cast<LeafPage *>(rootpage);
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  // 此时rootpage也为leafpage
  root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(newpage->GetPageId(), true);
  UpdateRootPageId(1);
}

// 内嵌InsertToInternal 1.更新leaf 2.更新internal
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertToLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  auto *leaf_page = FindLeafPage(key, Operation::INSERT, transaction, false, false);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  ValueType v;
  bool if_existed = leaf_node->Lookup(key, &v, comparator_);

  if (if_existed) {
    ReleaseAllAncestor(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  leaf_node->Insert(key, value, comparator_);  // ++size

  // Not full
  if (leaf_node->GetSize() < leaf_max_size_) {
    ReleaseAllAncestor(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  // Full need split
  LeafPage *new_leaf_page = SplitLeaf(leaf_node);
  // split初始化==new_leaf_page->SetNextPageId(INVALID_PAGE_ID);
  leaf_node->SetNextPageId(new_leaf_page->GetPageId());
  InsertIntoParent(leaf_node, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
  // ReleaseAllAncestor(transaction);
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // old_node为root节点时 创建一个new_page去作为新的root
  if (old_node->IsRootPage()) {
    // new了page一定要init！！
    auto *page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(page != nullptr);
    auto *newroot = reinterpret_cast<BPlusTreePage *>(page->GetData());
    auto *new_root = reinterpret_cast<InternalPage *>(newroot);
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    // 更新root值
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    UpdateRootPageId(0);
    ReleaseAllAncestor(transaction);
    return;
  }

  // 接下来处理普通internal_page
  // get original parent_id
  page_id_t old_parent_id = old_node->GetParentPageId();
  auto *page = buffer_pool_manager_->FetchPage(old_parent_id);
  assert(page != nullptr);
  auto *parentpage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  auto *parent_page = reinterpret_cast<InternalPage *>(parentpage);

  new_node->SetParentPageId(old_parent_id);

  // insert key
  if (parent_page->GetSize() < internal_max_size_) {
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    ReleaseAllAncestor(transaction);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return;
  }
  auto *mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_page->GetSize() + 1)];
  auto *copy_parent_node = reinterpret_cast<InternalPage *>(mem);
  std::memcpy(mem, page->GetData(), INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_page->GetSize()));
  copy_parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // 这里犯错了！因为先开辟了maxsize+1的内存空间，再插入新key. 所以分裂的时候maxsize = GetMaxSize()+1;
  auto parent_sibling_node = SplitInternal(copy_parent_node);
  KeyType new_key = parent_sibling_node->KeyAt(0);
  // memcpy
  std::memcpy(page->GetData(), mem, INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * copy_parent_node->GetMinSize());
  InsertIntoParent(parent_page, new_key, parent_sibling_node, transaction);
  buffer_pool_manager_->UnpinPage(parentpage->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_sibling_node->GetPageId(), true);
  delete[] mem;
}

/*******
 * SPLIT
 *******/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *node) -> LeafPage * {
  // step 1 ask for new page from buffer pool manager
  page_id_t new_page_id;
  auto *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  assert(new_page != nullptr);
  // step 2 move half of key & value pairs from input page to newly created page
  auto newnode = reinterpret_cast<BPlusTreePage *>(new_page->GetData());
  auto *new_node = reinterpret_cast<LeafPage *>(newnode);
  new_node->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
  node->MoveHalfTo(new_node);
  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *node) -> InternalPage * {
  // step 1 ask for new page from buffer pool manager
  page_id_t new_page_id;
  auto *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  assert(new_page != nullptr);
  // step 2 move half of key & value pairs from input page to newly created page
  auto newnode = reinterpret_cast<BPlusTreePage *>(new_page->GetData());
  auto *new_node = reinterpret_cast<InternalPage *>(newnode);
  new_node->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_id_latch_.WLock();
  // nullptr代表root_page_id_latch
  transaction->AddIntoPageSet(nullptr);

  // Tree is empty
  if (root_page_id_ == INVALID_PAGE_ID) {
    ReleaseAllAncestor(transaction);
    return;
  }
  // Find the leaf page that contains the key
  auto *de_leaf = FindLeafPage(key, Operation::DELETE, transaction, false, false);
  auto *deleaf = reinterpret_cast<LeafPage *>(de_leaf->GetData());
  bool isfirst = (comparator_(deleaf->KeyAt(0), key) == 0);
  // 1. The key is not existed
  auto pastsize = deleaf->GetSize();
  auto nowsize = deleaf->RemoveAndDeleteRecord(key, comparator_);
  if (pastsize == nowsize) {
    ReleaseAllAncestor(transaction);
    de_leaf->WUnlatch();
    buffer_pool_manager_->UnpinPage(de_leaf->GetPageId(), false);
    return;
  }
  // 2. The key is existed
  // 特殊情况: 删除的leaf key是0号位，而且其在parent page的idx不为0，那么应该去更新internal_page对应的idx key值.
  if (!deleaf->IsRootPage() && isfirst) {
    auto ppage =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(deleaf->GetParentPageId())->GetData());
    auto p_page = reinterpret_cast<InternalPage *>(ppage);
    auto idx = p_page->ValueIndex(deleaf->GetPageId());
    if (idx != 0) {
      p_page->SetKeyAt(idx, deleaf->KeyAt(0));
    }
    buffer_pool_manager_->UnpinPage(ppage->GetPageId(), true);
  }

  auto if_deleted = CoalesceOrRedistribute(deleaf, transaction);
  // 如果de_leaf被删除 需要清空其在bpm里的记录
  if (if_deleted) {
    transaction->AddIntoDeletedPageSet(deleaf->GetPageId());
  }

  de_leaf->WUnlatch();
  buffer_pool_manager_->UnpinPage(de_leaf->GetPageId(), true);

  for (int it : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(it);
  }
  transaction->GetDeletedPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
  // Root page is not need to considerate Coalesce/Redistribute
  if (node->IsRootPage()) {
    // 1. Root page has only one child page, so its child page turns to be the new root page
    if (!node->IsLeafPage() && node->GetSize() <= 1) {
      auto root_node = reinterpret_cast<InternalPage *>(node);
      auto only_child_page = buffer_pool_manager_->FetchPage(root_node->ValueAt(0));
      auto *only_child_node = reinterpret_cast<BPlusTreePage *>(only_child_page->GetData());
      only_child_node->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = only_child_node->GetPageId();
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(only_child_page->GetPageId(), true);
      ReleaseAllAncestor(transaction);
      return true;
    }
    // 2. The tree is empty now
    if (node->IsLeafPage() && node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      ReleaseAllAncestor(transaction);
      return true;
    }
    ReleaseAllAncestor(transaction);
    return false;
  }

  // 未触发合并或重分配条件
  if (node->GetSize() >= node->GetMinSize()) {
    ReleaseAllAncestor(transaction);
    return false;
  }

  auto ppage = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  auto parent_page = reinterpret_cast<InternalPage *>(ppage);
  auto idx = parent_page->ValueIndex(node->GetPageId());

  assert(idx < parent_page->GetSize() && idx >= 0);
  assert(parent_page->GetSize() >= 2);
  if (idx > 0) {
    auto lnode = buffer_pool_manager_->FetchPage(parent_page->ValueAt(idx - 1));
    lnode->WLatch();
    auto leftnode = reinterpret_cast<BPlusTreePage *>(lnode->GetData());
    N *left_node = reinterpret_cast<N *>(leftnode);

    // Redistribute
    if (left_node->GetSize() > left_node->GetMinSize()) {
      Redistribute(left_node, node, parent_page, idx, true);
      ReleaseAllAncestor(transaction);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      lnode->WUnlatch();
      buffer_pool_manager_->UnpinPage(leftnode->GetPageId(), true);
      return false;
    }

    // Coalesce with left page
    assert(left_node->GetSize() == left_node->GetMinSize());
    auto if_parent_deleted = Coalesce(left_node, node, parent_page, idx, transaction);
    if (if_parent_deleted) {
      transaction->AddIntoDeletedPageSet(parent_page->GetPageId());
    }

    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    lnode->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_node->GetPageId(), true);
    return true;
  }

  // idx == 0
  auto rnode = buffer_pool_manager_->FetchPage(parent_page->ValueAt(idx + 1));
  rnode->WLatch();
  auto rightnode = reinterpret_cast<BPlusTreePage *>(rnode->GetData());
  N *right_node = reinterpret_cast<N *>(rightnode);

  // Redistribute
  if (right_node->GetSize() > right_node->GetMinSize()) {
    Redistribute(right_node, node, parent_page, idx + 1, false);
    ReleaseAllAncestor(transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    rnode->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_node->GetPageId(), true);
    return false;
  }

  // Coalesce with right page, Size == MinSize
  assert(right_node->GetSize() == right_node->GetMinSize());
  transaction->AddIntoDeletedPageSet(right_node->GetPageId());
  auto if_parent_deleted = Coalesce(node, right_node, parent_page, idx + 1, transaction);
  if (if_parent_deleted) {
    transaction->AddIntoDeletedPageSet(parent_page->GetPageId());
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  rnode->WUnlatch();
  buffer_pool_manager_->UnpinPage(right_node->GetPageId(), true);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node, InternalPage *parent, int index, Transaction *transaction)
    -> bool {
  auto middle_key = parent->KeyAt(index);

  if (node->IsLeafPage()) {
    auto *nnode = reinterpret_cast<LeafPage *>(node);
    auto *sibling_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    nnode->MoveAllTo(sibling_leaf_node);
  } else {
    auto *nnode = reinterpret_cast<InternalPage *>(node);
    auto *sibling_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    nnode->MoveAllTo(sibling_internal_node, middle_key, buffer_pool_manager_);
  }
  // Remove the page record that be merged
  parent->Remove(index);
  // Recursion
  return CoalesceOrRedistribute(parent, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, InternalPage *parent, int index, bool from_prev) {
  if (node->IsLeafPage()) {
    auto *nnode = reinterpret_cast<LeafPage *>(node);
    auto *sibling_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (from_prev) {
      sibling_leaf_node->MoveMiddleTo(nnode);
      parent->SetKeyAt(index, nnode->KeyAt(0));
    }
    sibling_leaf_node->MoveAheadTo(nnode);
    parent->SetKeyAt(index, sibling_leaf_node->KeyAt(0));
  } else {
    auto *nnode = reinterpret_cast<InternalPage *>(node);
    auto *sibling_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (from_prev) {
      sibling_internal_node->MoveMiddleTo(nnode, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, nnode->KeyAt(0));
    }
    sibling_internal_node->MoveAheadTo(nnode, parent->KeyAt(index), buffer_pool_manager_);
    parent->SetKeyAt(index, sibling_internal_node->KeyAt(0));
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto leftmost_page = FindLeafPage(KeyType(), Operation::SEARCH, nullptr, true);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leftmost_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto leaf_page = FindLeafPage(key, Operation::SEARCH, nullptr);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  auto idx = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  auto rightmost_page = FindLeafPage(KeyType(), Operation::SEARCH, nullptr, false, true);
  auto *leaf_node = reinterpret_cast<LeafPage *>(rightmost_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, rightmost_page, leaf_node->GetSize());
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  root_page_id_latch_.RLock();
  root_page_id_latch_.RUnlock();
  return root_page_id_;
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
