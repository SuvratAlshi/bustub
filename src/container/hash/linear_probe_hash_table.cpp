//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"
#include "storage/page/hash_table_block_page.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {

      // allocate a header page
      page_id_t header_page_id = INVALID_PAGE_ID;
      auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id, nullptr)->GetData());
      assert(header_page != nullptr);

      // allocate block pages
      page_id_t block_page_id = INVALID_PAGE_ID;
      
      // get size per block page
      int block_pages_to_allocate = num_buckets / BLOCK_ARRAY_SIZE + 1;

      for (int i = 0; i < block_pages_to_allocate; i++) {
        auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->NewPage(&block_page_id, nullptr)->GetData());
        assert(block_page != nullptr);
        header_page->AddBlockPageId(block_page_id);
        buffer_pool_manager_->UnpinPage(block_page_id, true, nullptr);
        buffer_pool_manager_->FlushPage(block_page_id, nullptr);
      }

      // store hash table size in the header page
      header_page->SetSize(num_buckets);
      
      // store header page id
      header_page->SetPageId(header_page_id);      

      // save header page id with hash table
      this->header_page_id_ = header_page_id;

      buffer_pool_manager_->UnpinPage(header_page_id, true, nullptr);
      buffer_pool_manager_->FlushPage(header_page_id, nullptr);
      return;
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  
  this->table_latch_.RLock();

  // get header page
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(this->buffer_pool_manager_->FetchPage(this->header_page_id_)->GetData());
  
  // hash the key
  uint64_t hash_value = this->hash_fn_.GetHash(key);
  
  // get block page id from modulo of total pages
  auto block_page_index = hash_value % header_page->NumBlocks();
  
  // get bucket id from modulo of total buckets within a block page
  int bucket_id = hash_value % BLOCK_ARRAY_SIZE;
  
  // loop to search for empty location, should start and circle back to start
  // do while we do not return to the same location
  auto i = block_page_index;
  int j = bucket_id;

  
  // fetch the block page
  int block_page_id = header_page->GetBlockPageId(i);
  auto page = this->buffer_pool_manager_->FetchPage(block_page_id);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  KeyType storedKey; 

  do {
    page->RLatch();
    storedKey = block_page->KeyAt(j);
    if ((block_page->IsReadable(j)) &&  (this->comparator_(storedKey, key) == 0)){
      result->push_back(block_page->ValueAt(j));
    }
    page->RUnlatch();
    
    j = (j + 1) % BLOCK_ARRAY_SIZE;
    // fetch the corect block page
    if (j == bucket_id) {
      i = (i + 1) % header_page->NumBlocks();
      // unpin the previous page before fetching the next one
      this->buffer_pool_manager_->UnpinPage(block_page_id, false, nullptr);
      block_page_id = header_page->GetBlockPageId(i);
      page = this->buffer_pool_manager_->FetchPage(block_page_id);
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
    }
  } while(i != block_page_index || j != bucket_id);

  this->table_latch_.RUnlock();
  return result->size() > 0;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {

  this->table_latch_.RLock();

  // get header page
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(this->buffer_pool_manager_->FetchPage(this->header_page_id_)->GetData());
  
  // hash the key
  uint64_t hash_value = this->hash_fn_.GetHash(key);
  
  // get block page id from modulo of total pages
  auto block_page_index = hash_value % header_page->NumBlocks();
  
  // get bucket id from modulo of total buckets within a block page
  int bucket_id = hash_value % BLOCK_ARRAY_SIZE;
  
  // loop to search for empty location, should start and circle back to start
  // do while we do not return to the same location
  auto i = block_page_index;
  int j = bucket_id;

  // fetch the block page
  int block_page_id = header_page->GetBlockPageId(i);
  auto page = this->buffer_pool_manager_->FetchPage(block_page_id);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(page->GetData());

  do {
    page->WLatch();
    if (block_page->Insert(j, key, value) == true){
      this->buffer_pool_manager_->UnpinPage(block_page_id, true, nullptr);
      page->WUnlatch();
      this->table_latch_.RUnlock();
      return true;
    }

    // remove writer latch
    page->WUnlatch();

    if ((block_page->IsReadable(j)) &&  (this->comparator_(block_page->KeyAt(j), key) == 0) && (block_page->ValueAt(j) == value)) {
      this->table_latch_.RUnlock();
      return false;
    }

    j = (j + 1) % BLOCK_ARRAY_SIZE;

    // fetch the corect block page
    if (j == bucket_id) {
      i = (i + 1) % header_page->NumBlocks();
      // unpin the previous page before fetching the next one
      this->buffer_pool_manager_->UnpinPage(block_page_id, false, nullptr);
      block_page_id = header_page->GetBlockPageId(i);
      page = this->buffer_pool_manager_->FetchPage(block_page_id);
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
    }
  } while(i != block_page_index || j != bucket_id);

  // remover reader lock
  this->table_latch_.RUnlock();
  this->Resize(this->GetSize());
  this->Insert(transaction, key, value);
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {

  this->table_latch_.RLock();

  // get header page
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(this->buffer_pool_manager_->FetchPage(this->header_page_id_)->GetData());
  
  // hash the key
  uint64_t hash_value = this->hash_fn_.GetHash(key);
  
  // get block page id from modulo of total pages
  auto block_page_index = hash_value % header_page->NumBlocks();
  
  // get bucket id from modulo of total buckets within a block page
  int bucket_id = hash_value % BLOCK_ARRAY_SIZE;
  
  // loop to search for empty location, should start and circle back to start
  // do while we do not return to the same location
  auto i = block_page_index;
  int j = bucket_id;

  // fetch the block page
  int block_page_id = header_page->GetBlockPageId(i);
  auto page = this->buffer_pool_manager_->FetchPage(block_page_id);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(page->GetData());

  do {
    page->WLatch();
    if ((block_page->IsReadable(j)) &&  (this->comparator_(block_page->KeyAt(j), key) == 0) && (block_page->ValueAt(j) == value)) {
      block_page->Remove(j);
      page->WUnlatch();
      this->table_latch_.RUnlock();
      return true;
    }

    page->WUnlatch();
  
    j = (j + 1) % BLOCK_ARRAY_SIZE;
    // fetch the corect block page
    if (j == bucket_id) {
      i = (i + 1) % header_page->NumBlocks();
      block_page_id = header_page->GetBlockPageId(i);
      page = this->buffer_pool_manager_->FetchPage(block_page_id);
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
    }
  } while(i != block_page_index || j != bucket_id);

  this->table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {

  this->table_latch_.WLock();
  // get header page
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(this->buffer_pool_manager_->FetchPage(this->header_page_id_)->GetData());

  size_t num_buckets_old = header_page->GetSize();
  // check if resize is justified
  if (2*initial_size < num_buckets_old) {
    this->table_latch_.WUnlock();
    return;
  }

  header_page->SetSize(2*initial_size);
  
  // total number of blocks
  int total_block_pages_old = header_page->NumBlocks();
  
  
  // copy over old block page ids
  page_id_t *old_block_page = new page_id_t[total_block_pages_old];
  for (int i = 0; i < total_block_pages_old; i++) {
    auto page_id = header_page->GetBlockPageId(i);
    old_block_page[i] = page_id;
  }

  // reset block index for new start
  header_page->ResetBlockIndex();

  // make a new list of block page ids, allocate all new block pages
  // allocate block pages
  page_id_t block_page_id = INVALID_PAGE_ID;
  
  // get size per block page
  int block_pages_to_allocate = (2*initial_size) / BLOCK_ARRAY_SIZE + 1;
  for (int i = 0; i < block_pages_to_allocate; i++) {
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->NewPage(&block_page_id, nullptr)->GetData());
    assert(block_page != nullptr);
    header_page->AddBlockPageId(block_page_id);
    buffer_pool_manager_->UnpinPage(block_page_id, true, nullptr);
    buffer_pool_manager_->FlushPage(block_page_id, nullptr);
  }

  // since new table space is created
  // we can unlock the writer locks
  this->table_latch_.WUnlock();
  // at the same time have a reader lock open ?
  this->table_latch_.RLock();

  // iterate over old block pages, and insert all entries again
  for (int i = 0; i < total_block_pages_old; i++) {
    // fetch old block page
    int block_page_id = old_block_page[i];
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(this->buffer_pool_manager_->FetchPage(block_page_id)->GetData());
    for (unsigned long int j = 0; j < (BLOCK_ARRAY_SIZE); j++) {
      if (block_page->IsReadable(j)) {
        KeyType key = block_page->KeyAt(j);
        ValueType value = block_page->ValueAt(j);
        this->Insert(nullptr, key, value);
      }
    } // end of old block page iteration
    // delete old block page
    this->buffer_pool_manager_->DeletePage(block_page_id, nullptr);
  }

  delete[] old_block_page;
  this->table_latch_.RUnlock();
  return;
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  this->table_latch_.RLock();
  // get header_page
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(this->buffer_pool_manager_->FetchPage(this->header_page_id_));
  int size = header_page->GetSize();
  this->table_latch_.RUnlock();
  return size;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
