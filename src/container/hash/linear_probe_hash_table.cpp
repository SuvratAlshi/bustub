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

      // allocate block pages
      // get size per block page
      int block_pages_to_allocate = num_buckets / BLOCK_ARRAY_SIZE;
      for (int i = 0; i < block_pages_to_allocate; i++) {
        header_page->AddBlockPageId(i);
      }

      // store hash table size in the header page
      header_page->SetSize(num_buckets * BLOCK_ARRAY_SIZE);
      // store header page id
      header_page->SetPageId(header_page_id);      

      // save header page id with hash table
      this->header_page_id_ = header_page_id;

      return;
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
    // get hash_table size
  int table_size = this->GetSize();
  int total_buckets = table_size / BLOCK_ARRAY_SIZE;  

  // get header page
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(this->buffer_pool_manager_->FetchPage(this->header_page_id_)->GetData());
  // hash the key
  uint64_t hash_value = this->hash_fn_.GetHash(key);
  // std::cout <<"HashTable GetValue, (key, hash) :: (" << key << ", " << hash_value << ")" << std::endl;
  // get block page id from modulo of total pages
  int block_page_index = hash_value % total_buckets;
  // get bucket id from modulo of total buckets within a block page
  int bucket_id = hash_value % BLOCK_ARRAY_SIZE;
  // loop to search for empty location, should start and circle back to start
  // do while we do not return to the same location
  int i = block_page_index;
  int j = bucket_id;

  // std::cout << "HashTable GetValue, initial (i, j) :: (" << i << ", " << j << ")" << std::endl;

  // fetch the block page
  int block_page_id = header_page->GetBlockPageId(i);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(this->buffer_pool_manager_->FetchPage(block_page_id)->GetData());
  KeyType storedKey; 

  do {
    storedKey = block_page->KeyAt(j);
    if ((block_page->IsReadable(j)) &&  (this->comparator_(storedKey, key) == 0)){
      // std::cout << "Got a key match for (key, storedKey) :: (" << key << ", " << storedKey << ")" << std::endl;
      result->push_back(block_page->ValueAt(j));
    }
    i = (i + 1) % total_buckets;
    j = (j + 1) % BLOCK_ARRAY_SIZE;
    // fetch the corect block page
    if (j == 0) {
      block_page_id = header_page->GetBlockPageId(i);
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(this->buffer_pool_manager_->FetchPage(block_page_id)->GetData());
    }
  } while(i != block_page_id && j != bucket_id);
  return result->size() > 0;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // get hash_table size
  int table_size = this->GetSize();
  int total_buckets = table_size / BLOCK_ARRAY_SIZE;  

  // get header page
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(this->buffer_pool_manager_->FetchPage(this->header_page_id_)->GetData());
  // hash the key
  uint64_t hash_value = this->hash_fn_.GetHash(key);
  // std::cout <<"HashTable Insert, (key, hash) :: (" << key << ", " << hash_value << ")" << std::endl;
  // get block page id from modulo of total pages
  int block_page_index = hash_value % total_buckets;
  // get bucket id from modulo of total buckets within a block page
  int bucket_id = hash_value % BLOCK_ARRAY_SIZE;
  // loop to search for empty location, should start and circle back to start
  // do while we do not return to the same location
  int i = block_page_index;
  int j = bucket_id;

  // std::cout << "HashTable Insert, initial (i, j) :: (" << i << ", " << j << ")" << std::endl;

  // fetch the block page
  int block_page_id = header_page->GetBlockPageId(i);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(this->buffer_pool_manager_->FetchPage(block_page_id)->GetData());

  do {
    if (block_page->Insert(j, key, value) == true){
      std::cout << "HashTable Insert, (j, key, value) : (" << j << ", " << key << ", " << value << ")" << std::endl; 
      return true;
    } if ((block_page->IsReadable(j)) &&  (this->comparator_(block_page->KeyAt(j), key) == 0) && (block_page->ValueAt(j) == value)) {
      // TODO: value comparator needs to be revisited
      std::cout << "Duplicate value found, should break" << std::endl; 
      break;
    }
    i = (i + 1) % total_buckets;
    j = (j + 1) % BLOCK_ARRAY_SIZE;
    // fetch the corect block page
    if (j == 0) {
      block_page_id = header_page->GetBlockPageId(i);
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(this->buffer_pool_manager_->FetchPage(block_page_id)->GetData());
    }
  } while(i != block_page_id && j != bucket_id);
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // get hash_table size
  int table_size = this->GetSize();
  int total_buckets = table_size / BLOCK_ARRAY_SIZE;  

  // get header page
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(this->buffer_pool_manager_->FetchPage(this->header_page_id_)->GetData());
  // hash the key
  uint64_t hash_value = this->hash_fn_.GetHash(key);
  // std::cout <<"HashTable Insert, (key, hash) :: (" << key << ", " << hash_value << ")" << std::endl;
  // get block page id from modulo of total pages
  int block_page_index = hash_value % total_buckets;
  // get bucket id from modulo of total buckets within a block page
  int bucket_id = hash_value % BLOCK_ARRAY_SIZE;
  // loop to search for empty location, should start and circle back to start
  // do while we do not return to the same location
  int i = block_page_index;
  int j = bucket_id;

  // std::cout << "HashTable Insert, initial (i, j) :: (" << i << ", " << j << ")" << std::endl;

  // fetch the block page
  int block_page_id = header_page->GetBlockPageId(i);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(this->buffer_pool_manager_->FetchPage(block_page_id)->GetData());

  do {
    if ((block_page->IsReadable(j)) &&  (this->comparator_(block_page->KeyAt(j), key) == 0) && (block_page->ValueAt(j) == value)) {
      // TODO: value comparator needs to be revisited
      block_page->Remove(j);
      return true;
    }
    i = (i + 1) % total_buckets;
    j = (j + 1) % BLOCK_ARRAY_SIZE;
    // fetch the corect block page
    if (j == 0) {
      block_page_id = header_page->GetBlockPageId(i);
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(this->buffer_pool_manager_->FetchPage(block_page_id)->GetData());
    }
  } while(i != block_page_id && j != bucket_id);
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  // get header_page
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(this->buffer_pool_manager_->FetchPage(this->header_page_id_));
  return header_page->GetSize();
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
