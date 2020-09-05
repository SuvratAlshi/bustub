//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  // check that bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE)
  {
    // check if bucket_ind is occupied and readable
    if (occupied_[bucket_ind] == 1 and readable_[bucket_ind] == 1) {
      return array_[bucket_ind].first;
    }
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  // check that bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE)
  {
    // check if bucket_ind is occupied and readable
    if (occupied_[bucket_ind] == 1 and readable_[bucket_ind] == 1) {
      return array_[bucket_ind].second;
    }
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  // check bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE){
    // check bucket_ind is empty and can be occupied
    // either readable and occupied are 0 or readable is 0 and tombstone marked
    if ((occupied_[bucket_ind] == 0 && readable_[bucket_ind] == 0) ||
       (occupied_[bucket_ind] == 1 && readable_[bucket_ind] == 0)){
         array_[bucket_ind].first = key;
         array_[bucket_ind].second = value;
         occupied_[bucket_ind] = 1;
         readable_[bucket_ind] = 1;
         return true;
       }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  // check bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE) {
    // reset all fields without checking validity of location
    // array_[bucket_ind].first = 0;
    // array_[bucket_ind].second = 0;
    occupied_[bucket_ind] = 0;
    readable_[bucket_ind] = 0;
  }
  return;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  // check bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE){
    if (occupied_[bucket_ind] == 1){
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  // check bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE){
    if (readable_[bucket_ind] == 1){
      return true;
    }
  }
  return false;
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
