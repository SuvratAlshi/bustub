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
  int bucket_bit = bucket_ind % 8;
  int index = bucket_ind / 8;
  int occupied_bit = (occupied_[index] >> bucket_bit) & 0x1;
  int readable_bit = (readable_[index] >> bucket_bit) & 0x1;
  // check that bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE)
  {
    // check if bucket_ind is occupied and readable
    if (occupied_bit == 1 and readable_bit == 1) {
      return array_[bucket_ind].first;
    }
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  int bucket_bit = bucket_ind % 8;
  int index = bucket_ind / 8;
  int occupied_bit = (occupied_[index] >> bucket_bit) & 0x1;
  int readable_bit = (readable_[index] >> bucket_bit) & 0x1;
  // check that bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE)
  {
    // check if bucket_ind is occupied and readable
    if (occupied_bit == 1 and readable_bit == 1) {
      return array_[bucket_ind].second;
    }
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  int bucket_bit = bucket_ind % 8;
  int index = bucket_ind / 8;
  int occupied_bit = (occupied_[index] >> bucket_bit) & 0x1;
  int readable_bit = (readable_[index] >> bucket_bit) & 0x1;
  // check bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE){
    // check bucket_ind is empty and can be occupied
    // either readable and occupied are 0 or readable is 0 and tombstone marked
    if ((occupied_bit == 0 && readable_bit == 0) ||
       (occupied_bit == 1 && readable_bit == 0)){
         array_[bucket_ind].first = key;
         array_[bucket_ind].second = value;
         occupied_[index] |= (1 << bucket_bit);
         readable_[index] |= (1 << bucket_bit);
         return true;
       }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  int bucket_bit = bucket_ind % 8;
  int index = bucket_ind / 8;
  // check bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE) {
    // reset all fields without checking validity of location
    // array_[bucket_ind].first = 0;
    // array_[bucket_ind].second = 0;
    readable_[index] &= ~(1 << bucket_bit);
  }
  return;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  int bucket_bit = bucket_ind % 8;
  int index = bucket_ind / 8;
  int occupied_bit = (occupied_[index] >> bucket_bit) & 0x1;
  // check bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE){
    if (occupied_bit == 1){
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  int bucket_bit = bucket_ind % 8;
  int index = bucket_ind / 8;
  int readable_bit = (readable_[index] >> bucket_bit) & 0x1;
  // check bucket_ind does not exceed limit
  if (bucket_ind < BLOCK_ARRAY_SIZE){
    if (readable_bit == 1){
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
