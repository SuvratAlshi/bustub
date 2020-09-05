//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_header_page.h"

namespace bustub {
page_id_t HashTableHeaderPage::GetBlockPageId(size_t index) { 
    // check that index is less than the next_ind_
    if (index < next_ind_) {
        return block_page_ids_[index];
    }
    return 0; 
}

page_id_t HashTableHeaderPage::GetPageId() const { 
    return page_id_; 
}

void HashTableHeaderPage::SetPageId(bustub::page_id_t page_id) {
    page_id_ = page_id;
    return;
}

lsn_t HashTableHeaderPage::GetLSN() const { 
    return lsn_; 
}

void HashTableHeaderPage::SetLSN(lsn_t lsn) {
    lsn_ = lsn;
    return;
}

void HashTableHeaderPage::AddBlockPageId(page_id_t page_id) {
    block_page_ids_[next_ind_] = page_id;
    next_ind_ += 1;
    return;
}

size_t HashTableHeaderPage::NumBlocks() { 
    // next_ind_ indicates the total blocks already present
    return next_ind_; 
}

void HashTableHeaderPage::SetSize(size_t size) {
    size_ = size;
}

size_t HashTableHeaderPage::GetSize() const { 
    return size_; 
}

}  // namespace bustub
