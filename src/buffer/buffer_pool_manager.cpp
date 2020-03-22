//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}



BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::unordered_map<page_id_t, frame_id_t>::iterator it = page_table_.find(page_id);
  frame_id_t frame_id;

  // if page alread in buffer manager
  if (it != page_table_.end()) {
    frame_id = page_table_[it->second];
    return &pages_[frame_id];
  }

  // check free list
  if (free_list_.size() > 0) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
     bool victim_present = replacer_->Victim(&frame_id);
     // nothing in the free list and replacer
     // this also means that all pages are pinned
     if (!victim_present) {
       return nullptr;
     }
  }  

  // flush victim page if dirty
  if (pages_[frame_id].is_dirty_) {
    FlushPageImpl(pages_[frame_id].page_id_);
  }

  // erase victim page_id
  page_table_.erase(page_table_.find(pages_[frame_id].page_id_));

  // insert p into page_table
  page_table_[page_id] = frame_id;

  // clean the frame and reset meta data to new page
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  
  disk_manager_->ReadPage(page_id, &pages_[frame_id].data_);
  return &pages_[frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  if (is_dirty) {
    FlushPageImpl(page_id);
  }

  frame_id_t frame_id = page_table_[page_id];
  replacer_->Unpin(frame_id);
  pages_[frame_id].pin_count_ = 0;
  return true;
 }

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  // Make sure you call DiskManager::WritePage!
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  frame_id_t frame_id;
  page_id_t  new_page_id;

  // check free list
  if (free_list_.size() > 0) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
     bool victim_present = replacer_->Victim(&frame_id);
     // nothing in the free list and replacer
     // this also means that all pages are pinned
     if (!victim_present) {
       return nullptr;
     }
  }

  new_page_id = disk_manager_->AllocatePage();
  
  // zero out memory
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  memset(&pages_[frame_id].data_, 0, PAGE_SIZE);

  // add to the page table
  page_table_[new_page_id] = frame_id;

  // set page_id
  *page_id = new_page_id;

  // return page pointer
  return &pages_[frame_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  frame_id_t frame_id;
  page_id_t page_id;
  std::unordered_map<page_id_t, frame_id_t>::iterator it = page_table_.begin();
  while (it != page_table_.end()) {
    page_id = it->first;
    frame_id = it->second;
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  }
}



}  // namespace bustub
