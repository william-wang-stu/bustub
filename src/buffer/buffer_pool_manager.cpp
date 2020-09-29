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
  //  replacer_ = new LRUReplacer(pool_size);
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
  
  std::scoped_lock buffer_pool_lock{this->latch_};

  //  1.
  //  1.1
  std::unordered_map<page_id_t, frame_id_t>::const_iterator iter = this->page_table_.find(page_id);
  if(iter != this->page_table_.end()){
    this->replacer_->Pin(iter->second);
    this->pages_[iter->second].pin_count_++;
    return &this->pages_[iter->second];
  }

  //  1.2
  frame_id_t fetch_frame_id = INVALID_PAGE_ID;
  if(this->free_list_.empty() == false){
    fetch_frame_id = this->free_list_.front();
    this->free_list_.pop_front();
  }
  else if(this->replacer_->Victim(&fetch_frame_id) == true){
    page_id_t fetch_page_id = this->pages_[fetch_frame_id].GetPageId();
    if(this->pages_[fetch_frame_id].is_dirty_ == true){
      //  2.
      this->FlushPageImpl(fetch_page_id);
    }
    //  3.1 Delete
    this->page_table_.erase(fetch_page_id);
  }
  else{
    return nullptr;
  }

  //  3.2 Insert New Page
  this->pages_[fetch_frame_id].ResetMemory();
  this->pages_[fetch_frame_id].page_id_ = page_id;
  this->pages_[fetch_frame_id].pin_count_ = 1;
  this->pages_[fetch_frame_id].is_dirty_ = false;
  //  this->page_table_.insert(std::make_pair(page_id, fetch_frame_id));

  //  4.
  this->disk_manager_->ReadPage(page_id, this->pages_[fetch_frame_id].data_);
  this->page_table_.insert(std::make_pair(page_id, fetch_frame_id));

  return &this->pages_[fetch_frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock buffer_pool_lock{this->latch_};

  //  1. return false if not in buffer pool
  std::unordered_map<page_id_t, frame_id_t>::const_iterator iter = this->page_table_.find(page_id);
  if(iter == this->page_table_.end()){
    return false;
  }

  //  2. check pin_count
  if(this->pages_[iter->second].pin_count_ <= 0){
    return false;
  }

  //  3. set is_dirty_ & pin_count_
  this->pages_[iter->second].is_dirty_ = is_dirty | this->pages_[iter->second].is_dirty_;
  this->pages_[iter->second].pin_count_--;

  //  4. check Unpin
  if(this->pages_[iter->second].pin_count_ == 0){
    this->replacer_->Unpin(iter->second);
  }

  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!

  //  1. find page
  std::unordered_map<page_id_t, frame_id_t>::const_iterator iter = this->page_table_.find(page_id);
  if(iter == this->page_table_.end()){
    return false;
  }

  //  2. write dirty page back
  if(this->pages_[iter->second].is_dirty_ == false){
    return false;
  }
  else{
    this->disk_manager_->WritePage(page_id, this->pages_[iter->second].data_);
    this->pages_[iter->second].is_dirty_ = false;
  }

  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::scoped_lock buffer_pool_lock{this->latch_};

  //  2.
  frame_id_t fetch_frame_id = INVALID_PAGE_ID;
  if(this->free_list_.empty() == false){
    fetch_frame_id = this->free_list_.front();
    this->free_list_.pop_front();
  }
  else if(this->replacer_->Victim(&fetch_frame_id) == false){
    //  1.
    return nullptr;
  }
  else{
    page_id_t fetch_page_id = this->pages_[fetch_frame_id].GetPageId();
    if(this->pages_[fetch_frame_id].is_dirty_ == true){
      //  2.
      this->FlushPageImpl(fetch_page_id);
    }
    //  3.1 Delete
    this->page_table_.erase(fetch_page_id);
  }

  //  3.2 Insert New Page
  *page_id = this->disk_manager_->AllocatePage();
  this->pages_[fetch_frame_id].ResetMemory();
  this->pages_[fetch_frame_id].page_id_ = *page_id;
  this->pages_[fetch_frame_id].pin_count_ = 1;
  this->pages_[fetch_frame_id].is_dirty_ = false;
  //  this->page_table_.insert(std::make_pair(page_id, fetch_frame_id));

  //  4.
  this->disk_manager_->ReadPage(*page_id, this->pages_[fetch_frame_id].data_);
  this->page_table_.insert(std::make_pair(*page_id, fetch_frame_id));

  return &this->pages_[fetch_frame_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::scoped_lock buffer_pool_lock{this->latch_};

  //  1. 
  std::unordered_map<page_id_t, frame_id_t>::const_iterator iter = this->page_table_.find(page_id);
  if(iter == this->page_table_.end()){
    this->disk_manager_->DeallocatePage(page_id);
    //  return true;
  }
  else{
    //  2. 
    if(this->pages_[iter->second].pin_count_ > 0){
      return false;
    }
    else{
      this->page_table_.erase(page_id);
      
      this->pages_[iter->second].ResetMemory();
      this->pages_[iter->second].page_id_ = INVALID_PAGE_ID;
      this->pages_[iter->second].pin_count_ = 0;
      this->pages_[iter->second].is_dirty_ = false;

      this->free_list_.push_back(iter->second);

      this->disk_manager_->DeallocatePage(page_id);
      //  return true;
    }
  }

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!

  std::scoped_lock buffer_pool_lock{this->latch_};

  for(size_t i=0;i<this->pool_size_;i++){
    this->FlushPageImpl(this->pages_[i].page_id_);
  }
}

}  // namespace bustub
