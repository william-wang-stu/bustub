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

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
      // Hash Table Header Page
      Page* page_ = this->buffer_pool_manager_->NewPage(&this->header_page_id_);
      
      page_->WLatch();
      
      HashTableHeaderPage* ht_page = reinterpret_cast<HashTableHeaderPage*>(page_->GetData());
      ht_page->SetSize(num_buckets);
      ht_page->SetPageId(this->header_page_id_);

      // Hash Table Block Pages
      for(size_t i=0; i<num_buckets; i++){
        page_id_t block_page_id_;
        this->buffer_pool_manager_->NewPage(&block_page_id_);
        ht_page->AddBlockPageId(block_page_id_);
        this->buffer_pool_manager_->UnpinPage(block_page_id_, false);
      }

      // 
      page_->WUnlatch();
      this->buffer_pool_manager_->UnpinPage(this->header_page_id_, true);
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  Page *page_ = this->buffer_pool_manager_->FetchPage(this->header_page_id_);
  page_->RLatch();
  HashTableHeaderPage* ht_page = reinterpret_cast<HashTableHeaderPage*>(page_->GetData());

  // Get Block Index & Bucket Index
  size_t ht_length = ht_page->NumBlocks();
  size_t index = this->hash_fn_.GetHash(key) % (BLOCK_ARRAY_SIZE * ht_length);
  size_t block_index = index / BLOCK_ARRAY_SIZE;
  size_t bucket_index = index % BLOCK_ARRAY_SIZE;

  // Get Block Page
  size_t block_page_id = ht_page->GetBlockPageId(block_index);
  Page *bpage_ = this->buffer_pool_manager_->FetchPage(block_page_id);
  bpage_->RLatch();
  HashTableBlockPage<KeyType,ValueType,KeyComparator>* block_page = reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator>*>(bpage_->GetData());

  // Linear Probe Search
  while(block_page->IsOccupied(bucket_index)){
    // Find Correct Key
    if (block_page->IsReadable(bucket_index) && comparator_(block_page->KeyAt(bucket_index),key) == 0){
      result->push_back(block_page->ValueAt(bucket_index));
    }

    // After Each Cycle
    bucket_index++;
    // Already A Block
    if (bucket_index + block_index * BLOCK_ARRAY_SIZE == index){
      break;
    }

    // Switch to Another Block
    if (bucket_index == BLOCK_ARRAY_SIZE){
      bucket_index = 0;
      bpage_->RUnlatch();
      this->buffer_pool_manager_->UnpinPage(block_page_id, false);
    
      // Fetch New Page for New Block
      block_index++;
      block_index %= ht_length;
      block_page_id = ht_page->GetBlockPageId(block_index);

      bpage_ = this->buffer_pool_manager_->FetchPage(block_page_id);
      bpage_->RLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator>*>(bpage_->GetData());
    }
  }

  //  
  bpage_->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(block_page_id, false);

  page_->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(this->header_page_id_, false);

  table_latch_.RUnlock();

  return result->size() > 0;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  Page *page_ = this->buffer_pool_manager_->FetchPage(this->header_page_id_);
  page_->RLatch();
  HashTableHeaderPage* ht_page = reinterpret_cast<HashTableHeaderPage*>(page_->GetData());

  // Get Block Index & Bucket Index
  size_t ht_length = ht_page->NumBlocks();
  size_t index = this->hash_fn_.GetHash(key) % (BLOCK_ARRAY_SIZE * ht_length);
  size_t block_index = index / BLOCK_ARRAY_SIZE;
  size_t bucket_index = index % BLOCK_ARRAY_SIZE;

  // Get Block Page
  size_t block_page_id = ht_page->GetBlockPageId(block_index);
  Page *bpage_ = this->buffer_pool_manager_->FetchPage(block_page_id);
  bpage_->WLatch();
  HashTableBlockPage<KeyType,ValueType,KeyComparator>* block_page = reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator>*>(bpage_->GetData());

  // Linear Probe Search
  while(block_page->Insert(bucket_index, key, value) == false){
    // Completely the Same
    if (comparator_(block_page->KeyAt(bucket_index),key) == 0 && block_page->ValueAt(bucket_index) == value){
      bpage_->WUnlatch();
      this->buffer_pool_manager_->UnpinPage(block_page_id, false);
      page_->RUnlatch();
      this->buffer_pool_manager_->UnpinPage(this->header_page_id_, false);

      return false;
    }
    
    // After Each Cycle
    bucket_index++;
    // Already A Block, which means the Block is Full
    // So we need to resize the block
    if (bucket_index + block_index * BLOCK_ARRAY_SIZE == index){
      bpage_->WUnlatch();
      this->buffer_pool_manager_->UnpinPage(block_page_id, false);
      page_->RUnlatch();
      this->buffer_pool_manager_->UnpinPage(this->header_page_id_, false);

      this->Resize(ht_length * BLOCK_ARRAY_SIZE);

      /******* Re--Get Header page and Block page *******/
      page_ = this->buffer_pool_manager_->FetchPage(this->header_page_id_);
      page_->RLatch();
      ht_page = reinterpret_cast<HashTableHeaderPage*>(page_->GetData());

      // Get Block Index & Bucket Index
      ht_length = ht_page->NumBlocks();
      index = this->hash_fn_.GetHash(key) % (BLOCK_ARRAY_SIZE * ht_length);
      block_index = index / BLOCK_ARRAY_SIZE;
      bucket_index = index % BLOCK_ARRAY_SIZE;

      // Get Block Page
      block_page_id = ht_page->GetBlockPageId(block_index);
      bpage_ = this->buffer_pool_manager_->FetchPage(block_page_id);
      bpage_->WLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator>*>(bpage_->GetData());
    }

    // Switch to Another Block
    if (bucket_index == BLOCK_ARRAY_SIZE){
      bucket_index = 0;
      bpage_->WUnlatch();
      this->buffer_pool_manager_->UnpinPage(block_page_id, false);
    
      // Fetch New Page for New Block
      block_index++;
      block_index %= ht_length;
      block_page_id = ht_page->GetBlockPageId(block_index);

      bpage_ = this->buffer_pool_manager_->FetchPage(block_page_id);
      bpage_->WLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator>*>(bpage_->GetData());
    }
  }

  //  
  bpage_->WUnlatch();
  this->buffer_pool_manager_->UnpinPage(block_page_id, false);
  page_->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(this->header_page_id_, false);

  table_latch_.RUnlock();

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  Page *page_ = this->buffer_pool_manager_->FetchPage(this->header_page_id_);
  page_->RLatch();
  HashTableHeaderPage* ht_page = reinterpret_cast<HashTableHeaderPage*>(page_->GetData());

  // Get Block Index & Bucket Index
  size_t ht_length = ht_page->NumBlocks();
  size_t index = this->hash_fn_.GetHash(key) % (BLOCK_ARRAY_SIZE * ht_length);
  size_t block_index = index / BLOCK_ARRAY_SIZE;
  size_t bucket_index = index % BLOCK_ARRAY_SIZE;

  // Get Block Page
  size_t block_page_id = ht_page->GetBlockPageId(block_index);
  Page *bpage_ = this->buffer_pool_manager_->FetchPage(block_page_id);
  bpage_->WLatch();
  HashTableBlockPage<KeyType,ValueType,KeyComparator>* block_page = reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator>*>(bpage_->GetData());

  // Linear Probe Search
  while(block_page->IsOccupied(bucket_index)){
    //  Completely the Same
    if (comparator_(block_page->KeyAt(bucket_index),key) == 0 && block_page->ValueAt(bucket_index) == value){
      if (block_page->IsReadable(bucket_index)){
        block_page->Remove(bucket_index);

        bpage_->WUnlatch();
        this->buffer_pool_manager_->UnpinPage(block_page_id, false);
        page_->RUnlatch();
        this->buffer_pool_manager_->UnpinPage(this->header_page_id_, false);
        
        return true;
      }
      else{
        bpage_->WUnlatch();
        this->buffer_pool_manager_->UnpinPage(block_page_id, false);
        page_->RUnlatch();
        this->buffer_pool_manager_->UnpinPage(this->header_page_id_, false);

        return false;
      }
    }

    // After Each Cycle
    bucket_index++;
    // Already A Block
    if (bucket_index + block_index * BLOCK_ARRAY_SIZE == index){
      break;
    }

    // Switch to Another Block
    if (bucket_index == BLOCK_ARRAY_SIZE){
      bucket_index = 0;
      bpage_->WUnlatch();
      this->buffer_pool_manager_->UnpinPage(block_page_id, false);
    
      // Fetch New Page for New Block
      block_index++;
      block_index %= ht_length;
      block_page_id = ht_page->GetBlockPageId(block_index);

      bpage_ = this->buffer_pool_manager_->FetchPage(block_page_id);
      bpage_->WLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator>*>(bpage_->GetData());
    }
  }

  //  
  bpage_->WUnlatch();
  this->buffer_pool_manager_->UnpinPage(block_page_id, false);
  page_->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(this->header_page_id_, false);
  table_latch_.RUnlock();

  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  size_t new_size = initial_size * 2;
  size_t new_bucket_num = new_size / BLOCK_ARRAY_SIZE;

  page_id_t new_header_page_id = INVALID_PAGE_ID;
  Page* page_ = this->buffer_pool_manager_->NewPage(&new_header_page_id);
  page_->WLatch();
  HashTableHeaderPage* ht_page = reinterpret_cast<HashTableHeaderPage*>(page_->GetData());
  ht_page->SetSize(new_bucket_num);
  ht_page->SetPageId(new_header_page_id);

  // Allocate New Block Pages
  for(size_t i=0; i<new_bucket_num; i++){
    page_id_t block_page_id_ = INVALID_PAGE_ID;
    this->buffer_pool_manager_->NewPage(&block_page_id_);
    ht_page->AddBlockPageId(block_page_id_);
    this->buffer_pool_manager_->UnpinPage(block_page_id_, false);
  }

  // Move Key-Value Pair
  Page* old_page_ = this->buffer_pool_manager_->FetchPage(this->header_page_id_);
  old_page_->RLatch();
  HashTableHeaderPage* old_ht_page = reinterpret_cast<HashTableHeaderPage*>(old_page_->GetData());
  size_t old_ht_page_length = old_ht_page->NumBlocks();

  for(size_t block_index=0; block_index<old_ht_page_length; block_index++){
    page_id_t old_bpage_id = old_ht_page->GetBlockPageId(block_index);
    Page* old_bpage = this->buffer_pool_manager_->FetchPage(old_bpage_id);
    old_bpage->RLatch();
    HashTableBlockPage<KeyType,ValueType,KeyComparator>* old_block_page = reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator>*>(old_bpage);

    for(size_t bucket_index=0; bucket_index<BLOCK_ARRAY_SIZE; bucket_index++){
      if (old_block_page->IsReadable(bucket_index)){
        KeyType key = old_block_page->KeyAt(bucket_index);
        ValueType value = old_block_page->ValueAt(bucket_index);

        this->Insert(nullptr, key, value);
      }
    }

    old_bpage->RUnlatch();
    this->buffer_pool_manager_->UnpinPage(old_bpage_id, false);
    this->buffer_pool_manager_->DeletePage(old_bpage_id);
  }

  // 
  page_id_t old_header_page_id = this->header_page_id_;
  this->header_page_id_ = new_header_page_id;

  old_page_->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(old_header_page_id, false);
  this->buffer_pool_manager_->DeletePage(old_header_page_id);

  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  size_t table_size = 0;

  Page *page_ = this->buffer_pool_manager_->FetchPage(this->header_page_id_);
  page_->RLatch();
  HashTableHeaderPage* ht_page = reinterpret_cast<HashTableHeaderPage*>(page_->GetData());

  size_t block_num = ht_page->NumBlocks();
  table_size = BLOCK_ARRAY_SIZE * block_num;

  page_->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(this->header_page_id_, false);

  table_latch_.RUnlock();
  return table_size;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
