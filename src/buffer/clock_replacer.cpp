//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//  todo: thread-safe

#include "buffer/clock_replacer.h"
#include "common/logger.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    this->clock_size = 0;
    this->clock_hand = 0;
    for(size_t i=0;i<num_pages;i++){
        //  at the very first time, buffer pool is full, so clockreplacer is full, too
        //  each clock_element is pinned bcz there's a process using it
        //  and each clock_element would not be referenced, which stands for
        //  each member's isPin is true, and isRef is false
        this->clock_replacer_list.push_back({true, false});
    }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    std::scoped_lock clock_lock{this->clock_mutex};
    while(this->clock_size > 0){
        //  update
        this->clock_hand %= this->clock_replacer_list.size();
        if(this->clock_replacer_list[this->clock_hand].isPin == true){
            this->clock_hand++;
        }
        else if(this->clock_replacer_list[this->clock_hand].isRef == true){
            this->clock_replacer_list[this->clock_hand++].isRef = false;
        }
        else{
            //  set this frame to be replaced(pinned)
            //  dec the victimized number(this->clock_size)
            this->clock_replacer_list[this->clock_hand].isPin = true;
            this->clock_size--;
            *frame_id = this->clock_hand++;
            return true;
        }
    }
    return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock clock_lock{this->clock_mutex};
    if(this->clock_replacer_list[frame_id].isPin == false){
        this->clock_replacer_list[frame_id].isPin = true;
        this->clock_size--;
    }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock clock_lock{this->clock_mutex};
    if(this->clock_replacer_list[frame_id].isPin == true){
        this->clock_replacer_list[frame_id].isPin = false;
        this->clock_replacer_list[frame_id].isRef = true;
        this->clock_size++;
    }
}

size_t ClockReplacer::Size() {
    std::scoped_lock clock_lock{this->clock_mutex};
    return this->clock_size; 
}

}  // namespace bustub
