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

#include "buffer/clock_replacer.h"

using namespace std;

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    candidates = new list<int>;
    is_frame_present = new vector<int>(num_pages, 0);
}

ClockReplacer::~ClockReplacer() {
    delete candidates;
    delete is_frame_present;
}

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
    if (candidates->size() == 0) {
        return false;
    }

    int victim_id = candidates->front();
    candidates->pop_front();
    *frame_id = victim_id;
    is_frame_present->at(victim_id) = 0;
    return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    if (is_frame_present->at(frame_id) == 1) {
            candidates->remove(frame_id);
            is_frame_present->at(frame_id) = 0;
    }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    // add to the clock replacer vec
    if (is_frame_present->at(frame_id)) {
        return;
    }
    candidates->push_back(frame_id);
    is_frame_present->at(frame_id) = 1;
}

size_t ClockReplacer::Size() {
    return candidates->size();
 }

}  // namespace bustub
