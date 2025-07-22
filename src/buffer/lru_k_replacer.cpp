//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { 
  std::lock_guard<std::mutex> lock(latch_);
  
  if (curr_size_ == 0) {
    return std::nullopt;
  }
  
  frame_id_t evict_frame = -1;
  size_t max_backward_k_distance = 0;
  size_t earliest_timestamp = std::numeric_limits<size_t>::max();
  bool found_inf = false;
  
  for (auto& [frame_id, node] : node_store_) {
    if (!evictable_[frame_id]) {
      continue;
    }
    
    // 如果历史记录少于k次，后向k距离为无穷大
    if (access_history_[frame_id].size() < k_) {
      if (access_history_[frame_id].empty()) {
        continue;
      }
      
      size_t oldest_timestamp = access_history_[frame_id].front();
      if (!found_inf || oldest_timestamp < earliest_timestamp) {
        evict_frame = frame_id;
        earliest_timestamp = oldest_timestamp;
        found_inf = true;
      }
    } else if (!found_inf) {
      // 计算后向k距离，只有在没有找到无穷大距离的情况下才考虑
      size_t kth_timestamp = access_history_[frame_id].front();
      size_t backward_k_distance = current_timestamp_ - kth_timestamp;
      
      if (evict_frame == -1 || backward_k_distance > max_backward_k_distance) {
        evict_frame = frame_id;
        max_backward_k_distance = backward_k_distance;
      }
    }
  }
  
  if (evict_frame == -1) {
    return std::nullopt;
  }
  
  // 移除被淘汰的帧
  node_store_.erase(evict_frame);
  access_history_.erase(evict_frame);
  evictable_.erase(evict_frame);
  curr_size_--;
  
  return evict_frame;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("Invalid frame_id");
  }
  
  current_timestamp_++;
  
  // 如果是新的frame_id，初始化相关数据结构
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_[frame_id] = LRUKNode();
    access_history_[frame_id] = std::list<size_t>();
    evictable_[frame_id] = false;
  }
  
  // 记录访问时间
  access_history_[frame_id].push_back(current_timestamp_);
  
  // 如果历史记录超过k个，移除最旧的
  if (access_history_[frame_id].size() > k_) {
    access_history_[frame_id].pop_front();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("Invalid frame_id");
  }
  
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  
  bool current_evictable = evictable_[frame_id];
  
  if (current_evictable && !set_evictable) {
    // 从可淘汰变为不可淘汰
    evictable_[frame_id] = false;
    curr_size_--;
  } else if (!current_evictable && set_evictable) {
    // 从不可淘汰变为可淘汰
    evictable_[frame_id] = true;
    curr_size_++;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  
  if (!evictable_[frame_id]) {
    throw std::invalid_argument("Cannot remove non-evictable frame");
  }
  
  node_store_.erase(frame_id);
  access_history_.erase(frame_id);
  evictable_.erase(frame_id);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { 
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub