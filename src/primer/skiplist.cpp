//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// skiplist.cpp
//
// Identification: src/primer/skiplist.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/skiplist.h"
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "common/macros.h"
#include "fmt/core.h"

namespace bustub {

/** @brief Checks whether the container is empty. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Empty() -> bool {
  // Skip list is empty if there are no nodes after the header at the lowest level
  return header_->Next(LOWEST_LEVEL) == nullptr;
}

/** @brief Returns the number of elements in the skip list. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Size() -> size_t {
  size_t count = 0;
  auto curr = header_->Next(LOWEST_LEVEL);
  while (curr != nullptr) {
    count++;
    curr = curr->Next(LOWEST_LEVEL);
  }
  return count;
}

/**
 * @brief Iteratively deallocate all the nodes.
 *
 * We do this to avoid stack overflow when the skip list is large.
 *
 * If we let the compiler handle the deallocation, it will recursively call the destructor of each node,
 * which could block up the the stack.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Drop() {
  for (size_t i = 0; i < MaxHeight; i++) {
    auto curr = std::move(header_->links_[i]);
    while (curr != nullptr) {
      // std::move sets `curr` to the old value of `curr->links_[i]`,
      // and then resets `curr->links_[i]` to `nullptr`.
      curr = std::move(curr->links_[i]);
    }
  }
}

/**
 * @brief Removes all elements from the skip list.
 *
 * Note: You might want to use the provided `Drop` helper function.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Clear() {
  Drop();
  // Reset all header links to nullptr
  for (size_t i = 0; i < MaxHeight; i++) {
    header_->links_[i] = nullptr;
  }
}

/**
 * @brief Inserts a key into the skip list.
 *
 * Note: `Insert` will not insert the key if it already exists in the skip list.
 *
 * @param key key to insert.
 * @return true if the insertion is successful, false if the key already exists.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Insert(const K &key) -> bool {
  // Array to store predecessors at each level
  std::array<std::shared_ptr<SkipNode>, MaxHeight> predecessors;

  // Find the position to insert by traversing from top to bottom
  auto curr = header_;
  for (int level = MaxHeight - 1; level >= 0; level--) {
    // Move forward while the next node's key is less than the target key
    while (curr->Next(level) != nullptr && compare_(curr->Next(level)->Key(), key)) {
      curr = curr->Next(level);
    }
    predecessors[level] = curr;
  }

  // Check if key already exists
  auto next_node = curr->Next(LOWEST_LEVEL);
  if (next_node != nullptr && !compare_(key, next_node->Key()) && !compare_(next_node->Key(), key)) {
    return false; // Key already exists
  }

  // Generate random height for new node
  size_t new_height = RandomHeight();

  // Create new node - constructor takes (height, key)
  auto new_node = std::make_shared<SkipNode>(new_height, key);

  // Update links at each level
  for (size_t level = 0; level < new_height; level++) {
    new_node->SetNext(level, predecessors[level]->Next(level));
    predecessors[level]->SetNext(level, new_node);
  }

  return true;
}

/**
 * @brief Erases the key from the skip list.
 *
 * @param key key to erase.
 * @return bool true if the element got erased, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Erase(const K &key) -> bool {
  // Array to store predecessors at each level
  std::array<std::shared_ptr<SkipNode>, MaxHeight> predecessors;

  // Find the node to delete
  auto curr = header_;
  for (int level = MaxHeight - 1; level >= 0; level--) {
    while (curr->Next(level) != nullptr && compare_(curr->Next(level)->Key(), key)) {
      curr = curr->Next(level);
    }
    predecessors[level] = curr;
  }

  // Check if the key exists
  auto target_node = curr->Next(LOWEST_LEVEL);
  if (target_node == nullptr || compare_(key, target_node->Key()) || compare_(target_node->Key(), key)) {
    return false; // Key doesn't exist
  }

  // Update links at each level where the target node exists
  for (size_t level = 0; level < target_node->Height(); level++) {
    predecessors[level]->SetNext(level, target_node->Next(level));
  }

  return true;
}

/**
 * @brief Checks whether a key exists in the skip list.
 *
 * @param key key to look up.
 * @return bool true if the element exists, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Contains(const K &key) -> bool {
  // Following the standard library: Key `a` and `b` are considered equivalent if neither compares less
  // than the other: `!compare_(a, b) && !compare_(b, a)`.

  auto curr = header_;
  // Traverse from top level to bottom
  for (int level = MaxHeight - 1; level >= 0; level--) {
    // Move forward while next node's key is less than target
    while (curr->Next(level) != nullptr && compare_(curr->Next(level)->Key(), key)) {
      curr = curr->Next(level);
    }
  }

  // Check the next node at the lowest level
  auto next_node = curr->Next(LOWEST_LEVEL);
  if (next_node != nullptr && !compare_(key, next_node->Key()) && !compare_(next_node->Key(), key)) {
    return true; // Keys are equivalent
  }

  return false;
}

/**
 * @brief Prints the skip list for debugging purposes.
 *
 * Note: You may modify the functions in any way and the output is not tested.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Print() {
  auto node = header_->Next(LOWEST_LEVEL);
  while (node != nullptr) {
    fmt::println("Node {{ key: {}, height: {} }}", node->Key(), node->Height());
    node = node->Next(LOWEST_LEVEL);
  }
}

/**
 * @brief Generate a random height. The height should be cappped at `MaxHeight`.
 * Note: we implement/simulate the geometric process to ensure platform independence.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::RandomHeight() -> size_t {
  // Branching factor (1 in 4 chance), see Pugh's paper.
  static constexpr unsigned int branching_factor = 4;
  // Start with the minimum height
  size_t height = 1;
  while (height < MaxHeight && (rng_() % branching_factor == 0)) {
    height++;
  }
  return height;
}

/**
 * @brief Gets the current node height.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Height() const -> size_t {
  return links_.size();
}

/**
 * @brief Gets the next node by following the link at `level`.
 *
 * @param level index to the link.
 * @return std::shared_ptr<SkipNode> the next node, or `nullptr` if such node does not exist.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Next(size_t level) const
    -> std::shared_ptr<SkipNode> {
  if (level >= links_.size()) {
    return nullptr;
  }
  return links_[level];
}

/**
 * @brief Set the `node` to be linked at `level`.
 *
 * @param level index to the link.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::SkipNode::SetNext(
    size_t level, const std::shared_ptr<SkipNode> &node) {
  if (level < links_.size()) {
    links_[level] = node;
  }
}

/** @brief Returns a reference to the key stored in the node. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Key() const -> const K & {
  return key_;
}

// Below are explicit instantiation of template classes.
template class SkipList<int>;
template class SkipList<std::string>;
template class SkipList<int, std::greater<>>;
template class SkipList<int, std::less<>, 8>;

}