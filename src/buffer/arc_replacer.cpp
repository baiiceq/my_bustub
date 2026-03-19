// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <optional>
#include "common/config.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    return std::nullopt;
  }

  frame_id_t victim_id = -1;
  if (!mru_.empty() && (mru_.size() >= mru_target_size_)) {
    victim_id = EvictFromList(mru_, ArcStatus::MRU_GHOST);
    if (victim_id == -1) {
      victim_id = EvictFromList(mfu_, ArcStatus::MFU_GHOST);
    }
  } else {
    victim_id = EvictFromList(mfu_, ArcStatus::MFU_GHOST);
    if (victim_id == -1) {
      victim_id = EvictFromList(mru_, ArcStatus::MRU_GHOST);
    }
  }

  if (victim_id != -1) {
    return victim_id;
  }
  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  auto alive_it = alive_map_.find(frame_id);

  // 1. Access hits mru_ or mfu_
  if (alive_it != alive_map_.end()) {
    auto frame_status = alive_it->second;
    if (frame_status->arc_status_ == ArcStatus::MRU) {
      frame_status->arc_status_ = ArcStatus::MFU;
      mru_.erase(frame_status->iter_);
      mfu_.push_front(frame_status->frame_id_);
      frame_status->iter_ = mfu_.begin();
    } else {
      mfu_.erase(frame_status->iter_);
      mfu_.push_front(frame_status->frame_id_);
      frame_status->iter_ = mfu_.begin();
    }
    return;
  }

  auto ghost_it = ghost_map_.find(page_id);

  // 2/3. Access hits mru_ghost_ / mfu_ghost_
  if (ghost_it != ghost_map_.end()) {
    auto frame_status = ghost_it->second;
    if (frame_status->arc_status_ == ArcStatus::MRU_GHOST) {
      size_t delta = (mru_ghost_.size() >= mfu_ghost_.size()) ? 1 : (mfu_ghost_.size() / mru_ghost_.size());
      mru_target_size_ = std::min(replacer_size_, mru_target_size_ + delta);
      mru_ghost_.erase(frame_status->giter_);
    } else {
      size_t delta = (mfu_ghost_.size() >= mru_ghost_.size()) ? 1 : (mru_ghost_.size() / mfu_ghost_.size());
      mru_target_size_ = (mru_target_size_ > delta) ? mru_target_size_ - delta : 0;
      mfu_ghost_.erase(frame_status->giter_);
    }

    ghost_map_.erase(page_id);
    frame_status->frame_id_ = frame_id;
    frame_status->arc_status_ = ArcStatus::MFU;
    frame_status->evictable_ = false;
    mfu_.push_front(frame_id);
    frame_status->iter_ = mfu_.begin();
    alive_map_[frame_id] = frame_status;
    return;
  }

  size_t t1_size = mru_.size();
  size_t b1_size = mru_ghost_.size();
  size_t t2_size = mfu_.size();
  size_t b2_size = mfu_ghost_.size();

  if (t1_size + b1_size == replacer_size_) {
    if (b1_size > 0) {
      ghost_map_.erase(mru_ghost_.back());
      mru_ghost_.pop_back();
      b1_size--;
    } else {
      auto fid = EvictFromList(mru_, ArcStatus::MRU_GHOST);
      if (fid != -1) {
        auto new_fs = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
        mru_.push_front(frame_id);
        new_fs->iter_ = mru_.begin();
        alive_map_[frame_id] = new_fs;
        return;
      }
    }
  }

  if (t1_size + t2_size + b1_size + b2_size >= replacer_size_) {
    if (t1_size + t2_size + b1_size + b2_size == 2 * replacer_size_) {
      if (b2_size > 0) {
        ghost_map_.erase(mfu_ghost_.back());
        mfu_ghost_.pop_back();
      }
    }
  }

  auto new_fs = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
  mru_.push_front(frame_id);
  new_fs->iter_ = mru_.begin();
  alive_map_[frame_id] = new_fs;
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
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = alive_map_.find(frame_id);
  if (it != alive_map_.end()) {
    if (it->second->evictable_ != set_evictable) {
      curr_size_ += set_evictable ? 1 : -1;
      it->second->evictable_ = set_evictable;
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = alive_map_.find(frame_id);

  if (it == alive_map_.end()) {
    return;
  }

  if (!it->second->evictable_) {
    throw std::runtime_error("Attempting to remove a non-evictable frame");
  }

  if (it->second->arc_status_ == ArcStatus::MRU) {
    mru_.erase(it->second->iter_);
  } else if (it->second->arc_status_ == ArcStatus::MFU) {
    mfu_.erase(it->second->iter_);
  }

  alive_map_.erase(it);

  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t { return curr_size_; }

auto ArcReplacer::EvictFromList(std::list<frame_id_t> &list, ArcStatus ghost_status) -> frame_id_t {
  for (auto it = list.rbegin(); it != list.rend(); ++it) {
    frame_id_t fid = *it;
    auto fs = alive_map_[fid];
    if (fs->evictable_) {
      list.erase(fs->iter_);
      alive_map_.erase(fid);
      curr_size_--;

      fs->arc_status_ = ghost_status;
      if (ghost_status == ArcStatus::MRU_GHOST) {
        mru_ghost_.push_front(fs->page_id_);
        fs->giter_ = mru_ghost_.begin();
      } else {
        mfu_ghost_.push_front(fs->page_id_);
        fs->giter_ = mfu_ghost_.begin();
      }
      ghost_map_[fs->page_id_] = fs;
      return fid;
    }
  }
  return -1;
}

void ArcReplacer::EvictGhost() {
  if (!mru_ghost_.empty()) {
    auto page_id = mru_ghost_.back();
    mru_ghost_.pop_back();
    ghost_map_.erase(page_id);
    return;
  }

  if (!mfu_ghost_.empty()) {
    auto page_id = mfu_ghost_.back();
    mfu_ghost_.pop_back();
    ghost_map_.erase(page_id);
  }
}

}  // namespace bustub
