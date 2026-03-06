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
#include "buffer/arc_replacer.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) 
{
    
}

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
auto ArcReplacer::Evict() -> std::optional<frame_id_t> 
{ 
    std::lock_guard<std::mutex> lock(latch_);

    // MRU
    if(mru_.size() >= mru_target_size_) {
        auto frame_id = EvictMru();
        if(frame_id != -1) {
            return frame_id;
        } else {
            frame_id = EvictMfu();
            if(frame_id != -1) {
                return frame_id;
            }
        }
    }

    // MFU
    auto frame_id = EvictMfu();
    if(frame_id != -1) {
        return frame_id;
    } else {
        frame_id = EvictMru();
        if(frame_id != -1) {
            return frame_id;
        }
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
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) 
{
    std::lock_guard<std::mutex> lock(latch_);
    auto alive_it = alive_map_.find(frame_id);

    // 1. Access hits mru_ or mfu_
    if(alive_it != alive_map_.end()) {
        auto frame_status = alive_it->second;
        if(frame_status->arc_status_ == ArcStatus::MRU) {
            frame_status->arc_status_ = ArcStatus::MFU;
            mru_.erase(frame_status->iter);
            mfu_.push_front(frame_status->frame_id_);
            frame_status->iter = mfu_.begin();
        } else {
            mfu_.erase(frame_status->iter);
            mfu_.push_front(frame_status->frame_id_);
            frame_status->iter = mfu_.begin();
        }
        return ;
    }

    auto ghost_it = ghost_map_.find(page_id);
        
    // 2/3. Access hits mru_ghost_ / mfu_ghost_
    if(ghost_it != ghost_map_.end()) {
        auto frame_status = ghost_it->second;
        frame_status->frame_id_ = frame_id;

        if(frame_status->arc_status_ == ArcStatus::MRU_GHOST) {
            mru_target_size_++;
            frame_status->arc_status_ = ArcStatus::MFU;
            mru_ghost_.erase(frame_status->giter);
            ghost_map_.erase(ghost_it);
            
            alive_map_[frame_status->frame_id_] = frame_status;
            mfu_.push_front(frame_status->frame_id_);
            frame_status->iter = mfu_.begin();
            return ;
        } else {
            if (mru_target_size_ > 0) {
                mru_target_size_--;
            }
            frame_status->arc_status_ = ArcStatus::MFU;
            mfu_ghost_.erase(frame_status->giter);
            ghost_map_.erase(ghost_it);
            
            alive_map_[frame_status->frame_id_] = frame_status;
            mfu_.push_front(frame_status->frame_id_);
            frame_status->iter = mfu_.begin();

            return ;
        }
    }

    // 4. Access misses all the lists
    auto new_frame = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
    mru_.push_front(frame_id);
    new_frame->iter = mru_.begin();
    alive_map_[frame_id] = new_frame;
    if(mru_.size() + mru_ghost_.size() >= replacer_size_) {
        EvictGhost();
    }
    return ;
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
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) 
{
    std::lock_guard<std::mutex> lock(latch_);

    auto it = alive_map_.find(frame_id);
    if(it != alive_map_.end()) {
        if(it->second->evictable_ != set_evictable) {
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
void ArcReplacer::Remove(frame_id_t frame_id)
{
    std::lock_guard<std::mutex> lock(latch_);

    auto it = alive_map_.find(frame_id);

    if (it == alive_map_.end()) {
        return;
    }

    if (!it->second->evictable_) {
        throw std::runtime_error("Attempting to remove a non-evictable frame");
    }

    if (it->second->arc_status_ == ArcStatus::MRU) {
        mru_.erase(it->second->iter);
    } else if (it->second->arc_status_ == ArcStatus::MFU) {
        mfu_.erase(it->second->iter);
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
auto ArcReplacer::Size() -> size_t 
{ 
    return curr_size_; 
}

frame_id_t ArcReplacer::EvictMru() 
{ 
    for(auto it = mru_.rbegin(); it != mru_.rend(); ++it) {
        auto mpit = alive_map_.find(*it);
        if(mpit->second->evictable_) {
            auto frame_status = mpit->second;
            frame_status->arc_status_ = ArcStatus::MRU_GHOST;
            alive_map_.erase(mpit);
            ghost_map_[frame_status->page_id_] = frame_status;
            
            auto frame_id = *it;
            mru_.erase(frame_status->iter);
            curr_size_--;
            
            mru_ghost_.push_front(frame_status->page_id_);
            frame_status->giter = mru_ghost_.begin();
            while(mru_ghost_.size() + mfu_ghost_.size() > replacer_size_) {
                EvictGhost();
            }

            return frame_id;
        }
    }
    return -1; 
}

frame_id_t ArcReplacer::EvictMfu() 
{ 
    for(auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
        auto mpit = alive_map_.find(*it);
        if(mpit->second->evictable_) {
            auto frame_status = mpit->second;
            frame_status->arc_status_ = ArcStatus::MFU_GHOST;
            alive_map_.erase(mpit);
            ghost_map_[frame_status->page_id_] = frame_status;
            
            auto frame_id = *it;
            mfu_.erase(frame_status->iter);
            curr_size_--;
            
            mfu_ghost_.push_front(frame_status->page_id_);
            frame_status->giter = mfu_ghost_.begin();
            while(mru_ghost_.size() + mfu_ghost_.size() > replacer_size_) {
                EvictGhost();
            }

            return frame_id;
        }
    }
    return -1; 
}

void ArcReplacer::EvictGhost() {
    if(!mru_ghost_.empty()) {
        auto page_id = mru_ghost_.back();
        mru_ghost_.pop_back();
        ghost_map_.erase(page_id);
        return ;
    }

    if(!mfu_ghost_.empty()) {
        auto page_id = mfu_ghost_.back();
        mfu_ghost_.pop_back();
        ghost_map_.erase(page_id);
    }
}

}  // namespace bustub
