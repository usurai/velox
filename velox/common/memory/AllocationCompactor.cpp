/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/common/memory/AllocationCompactor.h"

namespace facebook::velox::detail {

int64_t tryAccommodate(int64_t destSize, int64_t srcSize, bool isSrcContinued) {
  static constexpr auto kMinAlloc = HashStringAllocator::kMinAlloc;
  static constexpr auto kMinBlockSize = AllocationCompactor::kMinBlockSize;
  static constexpr auto kContinuedPtrSize =
      HashStringAllocator::Header::kContinuedPtrSize;

  VELOX_CHECK_GE(destSize, kMinAlloc);

  if (srcSize + kMinBlockSize <= destSize || srcSize == destSize) {
    return srcSize;
  }

  if (!isSrcContinued) {
    if (srcSize < destSize) {
      // After accommodating src block, the rest of space of the 'destBlock' is
      // not enough for a valid block, use it up.
      return srcSize;
    }
    // src block is larger than dest block, split src block so the first part
    // can fill the dest block.
    return destSize - kContinuedPtrSize;
  }

  // src block is continued.
  VELOX_CHECK_GE(srcSize, kMinAlloc);
  if (srcSize - destSize + kContinuedPtrSize >= kMinAlloc) {
    // src block is larger than dest block and when splitting src block to fill
    // the dest block, the second part of the splitted block is valid.
    return destSize - kContinuedPtrSize;
  }

  // src block's size is slightly different from dest block's size: If src block
  // is smaller than dest block, the diff is not enough for a valid free block;
  // otherwise, the diff is not enough to put in a valid continued block. Try
  // splitting src block into two blocks A and B, put A into dest block so
  // that remaining of dest block after accommodating A forms a minimum valid
  // free block(Will not be used for later accommodation, just for keeping the
  // invariant of HSA). We can put B into another page later. The constraint is
  // that A and B should both be valid(>= kMinAlloc), if unsatisfied, we can
  // only skip this dest block and put src block into later page.
  if (srcSize < destSize) {
    VELOX_CHECK_GT(srcSize, destSize - kMinBlockSize);
  } else {
    VELOX_CHECK_LT(srcSize - destSize + kContinuedPtrSize, kMinAlloc);
  }
  const auto sizeA{std::min<int32_t>(
      destSize - kMinBlockSize - HashStringAllocator::Header::kContinuedPtrSize,
      srcSize - HashStringAllocator::kMinAlloc)};
  const auto sizeB{srcSize - sizeA};
  if (sizeA + HashStringAllocator::Header::kContinuedPtrSize >= kMinAlloc &&
      sizeB >= kMinAlloc) {
    return sizeA;
  }
  return 0;
}

} // namespace facebook::velox::detail

namespace facebook::velox {

int64_t AllocationCompactionStrategy::estimateReclaimableSize() {
  if (hsa_->requestedContiguous_ && !hsa_->allowSplittingContiguous_) {
    return 0;
  }

  // The last allocation is excluded from the compaction since it's currently in
  // use for serving allocations.
  const auto allocations{hsa_->pool_.numRanges() - 1};
  if (allocations == 0) {
    return 0;
  }
  compactors_.clear();
  compactors_.reserve(allocations);

  // TODO naming: SortableEntry -> AllocationState
  struct SortableEntry {
    int64_t size;
    int64_t nonFreeBlockSize;
    int32_t allocationIndex;
  };
  std::vector<SortableEntry> entries;
  entries.reserve(allocations);

  // TODO naming: usable -> free
  int64_t remainingUsableSize{0};
  int64_t totalNonFreeBlockSize{0};
  for (auto i = 0; i < allocations; ++i) {
    compactors_.emplace_back(hsa_->pool_.rangeAt(i));
    const auto& compactor{compactors_.back()};
    remainingUsableSize += compactor.usableSize();
    totalNonFreeBlockSize += compactor.nonFreeBlockSize();
    entries.push_back(
        SortableEntry{compactor.size(), compactor.nonFreeBlockSize(), i});
  }

  // Priority is given to allocations with larger sizes. When allocations have
  // identical sizes, those with less stored data take precedence.
  std::sort(
      entries.begin(),
      entries.end(),
      [](const SortableEntry& lhs, const SortableEntry& rhs) {
        if (lhs.size == rhs.size) {
          return lhs.nonFreeBlockSize < rhs.nonFreeBlockSize;
        }
        return lhs.size > rhs.size;
      });

  int64_t reclaimableSize{0};
  for (const auto& entry : entries) {
    auto& compactor{compactors_[entry.allocationIndex]};
    if (remainingUsableSize - compactor.usableSize() >= totalNonFreeBlockSize) {
      remainingUsableSize -= compactor.usableSize();
      compactor.setReclaimable();
      reclaimableSize += compactor.size();
    }
  }
  return reclaimableSize;
}

// TODO: Update cumulativeBytes_.
std::pair<int64_t, folly::F14FastMap<Header*, Header*>>
AllocationCompactionStrategy::compact() {
  // TODO: move ac into hsa.
  using AC = AllocationCompactor;

  // TODO: If this is necessary.
  if (estimateReclaimableSize() == 0) {
    return {0, {}};
  }

  AC::HeaderMap movedBlocks;
  // TODO: who points to who
  AC::HeaderMap multipartMap;
  // Accumulate multipart map from all alloctions.
  for (const auto& compactor : compactors_) {
    compactor.accumulateMultipartMap(multipartMap);
  }

  // TODO: below is
  // 1. remove free blocks from free list
  // 2. compact unreclaimable allocations and collect free blocks
  std::queue<Header*> destBlocks;
  for (auto& compactor : compactors_) {
    // Remove all the free blocks in candidate allocations from free list since:
    // 1. Reclaimable allocations will be freed to AllocationPool;
    // 2. Unreclaimable allocations's free blocks will be squeezed, at the
    // end of the compaction the remaining free blocks will be added back to
    // free list.
    // TODO: consider move into AC
    compactor.foreachBlock([&](Header* header) {
      if (header->isFree()) {
        hsa_->removeFromFreeList(header);
        header->setFree();
        --hsa_->numFree_;
        hsa_->freeBytes_ -= sizeof(Header) + header->size();
      }
    });

    // Squeeze unreclaimable allocations, whose remaining free blocks will be
    // the destination blocks for the compaction.
    if (!compactor.isReclaimable()) {
      auto freeBlocks = compactor.squeeze(multipartMap, movedBlocks);
      for (auto* freeBlock : freeBlocks) {
        destBlocks.push(freeBlock);
      }
    }
  }

  // TODO: below is moving reclaimale allocations' blocks to 'destBlocks'.
  int64_t compactedSize{0};
  Header* destBlock{nullptr};
  for (auto& compactor : compactors_) {
    if (!compactor.isReclaimable()) {
      continue;
    }

    auto srcBlock = compactor.nextBlock(false);
    int64_t srcOffset{0};
    Header** prevContPtr{nullptr};
    while (srcBlock != nullptr) {
      if (destBlock == nullptr) {
        VELOX_CHECK(!destBlocks.empty());
        destBlock = destBlocks.front();
        destBlocks.pop();
      }

      VELOX_CHECK_EQ((srcOffset == 0), (prevContPtr == nullptr));
      // TODO: consider std::tie
      auto moveResult = AC::moveBlock(
          srcBlock,
          srcOffset,
          prevContPtr,
          destBlock,
          multipartMap,
          movedBlocks);
      srcOffset += moveResult.srcMovedSize;
      if (moveResult.prevContPtr != nullptr) {
        VELOX_CHECK_GT(moveResult.srcMovedSize, 0);
        VELOX_CHECK_LT(srcOffset, srcBlock->size());
        prevContPtr = moveResult.prevContPtr;
      }
      if (srcOffset == srcBlock->size()) {
        srcBlock = compactor.nextBlock(false, srcBlock);
        srcOffset = 0;
        prevContPtr = nullptr;
      }
      destBlock = moveResult.remainingDestBlock;
    }
    compactedSize += compactor.size();
  }

  testCheckFreeInCurrentRange();
  addFreeBlocksToFreeList();

  // Free empty allocations.
  for (int32_t i = compactors_.size() - 1; i >= 0; --i) {
    if (compactors_[i].isReclaimable()) {
      LOG(INFO) << "Freeing allocation " << i << " with size of "
                << compactors_[i].size();
      hsa_->pool_.freeRangeAt(i);
    }
  }
  compactors_.clear();

  hsa_->checkConsistency();
  return {compactedSize, movedBlocks};
};

void AllocationCompactionStrategy::testCheckFreeInCurrentRange() const {
  for (auto i = 0; i < HashStringAllocator::kNumFreeLists; ++i) {
    auto* item = hsa_->free_[i].next();
    while (item != &hsa_->free_[i]) {
      auto header = HashStringAllocator::headerOf(item);
      VELOX_CHECK(hsa_->pool_.isInCurrentRange(header));
      item = item->next();
    }
  }
}

void AllocationCompactionStrategy::addFreeBlocksToFreeList() {
  for (auto& compactor : compactors_) {
    if (compactor.isReclaimable()) {
      continue;
    }
    compactor.foreachBlock([&](Header* header) {
      if (!header->isFree()) {
        return;
      }
      header->clearFree();
      hsa_->free(header);
    });
  }
}

AllocationCompactor::AllocationCompactor(AllocationRange allocationRange)
    : range_{allocationRange} {
  if (size() >= kHugePageSize) {
    VELOX_CHECK_EQ(0, size() % kHugePageSize);
  }

  // Collects allocation info.
  int64_t nonFreeBlockSize{0};
  foreachBlock([&nonFreeBlockSize](Header* header) {
    if (!header->isFree()) {
      nonFreeBlockSize += header->size() + sizeof(Header);
    }
  });
  nonFreeBlockSize_ = nonFreeBlockSize;
}

void AllocationCompactor::accumulateMultipartMap(
    HeaderMap& multipartMap) const {
  foreachBlock([&multipartMap](Header* header) {
    if (!header->isContinued()) {
      return;
    }
    const auto nextContinued{header->nextContinued()};
    VELOX_CHECK(!multipartMap.contains(nextContinued));
    multipartMap[nextContinued] = header;
  });
}

void AllocationCompactor::foreachBlock(
    folly::Range<char*> range,
    const std::function<void(Header*)>& func) {
  for (int64_t subRangeOffset = 0; subRangeOffset < range.size();
       subRangeOffset += kHugePageSize) {
    auto header = reinterpret_cast<Header*>(range.data() + subRangeOffset);
    while (header) {
      func(header);
      header = header->next();
    }
  }
}

Header* AllocationCompactor::squeezeArena(
    AllocationRange arena,
    HeaderMap& multipartMap,
    HeaderMap& movedBlocks) {
  VELOX_CHECK(
      reinterpret_cast<Header*>(arena.data() + arena.size())->isArenaEnd());

  auto offsetFromArenaStart = [&arena](Header* header) {
    return reinterpret_cast<char*>(header) - arena.data();
  };

  // Returns the next non free block in the arena. Returns nullptr if there is
  // no. 'header' should be the header of a block within the arena. If 'header'
  // is nullptr, returns the first non free block in arena.
  auto nextNonFreeBlockInArena = [&](Header* header) {
    if (header != nullptr) {
      auto nextNonFreeBlock = nextBlock(false, header);
      if (nextNonFreeBlock == nullptr ||
          offsetFromArenaStart(nextNonFreeBlock) >= arena.size()) {
        return static_cast<Header*>(nullptr);
      }
      return nextNonFreeBlock;
    }

    header = reinterpret_cast<Header*>(arena.data());
    while (header) {
      if (!header->isFree()) {
        return header;
      }
      header = header->next();
    }
    return static_cast<Header*>(nullptr);
  };

  auto movedFrom = nextNonFreeBlockInArena(nullptr);
  int64_t movedToOffset{0};
  while (movedFrom != nullptr) {
    const auto movedFromOffset{offsetFromArenaStart(movedFrom)};
    if (movedFromOffset == movedToOffset) {
      movedToOffset = movedFrom->end() - arena.data();
      movedFrom = nextNonFreeBlockInArena(movedFrom);
      continue;
    }
    VELOX_CHECK_GT(movedFromOffset, movedToOffset);

    auto movedTo = reinterpret_cast<Header*>(arena.data() + movedToOffset);
    auto nextNonFreeBlock = nextNonFreeBlockInArena(movedFrom);
    updateMap(movedFrom, movedTo, multipartMap, movedBlocks);
    // Don't use memcpy here since 'movedTo' and 'movedFrom' might overlap.
    memmove(movedTo, movedFrom, sizeof(Header) + movedFrom->size());
    movedTo->clearPreviousFree();

    movedFrom = nextNonFreeBlock;
    movedToOffset = movedTo->end() - arena.data();
  }

  if (movedToOffset == arena.size()) {
    return nullptr;
  }

  const auto remainingSize{arena.size() - movedToOffset};
  VELOX_CHECK_GE(remainingSize, kMinBlockSize);

  auto freeBlock =
      new (arena.data() + movedToOffset) Header(remainingSize - sizeof(Header));
  freeBlock->setFree();
  // TODO: Size at the end is not set.
  return freeBlock;
}

std::vector<HashStringAllocator::Header*> AllocationCompactor::squeeze(
    HeaderMap& multipartMap,
    HeaderMap& movedBlocks) {
  std::vector<HashStringAllocator::Header*> freeBlocks;
  for (int64_t subRangeOffset = 0; subRangeOffset < size();
       subRangeOffset += kHugePageSize) {
    const auto arenaStart = range_.data() + subRangeOffset;
    const auto arenaSize =
        std::min<int64_t>(range_.size(), kHugePageSize) - simd::kPadding;
    const auto arena = folly::Range<char*>(arenaStart, arenaSize);

    auto freeBlock = squeezeArena(arena, multipartMap, movedBlocks);
    if (freeBlock != nullptr) {
      freeBlocks.push_back(freeBlock);
    }
  }
  return freeBlocks;
}

void AllocationCompactor::updateMapAsNext(
    Header* from,
    Header* to,
    HeaderMap& multipartMap,
    HeaderMap& movedBlocks) {
  if (!multipartMap.contains(from)) {
    VELOX_CHECK(!movedBlocks.contains(from));
    movedBlocks[from] = to;
    return;
  }

  auto prevHeader = multipartMap[from];
  VELOX_CHECK(!prevHeader->isFree());
  VELOX_CHECK(!prevHeader->isArenaEnd());
  VELOX_CHECK(prevHeader->isContinued());
  VELOX_CHECK_EQ(
      reinterpret_cast<void*>(prevHeader->nextContinued()),
      reinterpret_cast<void*>(from));
  prevHeader->setNextContinued(to);
  multipartMap.erase(from);
  multipartMap[to] = prevHeader;
}

void AllocationCompactor::updateMapAsPrevious(
    Header* from,
    Header* to,
    HeaderMap& multipartMap) {
  VELOX_CHECK(from->isContinued());
  auto nextHeader = from->nextContinued();
  VELOX_CHECK(!nextHeader->isFree());
  VELOX_CHECK(!nextHeader->isArenaEnd());
  VELOX_CHECK(multipartMap.contains(nextHeader));
  multipartMap[nextHeader] = to;
}

void AllocationCompactor::updateMap(
    Header* from,
    Header* to,
    HeaderMap& multipartMap,
    HeaderMap& movedBlocks) {
  updateMapAsNext(from, to, multipartMap, movedBlocks);

  if (from->isContinued()) {
    updateMapAsPrevious(from, to, multipartMap);
  }
}

Header* AllocationCompactor::nextBlock(bool isFree, Header* header) const {
  if (header != nullptr) {
    VELOX_CHECK_GE(reinterpret_cast<char*>(header), range_.data());
    VELOX_CHECK_LT(
        reinterpret_cast<char*>(header),
        range_.data() + range_.size() - simd::kPadding);
    header = reinterpret_cast<Header*>(header->end());
  } else {
    header = reinterpret_cast<Header*>(range_.data());
  }

  while (!header->isArenaEnd()) {
    if (isFree == header->isFree()) {
      return header;
    }
    header = reinterpret_cast<Header*>(header->end());
  }

  const auto boundary = reinterpret_cast<char*>(header) + simd::kPadding;
  VELOX_CHECK_LE(boundary, range_.data() + range_.size());
  if (boundary == range_.data() + range_.size()) {
    return nullptr;
  }

  auto arenaStart = boundary;
  while (arenaStart < range_.data() + range_.size()) {
    header = reinterpret_cast<Header*>(arenaStart);
    while (!header->isArenaEnd()) {
      if (isFree == header->isFree()) {
        return header;
      }
      header = reinterpret_cast<Header*>(header->end());
    }
    arenaStart += kHugePageSize;
  }
  return nullptr;
}

AllocationCompactor::MoveResult AllocationCompactor::moveBlock(
    Header* srcBlock,
    int64_t srcOffset,
    Header** prevContPtr,
    Header* destBlock,
    HeaderMap& multipartMap,
    HeaderMap& movedBlocks) {
  // Sanity check.
  VELOX_CHECK(!srcBlock->isFree());
  VELOX_CHECK(destBlock->isFree());
  VELOX_CHECK(srcOffset < srcBlock->size());
  VELOX_CHECK((srcOffset > 0) == (prevContPtr != nullptr));
  // TODO: check srcBlock and destBlock is not overlapped

  // Determine move size.
  const auto bytesToMove{srcBlock->size() - srcOffset};
  auto movableSize = detail::tryAccommodate(
      destBlock->size(), bytesToMove, srcBlock->isContinued());
  if (movableSize == 0) {
    VELOX_CHECK(srcBlock->isContinued());
    return {0, prevContPtr, nullptr, destBlock->size()};
  }

  VELOX_CHECK_LE(movableSize, bytesToMove);
  const auto movingRestOfSrc = (movableSize == bytesToMove);
  // When moving final part of non-continued block, 'bytesTomove' may be less
  // than kMinAlloc, which will need padding to make the result block valid.
  const auto needsPadding = (bytesToMove < HashStringAllocator::kMinAlloc);
  if (needsPadding) {
    VELOX_CHECK(!srcBlock->isContinued());
    VELOX_CHECK(movingRestOfSrc);
  }

  // Update map
  if (srcOffset == 0) {
    updateMapAsNext(srcBlock, destBlock, multipartMap, movedBlocks);
  }
  if (movingRestOfSrc && srcBlock->isContinued()) {
    // Moving last part of a continued 'srcBlock', and this is previous.
    // Update next->previous
    updateMapAsPrevious(srcBlock, destBlock, multipartMap);
  }

  auto destSizeNeeded = movableSize;
  if (needsPadding) {
    VELOX_CHECK_LE(movableSize, HashStringAllocator::kMinAlloc);
    destSizeNeeded = HashStringAllocator::kMinAlloc;
  } else if (!movingRestOfSrc) {
    destSizeNeeded += Header::kContinuedPtrSize;
  }
  VELOX_CHECK_LE(destSizeNeeded, destBlock->size());

  const auto remainingDestSize{destBlock->size() - destSizeNeeded};
  Header* remainingDestBlock{nullptr};
  auto destNewSize{destBlock->size()};
  if (remainingDestSize >= kMinBlockSize) {
    // Create a new free block at the end of moved block.
    // TODO: size at the end.
    remainingDestBlock = new (destBlock->begin() + destSizeNeeded)
        Header(destBlock->size() - destSizeNeeded - sizeof(Header));
    remainingDestBlock->setFree();
    destNewSize = destSizeNeeded;
  }

  // Actual move.
  memcpy(destBlock->begin(), srcBlock->begin() + srcOffset, movableSize);
  destBlock->clearFree();
  if (movingRestOfSrc && !srcBlock->isContinued()) {
    destBlock->clearContinued();
  } else {
    destBlock->setContinued();
  }
  destBlock->setSize(destNewSize);

  if (prevContPtr != nullptr) {
    *prevContPtr = destBlock;
  }

  Header** resultPrevContPtr{nullptr};
  if (!movingRestOfSrc) {
    resultPrevContPtr = reinterpret_cast<Header**>(
        destBlock->end() - Header::kContinuedPtrSize);
  }

  auto destDiscardedSize{0};
  // 'srcBlock' is splitted for this move and there is still 'destBlock'
  // remaining, means that 'srcBlock' is splitted to not break the invariance of
  // HSA: a block has at least kMinAlloc bytes as data.
  const auto discardRemaining = (!movingRestOfSrc) && (remainingDestSize > 0);
  if (discardRemaining) {
    // TODO: this does not compile
    // VELOX_CHECK_EQ(remainingDestSize, AllocationCompactor::kMinBlockSize);
    remainingDestBlock = nullptr;
    destDiscardedSize = remainingDestSize;
  }

  return {
      movableSize, resultPrevContPtr, remainingDestBlock, destDiscardedSize};
}

} // namespace facebook::velox
