#include <algorithm>
#include <cassert>
#include <chrono>

#include "velox/common/memory/AllocationCompactor.h"

using namespace facebook::velox;

using Header = HashStringAllocator::Header;

folly::Random::DefaultGenerator rng;

// TODO: Stolen from HashStringAllocatorTest, consider move into util.
uint32_t rand32() {
  return folly::Random::rand32(rng);
}

double randDouble01() {
  return folly::Random::randDouble01(rng);
}

std::string randomString(int32_t size = 0) {
  std::string result;
  result.resize(
      size != 0
          ? size
          : 20 + (rand32() % 10 > 8 ? rand32() % 200 : 1000 + rand32() % 1000));
  for (auto i = 0; i < result.size(); ++i) {
    result[i] = 32 + (rand32() % 96);
  }
  return result;
}

void fillAllocation(
    HashStringAllocator* hsa,
    int64_t bytes,
    std::vector<std::vector<std::pair<Header*, std::string>>>& payloads,
    std::vector<size_t>& allocationsDataSize) {
  std::vector<Header*> headers;

  int64_t bytesAllocated{0};
  while (bytesAllocated < bytes) {
    const auto size = 16 + (rand32() % (128 - 16));
    auto header = hsa->allocate(size);
    assert(!header->isFree());
    assert(!header->isContinued());
    assert(header->size() == size);
    bytesAllocated += header->size();
    headers.push_back(header);
  }

  auto& pool = hsa->allocationPool();
  const auto allocations = pool.numRanges();
  std::map<char*, int32_t> rangeStartToAllocId;
  for (int32_t i = 0; i < allocations; ++i) {
    rangeStartToAllocId[pool.rangeAt(i).data()] = i;
  }

  payloads.clear();
  payloads.resize(allocations);
  allocationsDataSize.resize(allocations, 0);
  for (const auto header : headers) {
    auto it = rangeStartToAllocId.upper_bound(reinterpret_cast<char*>(header));
    --it;
    const auto allocationId{it->second};

    auto payload = randomString(header->size());
    memcpy(header->begin(), payload.data(), header->size());
    payloads[allocationId].emplace_back(header, std::move(payload));

    allocationsDataSize[allocationId] += header->size() + sizeof(Header);
  }
}

int64_t freeDataAtAllocation(
    HashStringAllocator* hsa,
    size_t bytesToFree,
    std::vector<std::pair<Header*, std::string>>& allocation_data) {
  std::random_shuffle(allocation_data.begin(), allocation_data.end());

  int64_t freedBytes{0};
  while (freedBytes < bytesToFree) {
    if (allocation_data.empty()) {
      break;
    }
    auto& [header, payload] = allocation_data.back();
    freedBytes += header->size() + sizeof(Header);
    hsa->free(header);
    allocation_data.pop_back();
  }
  return freedBytes;
}

// Verify all the content that's not been freed are still valid. Takes an map
// of moved blocks' header, which is the product of HSA::compact.
void verifyContent(
    const std::vector<std::vector<std::pair<Header*, std::string>>>& blockData,
    folly::F14FastMap<Header*, Header*>& movedBlocks) {
  for (const auto& allocationData : blockData) {
    for (const auto& [header, str] : allocationData) {
      if (str.empty()) {
        continue;
      }
      auto currentHeader = header;
      int32_t offset{0};
      if (movedBlocks.contains(currentHeader)) {
        currentHeader = movedBlocks[header];
      }
      while (true) {
        assert(!currentHeader->isFree());
        const auto sizeToCompare =
            std::min<int32_t>(currentHeader->usableSize(), str.size() - offset);
        assert(sizeToCompare > 0);
        assert(!memcmp(
            currentHeader->begin(), str.data() + offset, sizeToCompare));
        offset += sizeToCompare;
        if (offset == str.size()) {
          break;
        }
        assert(currentHeader->isContinued());
        currentHeader = currentHeader->nextContinued();
      }
    }
  }
}

int main() {
  // TODO: use velox_check
  memory::MemoryManager::initialize({});
  // TODO: lifetime
  auto pool = memory::memoryManager()->addLeafPool();
  auto hsa = std::make_unique<HashStringAllocator>(pool.get());
  rng.seed(1);

  // Stores all the data(Header and payload) created in HSA indexed by
  // allocation index.
  std::vector<std::vector<std::pair<Header*, std::string>>> payloads;
  // Total data size (header size + payload size) of each allocation.
  std::vector<size_t> allocationsDataSize;

  constexpr size_t payloadSize = 1LL << 31;
  // Fills >= 'payloadSize' bytes of random generated data into memory allocated
  // via HSA.
  LOG(INFO) << "Filling not less than " << payloadSize << " bytes to data...";
  fillAllocation(hsa.get(), payloadSize, payloads, allocationsDataSize);
  if (allocationsDataSize.size() <= 1) {
    LOG(ERROR)
        << "HashStringAllocator only allocated 1 allocation, cannot be compacted.";
    return 1;
  }
  LOG(INFO) << "Allocated " << payloads.size() << " allocations.";

  LOG(INFO) << "Randomly free data...";
  size_t totalFreed{0};
  for (size_t i = 0; i < allocationsDataSize.size() - 1; ++i) {
    const auto freeRatio = randDouble01();
    const auto freed = freeDataAtAllocation(
        hsa.get(), allocationsDataSize[i] * freeRatio, payloads[i]);
    LOG(INFO) << "Allocation " << i << ": Freed " << freed << " out of "
              << allocationsDataSize[i] << " bytes (" << freeRatio * 100
              << "%).";
    totalFreed += freed;
  }
  LOG(INFO) << "Freed " << totalFreed << " bytes in total.";

  AllocationCompactionStrategy compactor(hsa.get());
  hsa->allowSplittingContiguous();
  const auto estimatedReclaimable = compactor.estimateReclaimableSize();
  LOG(INFO) << "Estimated reclaimable data size:" << estimatedReclaimable;

  LOG(INFO) << "Starting compaction...";
  const auto startTime = std::chrono::steady_clock::now();
  auto [bytesFreed, updatedBlocks] = compactor.compact();
  assert(bytesFreed == estimatedReclaimable);
  const auto elapsed = std::chrono::steady_clock::now() - startTime;
  // TODO: Make elapsed millisecond.
  LOG(INFO) << "Compaction done, elapsed time: "
            << std::chrono::duration_cast<std::chrono::duration<double>>(
                   elapsed)
                   .count();

  LOG(INFO) << "Starting to verify payload...";
  verifyContent(payloads, updatedBlocks);
  LOG(INFO) << "Verification done.";

  assert(compactor.estimateReclaimableSize() == 0);

  return 0;
}
