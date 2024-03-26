#ifndef RDMA_LIBRARY_DYNAMIC_REGION_ALLOCATOR_HH
#define RDMA_LIBRARY_DYNAMIC_REGION_ALLOCATOR_HH

#include <mutex>

#include "common/debug.hh"
#include "memory_region.hh"
#include "span.hh"
#include "types.hh"
#include "utils.hh"

template <typename BufferEntryType>
class DynamicRegionAllocator {
public:
  DynamicRegionAllocator(Context& context, u32 preallocate, size_t region_size)
      : context_(context),
        region_size_(region_size),
        region_length_(region_size / sizeof(BufferEntryType)) {
    // pre-allocate some regions
    for (u32 i = 0; i < preallocate; ++i) {
      allocate_region(true);
    }
  }

  u32 get_free_region_id() {
    u32 id;

    while (!free_list_.try_dequeue(id)) {
      allocate_region();
      lib_debug("allocated response regions: " +
                std::to_string(region_buffers_.size()));
    }

    return id;
  }

  void free_region(u32 region_id) { free_list_.enqueue(region_id); }

  LocalMemoryRegion* get_memory_region(u32 region_id) {
    return memory_regions_[region_id].get();
  }

  BufferEntryType* get_region_buffer(u32 region_id) {
    return region_buffers_[region_id].get();
  }

  size_t allocated_regions() const { return memory_regions_.size(); }

private:
  void allocate_region(bool touch = false) {
    std::scoped_lock lock{mutex_};
    // note that emplace_back of concurrent_vec returns an iterator
    auto buffer_ptr =
      region_buffers_.emplace_back(new BufferEntryType[region_length_]);
    memory_regions_.emplace_back(std::make_unique<LocalMemoryRegion>(
      context_, buffer_ptr->get(), region_size_));

    if (touch) {
      touch_memory(*buffer_ptr, region_length_);
    }

    free_list_.enqueue(region_buffers_.size() - 1);
  }

private:
  Context& context_;
  const size_t region_size_;
  const size_t region_length_;

  concurrent_vec<u_ptr<LocalMemoryRegion>> memory_regions_;
  concurrent_vec<u_ptr<BufferEntryType[]>> region_buffers_;
  concurrent_queue<u32> free_list_;
  std::mutex mutex_;
};

#endif  // RDMA_LIBRARY_DYNAMIC_REGION_ALLOCATOR_HH
