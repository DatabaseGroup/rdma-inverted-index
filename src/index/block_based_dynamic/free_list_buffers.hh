#ifndef INDEX_BLOCK_BASED_DYNAMIC_FREE_LIST_BUFFERS_HH
#define INDEX_BLOCK_BASED_DYNAMIC_FREE_LIST_BUFFERS_HH

#include <library/memory_region.hh>

namespace inv_index::block_based::dynamic::freelist {
constexpr static u32 TOMBSTONE = static_cast<u32>(-1);

struct FreeListBuffers {
  struct Buffers {
    u64 head{};
    u32 head_next{};
  };

  Buffers buffers{};
  LocalMemoryRegion memory_region;

  explicit FreeListBuffers(Context& context)
      : memory_region(context, std::addressof(buffers), sizeof(Buffers)) {}
};

}  // namespace inv_index::block_based::dynamic::freelist

#endif  // INDEX_BLOCK_BASED_DYNAMIC_FREE_LIST_BUFFERS_HH
