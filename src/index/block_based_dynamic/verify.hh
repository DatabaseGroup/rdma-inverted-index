#ifndef INDEX_BLOCK_BASED_DYNAMIC_VERIFY_HH
#define INDEX_BLOCK_BASED_DYNAMIC_VERIFY_HH

#include <library/memory_region.hh>
#include <library/types.hh>

#include "compute_thread.hh"
#include "index/query/query.hh"
#include "remote_pointer.hh"

namespace inv_index::block_based::dynamic {

bool verify_block(RemotePtr::BufferBlock& block, u32 id) {
  const u32 entries = block.block_length - DYNAMIC_FOOTER_SIZE / sizeof(u32);
  u32 previous_entry = 0;

  for (u32 idx = 0; idx < entries; ++idx) {
    if (idx % CACHE_LINE_ITEMS != 0) {
      const u32 entry = block.buffer[idx];

      if (entry == static_cast<u32>(-1)) {
        break;
      }

      lib_assert(previous_entry < entry, "unordered block");

      if (entry == id) {
        return true;
      }

      previous_entry = entry;
    }
  }

  return false;
}

void verify(query::Queries& queries,
            RemotePointers& remote_pointers_,
            u_ptr<ComputeThread>& thread,
            MemoryRegionTokens& remote_access_tokens) {
  u32 cnt = 0;

  for (auto& query : queries) {
    if (query.type == QueryType::INSERT) {
      if (cnt++ % std::max<u32>(queries.size() / 10, 1) == 0) {
        std::cerr << "verify query " << query << std::endl;
      }

      for (u32 k_idx = 0; k_idx < query.size(); ++k_idx) {
        u32 node = remote_pointers_[query.keys[k_idx]].memory_node;
        u32 offset = remote_pointers_[query.keys[k_idx]].offset;

        auto& block = thread->read_buffer.get_block(0, 0);
        bool verification_successful = false;

        do {
          RemotePtr r_ptr{node, offset};
          MRT& mrt = remote_access_tokens[node];

          // TODO: set NO_HANDLE
          r_ptr.READ_block(0, 0, mrt, thread);

          while (thread->post_balance > 0) {
            thread->poll_cq();
          }

          verification_successful |= verify_block(block, query.update_id);

          auto [next_node, next_offset] = block.get_remote_ptr();
          node = next_node;
          offset = next_offset;
        } while (!block.points_to_null() && !verification_successful);

        lib_assert(verification_successful, "verification failed");
      }
    }
  }
}

}  // namespace inv_index::block_based::dynamic

#endif  // INDEX_BLOCK_BASED_DYNAMIC_VERIFY_HH
