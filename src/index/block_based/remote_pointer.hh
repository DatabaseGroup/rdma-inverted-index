#ifndef INDEX_BLOCK_BASED_REMOTE_POINTER_HH
#define INDEX_BLOCK_BASED_REMOTE_POINTER_HH

#include <library/memory_region.hh>
#include <ostream>

#include "wr_ids.hh"

namespace inv_index::block_based {

struct RemotePtr {
  u32 memory_node;
  u32 offset;  // 32b are sufficient to address the blocks in a memory node
               // (we assume that the block size is sufficiently large)
               // offset is just the number of the block (not an address offset)

  static inline u32 block_size;

  // this only works for remote pointers contained in blocks (not in the
  // catalog), because (0, 0) is always the very first block due to the
  // partitioning scheme, so no block can point to a previous block
  bool is_null() const { return memory_node == 0 && offset == 0; }

  template <typename ComputeThreadPtr>
  void READ_block(u32 col, u32 row, MRT& mrt, ComputeThreadPtr& thread) {
    auto& block = thread->read_buffer.get_block(col, row);
    block.ready = false;

    thread->post_balance++;
    thread->rdma_reads_in_bytes += block_size;

    const u64 wr_id = encode_wr_id(thread->ctx_tid, col, row);
    QP& qp = thread->ctx->qps[memory_node]->qp;

    qp->post_send(block.get_address(),
                  block_size,
                  thread->ctx->get_lkey(),
                  IBV_WR_RDMA_READ,
                  true,
                  false,
                  mrt.get(),
                  offset * static_cast<u64>(block_size),
                  0,
                  wr_id);

    // because offset is block-wise
    // caution: offset * block_size must be u64
  }

  friend std::ostream& operator<<(std::ostream& os, const RemotePtr& r) {
    os << "[node: " << r.memory_node << ", offset: " << r.offset << "]";

    if (r.is_null()) {
      os << " (possibly null)";
    }

    return os;
  }
};

using RemotePointers = vec<RemotePtr>;

}  // namespace inv_index::block_based

#endif  // INDEX_BLOCK_BASED_REMOTE_POINTER_HH
