#ifndef INDEX_TERM_BASED_REMOTE_POINTER_HH
#define INDEX_TERM_BASED_REMOTE_POINTER_HH

#include <library/memory_region.hh>
#include <library/types.hh>
#include <ostream>

#include "compute_thread.hh"

namespace inv_index::term_based {

struct RemotePtr {
  u32 memory_node;
  u32 length;
  u64 offset;  // we need more than 32b for the address space of a memory node

  bool is_null() const {
    return memory_node == 0 && offset == 0 && length == 0;
  }

  void READ_list(u64 buffer_offset,
                 MRT& mrt,
                 u_ptr<ComputeThread>& thread,
                 bool signaled = true) const {
    lib_assert(buffer_offset + length <= thread->buffer_length,
               "READ result exceeds local buffer size (offset: " +
                 std::to_string(buffer_offset) +
                 ", length: " + std::to_string(length) + ")");

    thread->post_balance++;
    thread->rdma_reads_in_bytes += length * sizeof(u32);

    QP& qp = thread->ctx->qps[memory_node]->qp;
    qp->post_send(reinterpret_cast<u64>(thread->buffer_ptr),
                  length * sizeof(u32),
                  thread->ctx->get_lkey(),
                  IBV_WR_RDMA_READ,
                  signaled,
                  false,
                  mrt.get(),
                  offset,
                  buffer_offset * sizeof(u32),
                  thread->ctx_tid);
  }

  friend std::ostream& operator<<(std::ostream& os, const RemotePtr& r) {
    if (r.is_null()) {
      os << "not initialized";

    } else {
      os << "[node: " << r.memory_node << ", offset: " << r.offset
         << ", length: " << r.length << "]";
    }

    return os;
  }
};

using RemotePointers = vec<RemotePtr>;

}  // namespace inv_index::term_based

#endif  // INDEX_TERM_BASED_REMOTE_POINTER_HH
