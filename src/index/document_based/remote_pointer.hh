#ifndef INDEX_DOCUMENT_BASED_REMOTE_POINTER_HH
#define INDEX_DOCUMENT_BASED_REMOTE_POINTER_HH

#include <library/memory_region.hh>
#include <library/types.hh>
#include <ostream>

#include "compute_thread.hh"
#include "index/constants.hh"

namespace inv_index::document_based {

struct RemotePtr {
  u32 length;
  u64 offset;  // we need more than 32b for the address space of a memory node

  inline bool is_null() const { return offset == 0 && length == 0; }

  inline void READ_list(u64 buffer_offset,
                        u32 buffer_id,
                        QP& qp,
                        MRT& mrt,
                        u_ptr<ComputeThread>& thread,
                        bool signaled = true) const {
    lib_assert(length > 0, "Cannot READ 0 bytes");
    lib_assert(buffer_offset + length <= thread->local_buffer_length,
               "READ result exceeds local buffer size");

    thread->post_balance++;
    thread->rdma_reads_in_bytes += length * sizeof(u32);

    qp->post_send(reinterpret_cast<u64>(thread->local_buffers[buffer_id]),
                  length * sizeof(u32),
                  thread->ctx->get_lkey(),
                  IBV_WR_RDMA_READ,
                  signaled,
                  false,
                  mrt.get(),
                  offset,
                  buffer_offset * sizeof(u32),
                  encode_64bit(thread->ctx_tid, buffer_id));
  }

  friend std::ostream& operator<<(std::ostream& os, const RemotePtr& r) {
    if (r.is_null()) {
      os << "not initialized";

    } else {
      os << "[offset: " << r.offset << ", length: " << r.length << "]";
    }

    return os;
  }
};

using RemotePointers = vec<RemotePtr>;

}  // namespace inv_index::document_based

#endif  // INDEX_DOCUMENT_BASED_REMOTE_POINTER_HH
