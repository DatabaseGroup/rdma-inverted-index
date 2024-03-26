#ifndef INDEX_BLOCK_BASED_DYNAMIC_FREE_LIST_HH
#define INDEX_BLOCK_BASED_DYNAMIC_FREE_LIST_HH

#include <iostream>
#include <library/types.hh>
#include <random>

#include "compute_thread.hh"

namespace inv_index::block_based::dynamic::freelist {

// one free list per memory node that looks as follows:
// head (64) | block0-next (32) | block1-next (32) | ...
class FreeList {
public:
  FreeList(u32 block_size,
           u32 memory_node,
           u32 offset,
           MRT& remote_access_token)
      : block_size_(block_size),
        memory_node_(memory_node),
        offset_(offset),
        access_token_(remote_access_token) {
    // initialize PRNG
    dist_ = std::uniform_int_distribution<u32>(0, FREELIST_PARTITIONS - 1);
  }

  // returns the (raw) remote offset to the allocated buffer
  u64 allocate(u_ptr<ComputeThread>& thread) {
    thread->remote_allocations++;

    auto& flb = thread->free_list_buffers[memory_node_];
    QP& qp = thread->qps[memory_node_]->qp;
    u32 head;

    do {
      const u64 head_offset = get_random_head_offset();

      // READ head
      thread->post_balance++;
      qp->post_send_with_id(flb->memory_region,
                            sizeof(u64),
                            IBV_WR_RDMA_READ,
                            WR_READ_NO_HANDLE,
                            true,
                            access_token_.get(),
                            head_offset);

      // wait for head
      while (thread->post_balance > 0) {
        thread->poll_cq_and_handle();
      }

      head = flb->buffers.head;
      lib_assert(head != TOMBSTONE, "memory node out of memory");

      // READ head->next
      thread->post_balance++;
      qp->post_send_with_id(flb->memory_region,
                            sizeof(u32),
                            IBV_WR_RDMA_READ,
                            WR_READ_NO_HANDLE,
                            true,
                            access_token_.get(),
                            get_head_next_offset(head),
                            sizeof(u64));

      // wait for head->next
      while (thread->post_balance > 0) {
        thread->poll_cq_and_handle();
      }

      // swap head with head->next
      thread->post_balance++;
      thread->post_balance_CAS++;
      qp->post_CAS(flb->memory_region,
                   access_token_.get(),
                   head_offset,
                   head,
                   flb->buffers.head_next);

      // synchronous CAS
      while (thread->post_balance_CAS > 0) {
        thread->poll_cq_and_handle();
      }

      // CAS writes the old value into the buffer
    } while (flb->buffers.head != head);

    return flb->buffers.head * block_size_;
  }

  void deallocate(u64 raw_offset, u_ptr<ComputeThread>& thread) {
    thread->remote_deallocations++;

    auto& flb = thread->free_list_buffers[memory_node_];
    QP& qp = thread->qps[memory_node_]->qp;

    u32 new_head = raw_offset / block_size_;
    u32 current_head;

    const u64 head_offset = get_random_head_offset();

    do {
      // READ head
      thread->post_balance++;
      qp->post_send_with_id(flb->memory_region,
                            sizeof(u64),
                            IBV_WR_RDMA_READ,
                            WR_READ_NO_HANDLE,
                            true,
                            access_token_.get(),
                            head_offset);

      // wait for head
      while (thread->post_balance > 0) {
        thread->poll_cq_and_handle();
      }

      current_head = flb->buffers.head;

      // WRITE current-head as next-ptr to new-head
      //      thread->post_balance++;
      qp->post_send_inlined(std::addressof(current_head),
                            sizeof(u32),
                            IBV_WR_RDMA_WRITE,
                            false,  // I believe this can be unsignaled ...
                            access_token_.get(),
                            get_head_next_offset(new_head));

      // swap head with new-head, CAS guarantees sequential consistency
      thread->post_balance++;
      thread->post_balance_CAS++;
      qp->post_CAS(flb->memory_region,
                   access_token_.get(),
                   head_offset,
                   current_head,
                   new_head);

      // synchronous CAS
      while (thread->post_balance_CAS > 0) {
        thread->poll_cq_and_handle();
      }
      // CAS writes the old value into the buffer
    } while (flb->buffers.head != current_head);
  }

private:
  u64 get_address() const {
    return access_token_->address + static_cast<u64>(offset_) * block_size_;
  }

  u64 get_first_head_offset() const {
    return static_cast<u64>(offset_) * block_size_;
  }

  u64 get_random_head_offset() {
    u32 random_head = dist_(generator_);  // in [0, num_heads)
    return get_first_head_offset() + random_head * sizeof(u64);
  }

  u64 get_head_next_offset(u32 head) const {
    return get_first_head_offset() + sizeof(u64) * FREELIST_PARTITIONS +
           head * sizeof(u32);
  }

private:
  const u32 block_size_;
  const u32 memory_node_;
  const u32 offset_;

  MRT& access_token_;
  std::mt19937 generator_{std::random_device{}()};
  std::uniform_int_distribution<u32> dist_;
};

}  // namespace inv_index::block_based::dynamic::freelist

#endif  // INDEX_BLOCK_BASED_DYNAMIC_FREE_LIST_HH
