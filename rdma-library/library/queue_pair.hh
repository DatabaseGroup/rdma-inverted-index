#ifndef RDMA_LIBRARY_QUEUE_PAIR_HH
#define RDMA_LIBRARY_QUEUE_PAIR_HH

#include <infiniband/verbs.h>

#include "context.hh"
#include "memory_region.hh"
#include "types.hh"

// forward declarations
class Context;
class MemoryRegion;
struct MemoryRegionToken;

constexpr u32 INLINE_SIZE = 256;
constexpr u32 MESSAGE_SIZE = 1073741824;  // = 1GB (is max on our machines)

struct QPInfo {
  u16 lid{0};
  u32 qp_number{0};
  u32 node_id{0};
};

class QueuePair {
public:
  explicit QueuePair(Context* context, bool use_shared_receive_cq = false);
  QueuePair(Context* context,
            ibv_cq* send_cq,
            ibv_cq* recv_cq,
            bool use_shared_receive_cq = false);

  QueuePair(const QueuePair&) = delete;
  QueuePair& operator=(const QueuePair&) = delete;
  ~QueuePair();

  u32 get_qp_num() { return queue_pair_->qp_num; }
  ibv_qp* get_ibv_qp() { return queue_pair_; }

  void transition_to_init();
  void transition_to_rtr(const QPInfo& remote_buffer);
  void transition_to_rts();

  void post_receive(MemoryRegion& region);
  void post_receive(MemoryRegion& region,
                    u32 size_in_bytes,
                    u64 wr_id = 0,
                    u64 local_offset = 0);
  u32 receive_u32(Context& context);

  void post_send_inlined(const void* address,
                         u32 size_in_bytes,
                         enum ibv_wr_opcode opcode,
                         bool signaled = true,
                         MemoryRegionToken* token = nullptr,
                         u64 remote_offset = 0,
                         u64 local_offset = 0,
                         u64 wr_id = 0);
  void post_send_u32(u32& value, bool signaled);
  void post_send(MemoryRegion& region,
                 enum ibv_wr_opcode opcode,
                 bool signaled = true,
                 MemoryRegionToken* token = nullptr,
                 u64 remote_offset = 0,
                 u64 local_offset = 0);
  void post_send(MemoryRegion& region,
                 u32 size_in_bytes,
                 enum ibv_wr_opcode opcode,
                 bool signaled = true,
                 MemoryRegionToken* token = nullptr,
                 u64 remote_offset = 0,
                 u64 local_offset = 0);
  void post_send_with_id(MemoryRegion& region,
                         u32 size_in_bytes,
                         enum ibv_wr_opcode opcode,
                         u64 wr_id,
                         bool signaled = true,
                         MemoryRegionToken* token = nullptr,
                         u64 remote_offset = 0,
                         u64 local_offset = 0);
  ibv_qp_init_attr get_qp_initial_attributes(ibv_cq* send_cq, ibv_cq* recv_cq);
  void post_send(u64 address,
                 u32 size,
                 u32 lkey,
                 enum ibv_wr_opcode opcode,
                 bool signaled,
                 bool inlined,
                 MemoryRegionToken* token,
                 u64 remote_offset,
                 u64 local_offset,
                 u64 wr_id);

  void post_CAS(MemoryRegion& local_region,
                MemoryRegionToken* remote_token,
                u64 remote_offset,
                u64 compare_to,
                u64 swap_with,
                bool signaled = true,
                u64 wr_id = 0);

private:
  Context* context_;
  const u16 lid_;
  const bool use_shared_receive_cq_;

  ibv_qp* queue_pair_{nullptr};
};

using QP = u_ptr<QueuePair>;
using QPs = vec<QP>;

#endif  // RDMA_LIBRARY_QUEUE_PAIR_HH
