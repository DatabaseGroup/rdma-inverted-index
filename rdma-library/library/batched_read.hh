#ifndef RDMA_LIBRARY_BATCHED_READ_HH
#define RDMA_LIBRARY_BATCHED_READ_HH

#include "queue_pair.hh"
#include "utils.hh"

struct BatchedREAD {
  const u32 max_size;
  u32 requests{0};
  u64 total_size{0};

  vec<ibv_send_wr> work_requests;
  vec<ibv_sge> scatter_gather_entries;
  ibv_send_wr* bad_work_request{nullptr};

  explicit BatchedREAD(size_t max_batch_size)
      : max_size(max_batch_size),
        work_requests(max_batch_size),
        scatter_gather_entries(max_batch_size) {}

  void add_to_batch(u64 local_address,
                    u64 remote_address,
                    u32 length,
                    u32 lkey,
                    u32 rkey,
                    u64 wr_id,
                    bool signaled = true) {
    lib_assert(length > 0, "Cannot READ 0 bytes");
    lib_assert(requests < max_size, "Batch exceeds maximum batch size");

    auto& sge = scatter_gather_entries[requests];
    auto& wr = work_requests[requests];

    sge.addr = local_address;
    sge.length = length;
    sge.lkey = lkey;

    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    wr.next = nullptr;
    if (requests > 0) {
      work_requests[requests - 1].next = &wr;
    }

    wr.wr.rdma.remote_addr = remote_address;
    wr.wr.rdma.rkey = rkey;

    ++requests;
    total_size += length;
  }

  void post_batch(QP& qp) {
    lib_assert(requests > 0, "Empty READ batch");

    // send final WR signaled in any case
    work_requests[requests - 1].send_flags = IBV_SEND_SIGNALED;

    lib_assert(
      ibv_post_send(
        qp->get_ibv_qp(), work_requests.data(), &bad_work_request) == 0,
      "Cannot post send request");

    // reset batch
    requests = 0;
    total_size = 0;
  }
};

#endif  // RDMA_LIBRARY_BATCHED_READ_HH
