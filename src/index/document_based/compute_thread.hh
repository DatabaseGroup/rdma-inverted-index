#ifndef INDEX_DOCUMENT_BASED_COMPUTE_THREAD_HH
#define INDEX_DOCUMENT_BASED_COMPUTE_THREAD_HH

#include <library/connection_manager.hh>
#include <library/context.hh>
#include <library/detached_qp.hh>
#include <library/thread.hh>
#include <library/utils.hh>

#include "index/constants.hh"
#include "timing/timing.hh"

namespace inv_index::document_based {

class ComputeThread : public Thread {
public:
  ComputeThread(u32 id,
                i32 max_send_queue_wr,
                u32 num_memory_nodes,
                size_t buffer_size,
                HugePage<u32>& local_buffer)
      : Thread(id),
        num_servers(num_memory_nodes),
        local_buffer_length(buffer_size / NUM_READ_BUFFERS / sizeof(u32)),
        local_buffers(NUM_READ_BUFFERS),
        posted_to_buffer(NUM_READ_BUFFERS),
        max_send_queue_wr_(max_send_queue_wr) {
    t_operation = std::make_shared<timing::Timing::Interval>("operation");
    t_read_list = std::make_shared<timing::Timing::Interval>("read_list");
    t_poll = std::make_shared<timing::Timing::Interval>("polling");

    send_wcs.resize(max_send_queue_wr);

    for (u32 i = 0; i < NUM_READ_BUFFERS; ++i) {
      local_buffers[i] = local_buffer.get_slice(buffer_size / NUM_READ_BUFFERS);
    }
  }

  void poll_cq() {
    Context::poll_send_cq(
      send_wcs.data(), max_send_queue_wr_, ctx->get_cq(), [&](u64 wr_id) {
        auto [ctx_offset, buffer_id] = decode_64bit(wr_id);
        auto& thread = ctx->registered_threads[ctx_offset];
        thread->posted_to_buffer[buffer_id]--;
        thread->post_balance--;
      });
  }

public:
  const u32 num_servers;
  const u32 local_buffer_length;
  vec<ibv_wc> send_wcs;

  vec<u32*> local_buffers;
  vec<std::atomic<u32>> posted_to_buffer;

  u64 local_num_result{0};
  u64 rdma_reads_in_bytes{0};
  u64 processed_queries{0};
  std::atomic<i32> post_balance{0};

  // initializes the query_handler
  SharedContext<ComputeThread>* ctx{nullptr};
  u32 ctx_tid{};

  timing::Timing::IntervalPtr t_operation;
  timing::Timing::IntervalPtr t_read_list;
  timing::Timing::IntervalPtr t_poll;

private:
  const i32 max_send_queue_wr_;
};

}  // namespace inv_index::document_based

#endif  // INDEX_DOCUMENT_BASED_COMPUTE_THREAD_HH
