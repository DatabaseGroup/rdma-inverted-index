#ifndef INDEX_BLOCK_BASED_COMPUTE_THREAD_HH
#define INDEX_BLOCK_BASED_COMPUTE_THREAD_HH

#include <library/connection_manager.hh>
#include <library/thread.hh>

#include "index/shared_context.hh"
#include "read_buffer.hh"
#include "timing/timing.hh"
#include "wr_ids.hh"

namespace inv_index::block_based {

class ComputeThread : public Thread {
public:
  ComputeThread(u32 id,
                i32 max_send_queue_wr,
                u32 block_size,
                HugePage<u32>& local_buffer)
      : Thread(id),
        read_buffer(block_size, local_buffer),
        max_send_queue_wr_(max_send_queue_wr) {
    t_operation = std::make_shared<timing::Timing::Interval>("operation");
    t_read_list = std::make_shared<timing::Timing::Interval>("read_list");
    t_poll = std::make_shared<timing::Timing::Interval>("polling");

    send_wcs.resize(max_send_queue_wr);
  }

  void poll_cq_and_handle() {
    Context::poll_send_cq(
      send_wcs.data(), max_send_queue_wr_, ctx->get_cq(), [&](u64 wr_id) {
        auto [ctx_offset, col, row] = decode_wr_id(wr_id);
        auto& thread = ctx->registered_threads[ctx_offset];
        thread->read_buffer.set_block_ready(col, row);
        thread->post_balance--;
      });
  }

public:
  vec<ibv_wc> send_wcs;
  ReadBuffer<false> read_buffer;

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

}  // namespace inv_index::block_based

#endif  // INDEX_BLOCK_BASED_COMPUTE_THREAD_HH
