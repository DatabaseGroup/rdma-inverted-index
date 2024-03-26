#ifndef INDEX_TERM_BASED_COMPUTE_THREAD_HH
#define INDEX_TERM_BASED_COMPUTE_THREAD_HH

#include <library/connection_manager.hh>
#include <library/hugepage.hh>
#include <library/thread.hh>

#include "index/shared_context.hh"
#include "timing/timing.hh"

namespace inv_index::term_based {

class ComputeThread : public Thread {
public:
  ComputeThread(u32 id,
                i32 max_send_queue_wr,
                size_t buffer_size,
                HugePage<u32>& local_buffer)
      : Thread(id),
        buffer_length(buffer_size / sizeof(u32)),
        max_send_queue_wr_(max_send_queue_wr) {
    t_operation = std::make_shared<timing::Timing::Interval>("operation");
    t_read_list = std::make_shared<timing::Timing::Interval>("read_list");
    t_poll = std::make_shared<timing::Timing::Interval>("polling");

    send_wcs.resize(max_send_queue_wr);
    buffer_ptr = local_buffer.get_slice(buffer_size);
  }

  void poll_cq() {
    Context::poll_send_cq(
      send_wcs.data(), max_send_queue_wr_, ctx->get_cq(), [&](u64 ctx_offset) {
        ctx->registered_threads[ctx_offset]->post_balance--;
      });
  }

public:
  vec<ibv_wc> send_wcs;
  const u32 buffer_length;
  u32* buffer_ptr;

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

}  // namespace inv_index::term_based

#endif  // INDEX_TERM_BASED_COMPUTE_THREAD_HH
