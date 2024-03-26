#ifndef INDEX_BLOCK_BASED_DYNAMIC_COMPUTE_THREAD_HH
#define INDEX_BLOCK_BASED_DYNAMIC_COMPUTE_THREAD_HH

#include <library/connection_manager.hh>
#include <library/detached_qp.hh>
#include <library/thread.hh>
#include <random>

#include "free_list_buffers.hh"
#include "index/block_based/read_buffer.hh"
#include "timing/timing.hh"

namespace inv_index::block_based::dynamic {
constexpr static u64 WR_READ_NO_HANDLE = static_cast<u64>(-1);
constexpr static u64 WR_WRITE_ALLOCATION_BLOCK = static_cast<u64>(-1);

class ComputeThread : public Thread {
public:
  ComputeThread(u32 id,
                i32 max_send_queue_wr,
                Context& context,
                u32 block_size,
                HugePage<u32>& local_buffer)
      : Thread(id),
        local_context(context.get_config()),
        read_buffer(block_size, local_buffer),
        // register full buffer
        buffer_region(local_context,
                      local_buffer.get_full_buffer(),
                      local_buffer.buffer_size),
        cas_region(local_context, &cas_buffer, sizeof(u64)),
        allocation_buffer(std::make_unique<u32[]>(block_size / sizeof(u32))),
        allocation_block(allocation_buffer.get(), block_size),
        allocation_region(local_context, allocation_buffer.get(), block_size),
        max_send_queue_wr_(max_send_queue_wr) {
    t_operation = std::make_shared<timing::Timing::Interval>("operation");
    t_read_list = std::make_shared<timing::Timing::Interval>("read_list");
    t_poll = std::make_shared<timing::Timing::Interval>("polling");

    send_wcs.resize(max_send_queue_wr);
  }

  void connect_qps(Context& channel_context, ClientConnectionManager& cm) {
    // create for each memory node a new QP
    qps.reserve(cm.server_qps.size());
    for (QP& server_qp : cm.server_qps) {
      auto& qp = qps.emplace_back(
        std::make_unique<DetachedQP>(local_context,
                                     local_context.get_send_cq(),
                                     local_context.get_receive_cq()));
      qp->connect(channel_context, local_context.get_lid(), server_qp);

      // initialize free list buffers
      free_list_buffers.emplace_back(
        std::make_unique<freelist::FreeListBuffers>(local_context));
    }

    // initialize PRNG
    dist_ = std::uniform_int_distribution<u32>(0, qps.size() - 1);
  }

  void set_ready_and_validate(u64 wr_id) {
    auto [col, row] = decode_64bit(wr_id);
    auto& block = read_buffer.get_block(col, row);

    // READ the block again in case the arrived block is locked
    if (block.is_locked() || !block.validate_cache_lines()) {
      ++block_repeated_reads;
      ++post_balance;

      rdma_reads_in_bytes += read_buffer.block_size;
      QP& qp = qps[block.memory_node]->qp;

      qp->post_send(
        block.get_address(),
        read_buffer.block_size,
        buffer_region.get_lkey(),
        IBV_WR_RDMA_READ,
        true,
        false,
        block.mrt->get(),
        block.remote_offset * static_cast<u64>(read_buffer.block_size),
        0,
        wr_id);

    } else {
      read_buffer.set_block_ready(col, row);
    }
  }

  void poll_cq_and_handle() {
    i32 num_entries = ibv_poll_cq(
      local_context.get_send_cq(), max_send_queue_wr_, send_wcs.data());

    if (num_entries > 0) {
      // verify completion status
      for (i32 i = 0; i < num_entries; ++i) {
        lib_assert(send_wcs[i].status == IBV_WC_SUCCESS, "Send request failed");

        switch (send_wcs[i].opcode) {
        case IBV_WC_RDMA_READ: {
          const u64 wr_id = send_wcs[i].wr_id;
          if (wr_id != WR_READ_NO_HANDLE) {
            // call READ handler
            set_ready_and_validate(wr_id);
          }
          break;
        }
        case IBV_WC_COMP_SWAP:
          // call CAS handler
          --post_balance_CAS;
          break;
        case IBV_WC_RDMA_WRITE: {
          const u64 wr_id = send_wcs[i].wr_id;

          if (wr_id == WR_WRITE_ALLOCATION_BLOCK) {
            allocation_block.just_writing = false;
          } else {
            auto [col, row] = decode_64bit(wr_id);
            read_buffer.get_block(col, row).just_writing = false;
          }
          break;
        }
        default:
          lib_failure("cannot handle opcode");
        }
      }

      post_balance -= num_entries;

    } else if (num_entries < 0) {
      lib_failure("Cannot poll completion queue");
    }
  }

  void poll_cq() {
    post_balance -= Context::poll_send_cq(
      send_wcs.data(), max_send_queue_wr_, local_context.get_send_cq());
  }

  u32 get_random_memory_node() { return dist_(generator_); }

public:
  vec<ibv_wc> send_wcs;
  Context local_context;
  vec<u_ptr<DetachedQP>> qps;  // per memory node

  ReadBuffer<true> read_buffer;
  LocalMemoryRegion buffer_region;
  vec<u_ptr<freelist::FreeListBuffers>> free_list_buffers;

  u64 cas_buffer{};
  LocalMemoryRegion cas_region;

  u_ptr<u32[]> allocation_buffer;
  ReadBuffer<true>::BufferBlock allocation_block;
  LocalMemoryRegion allocation_region;

  u64 local_num_result{0};
  u64 rdma_reads_in_bytes{0};
  u64 processed_queries{0};
  u64 remote_allocations{0};
  u64 remote_deallocations{0};
  u64 block_repeated_reads{0};
  u64 list_repeated_reads{0};

  i32 post_balance{0};
  i32 post_balance_CAS{0};

  timing::Timing::IntervalPtr t_operation;
  timing::Timing::IntervalPtr t_read_list;
  timing::Timing::IntervalPtr t_poll;

  u64 locking_failed{0};
  u64 read_failed{0};
  u64 wait_for_write{0};

private:
  const i32 max_send_queue_wr_;

  std::mt19937 generator_{std::random_device{}()};
  std::uniform_int_distribution<u32> dist_;
};

}  // namespace inv_index::block_based::dynamic

#endif  // INDEX_BLOCK_BASED_DYNAMIC_COMPUTE_THREAD_HH
