#ifndef INDEX_TERM_BASED_QUERY_HANDLER_HH
#define INDEX_TERM_BASED_QUERY_HANDLER_HH

#include <library/latch.hh>

#include "compute_thread.hh"
#include "data_processing/serializer/deserializer.hh"
#include "index/configuration.hh"
#include "index/constants.hh"
#include "index/operations.hh"
#include "index/query/query.hh"
#include "index/shared_context.hh"
#include "remote_pointer.hh"

namespace inv_index::term_based {

class TermBasedQueryHandler {
public:
  using Configuration = configuration::IndexConfiguration;
  using ComputeThreads = vec<u_ptr<ComputeThread>>;
  using SharedContext = SharedContext<ComputeThread>;
  using Queue = concurrent_queue<u32>;
  inline static const str name = "term";

public:
  TermBasedQueryHandler(u32 num_compute_threads, i32 max_send_queue_wr, u32)
      : num_compute_threads_(num_compute_threads),
        max_send_queue_wr_(max_send_queue_wr) {
    // initialize latches
    start_latch_.init(static_cast<i32>(num_compute_threads));
    end_latch_.init(static_cast<i32>(num_compute_threads));
  }

  size_t allocate_worker_threads(Context& context,
                                 ClientConnectionManager& cm) {
    // TODO: leads to some weird effects (e.g., if the number of threads is a
    //       multiple of 5 although the buffer is aligned
    u32 denom = num_compute_threads_;
    while (denom > 1 && denom % 8 != 0) {
      ++denom;
    }

    const size_t buffer_size =
      COMPUTE_NODE_MAX_MEMORY / denom -
      16 * sizeof(u32);  // save some space for alignment

    size_t read_buffers_size = 0;

    // allocate a contiguous buffer for local memory
    local_buffer_.allocate(COMPUTE_NODE_MAX_MEMORY);
    local_buffer_.touch_memory();

    // create shared contexts (and QPs)
    for (u32 i = 0; i < std::min<u32>(num_compute_threads_, MAX_QPS); ++i) {
      shared_contexts_.emplace_back(
        std::make_unique<SharedContext>(context, cm, local_buffer_));
    }

    // pre-allocate worker threads
    for (u32 id = 0; id < num_compute_threads_; ++id) {
      compute_threads_.push_back(std::make_unique<ComputeThread>(
        id, max_send_queue_wr_, buffer_size, local_buffer_));

      read_buffers_size += buffer_size;
    }

    // assign the contexts (now the thread pointers can no longer change)
    for (u32 id = 0; id < num_compute_threads_; ++id) {
      auto& ctx = shared_contexts_[id % MAX_QPS];
      ctx->register_thread(compute_threads_[id].get());
    }

    return read_buffers_size;
  }

  std::pair<u32, u64> assign_remote_pointers(u32 num_servers,
                                             const str& index_directory) {
    u32 universe_size{};
    u64 catalog_size = 0;

    for (u32 memory_node = 0; memory_node < num_servers; ++memory_node) {
      const str binary_file = index_directory + name + "_m" +
                              std::to_string(memory_node + 1) + "_of" +
                              std::to_string(num_servers) + "_meta.dat";
      Deserializer deserializer{binary_file};

      // do not change the order
      lib_assert(deserializer.read_u32() == memory_node, "wrong meta file");
      universe_size = deserializer.read_u32();
      const u32 num_lists = deserializer.read_u32();

      // do this only once
      if (memory_node == 0) {
        remote_pointers_.resize(universe_size);
        catalog_size += universe_size * sizeof(RemotePtr);
      }

      u64 offset = 0;
      for (u32 i = 0; i < num_lists; ++i) {
        const u32 term = deserializer.read_u32();
        const u32 list_size = deserializer.read_u32();

        RemotePtr& r_ptr = remote_pointers_[term];
        lib_assert(r_ptr.is_null(), "remote pointer already assigned");

        r_ptr.memory_node = memory_node;
        r_ptr.length = list_size;
        r_ptr.offset = offset;

        offset += list_size * sizeof(u32);
      }
    }

#ifdef DEV_DEBUG
    u32 idx = 0;
    for (auto& r_ptr : remote_pointers_) {
      std::cerr << idx++ << " -> " << r_ptr << std::endl;
    }
#endif

    return {universe_size, catalog_size};
  }

  ComputeThreads& get_compute_threads() { return compute_threads_; }

  void process_queries(Queue& query_queue,
                       query::Queries& queries,
                       MemoryRegionTokens& remote_access_tokens,
                       Configuration::Operation& operation,
                       u32 thread_id) {
    auto& compute_thread = compute_threads_[thread_id];
    start_latch_.arrive_and_wait();
    u32 q;  // idx to query

    // try pop queue
    while (query_queue.try_dequeue(q)) {
      query::Query& query = queries[q];
      if (query.type != QueryType::READ) {
        continue;
      }

      compute_thread->processed_queries++;

      using AddressType = u32*;
      vec<AddressType> begin_addresses;
      vec<AddressType> end_addresses;
      vec<u64> buffer_offsets = {0};

      if (q % (queries.size() / 10) == 0) {
        std::cerr << "query " << query << std::endl;
      }

      // determine buffer_offsets bounds
      for (u32 k_idx = 0; k_idx < query.size(); ++k_idx) {
        RemotePtr& r_ptr = remote_pointers_[query.keys[k_idx]];

        begin_addresses.push_back(compute_thread->buffer_ptr +
                                  buffer_offsets.back());
        end_addresses.push_back(begin_addresses.back() + r_ptr.length);
        buffer_offsets.push_back(buffer_offsets.back() + r_ptr.length);
      }

      // RDMA READ lists block-wise
      compute_thread->t_read_list->start();
      for (u32 k_idx = 0; k_idx < query.size(); ++k_idx) {
        const query::Key key = query.keys[k_idx];

        RemotePtr& r_ptr = remote_pointers_[key];
        MRT& mrt = remote_access_tokens[r_ptr.memory_node];

        // prevent WR overflow
        while (compute_thread->post_balance == max_send_queue_wr_) {
          compute_thread->poll_cq();
        }

        r_ptr.READ_list(buffer_offsets[k_idx], mrt, compute_thread);
      }
      compute_thread->t_read_list->stop();

      // function: post READ when a result is found
      auto result_handler = [&](u32) { compute_thread->local_num_result++; };

      // wait until lists have been READ
      compute_thread->t_poll->start();
      while (compute_thread->post_balance > 0) {
        compute_thread->poll_cq();
      }
      compute_thread->t_poll->stop();

      // compute operation
      compute_thread->t_operation->start();
      if (operation == Configuration::Operation::intersection) {
        operations::compute_intersection(
          result_handler, begin_addresses, end_addresses);
      } else {
        operations::compute_union(
          result_handler, begin_addresses, end_addresses);
      }
      compute_thread->t_operation->stop();
    }

    end_latch_.arrive_and_wait();
  }

private:
  const u32 num_compute_threads_;
  const i32 max_send_queue_wr_;

  ComputeThreads compute_threads_;
  RemotePointers remote_pointers_;

  HugePage<u32> local_buffer_;
  vec<u_ptr<SharedContext>> shared_contexts_;

  Latch start_latch_{};
  Latch end_latch_{};
};

}  // namespace inv_index::term_based

#endif  // INDEX_TERM_BASED_QUERY_HANDLER_HH
