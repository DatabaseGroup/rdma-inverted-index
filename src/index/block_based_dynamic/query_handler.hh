#ifndef INDEX_BLOCK_BASED_DYNAMIC_QUERY_HANDLER_HH
#define INDEX_BLOCK_BASED_DYNAMIC_QUERY_HANDLER_HH

#include <algorithm>
#include <library/batched_read.hh>
#include <library/latch.hh>

#include "compute_thread.hh"
#include "data_processing/serializer/deserializer.hh"
#include "free_list.hh"
#include "index/block_based/block_operations.hh"
#include "index/configuration.hh"
#include "index/query/query.hh"
#include "remote_pointer.hh"

namespace inv_index::block_based::dynamic {

class DynamicBlockBasedQueryHandler {
public:
  using Configuration = configuration::IndexConfiguration;
  using ComputeThreads = vec<u_ptr<ComputeThread>>;
  using Queue = concurrent_queue<u32>;
  inline static const str name = "dynamic_block";

public:
  DynamicBlockBasedQueryHandler(u32 num_compute_threads,
                                i32 max_send_queue_wr,
                                u32 block_size)
      : num_compute_threads_(num_compute_threads),
        max_send_queue_wr_(max_send_queue_wr),
        block_size_(block_size) {
    // initialize latches
    start_latch_.init(static_cast<i32>(num_compute_threads));
    end_latch_.init(static_cast<i32>(num_compute_threads));

    // initialize static member
    RemotePtr::block_size = block_size;
  }

  void assign_free_lists(const vec<u32>& free_list_offsets,
                         MemoryRegionTokens& remote_access_tokens) {
    const u32 num_servers = remote_access_tokens.size();
    free_lists_.reserve(num_servers);

    for (u32 memory_node = 0; memory_node < num_servers; ++memory_node) {
      free_lists_.emplace_back(std::make_unique<freelist::FreeList>(
        block_size_,
        memory_node,
        free_list_offsets[memory_node],
        remote_access_tokens[memory_node]));
    }
  }

  size_t allocate_worker_threads(Context& context,
                                 ClientConnectionManager& cm) {
    size_t read_buffers_size = 0;
    // allocate a contiguous buffer for local memory
    const size_t total_buffer_size = num_compute_threads_ * READ_BUFFER_LENGTH *
                                     READ_BUFFER_DEPTH * block_size_;
    local_buffer_.allocate(total_buffer_size);
    local_buffer_.touch_memory();

    // TODO: shared contexts are not yet implemented for dynamic block based

    // pre-allocate worker threads
    for (u32 id = 0; id < num_compute_threads_; ++id) {
      compute_threads_.push_back(std::make_unique<ComputeThread>(
        id, max_send_queue_wr_, context, block_size_, local_buffer_));

      read_buffers_size +=
        READ_BUFFER_LENGTH * READ_BUFFER_DEPTH *
        (block_size_ + sizeof(ReadBuffer<true>::BufferBlock));
    }

    // connect queue pairs
    for (auto& compute_thread : compute_threads_) {
      compute_thread->connect_qps(context, cm);
    }

    return read_buffers_size;
  }

  std::pair<u32, u64> assign_remote_pointers(u32 num_servers,
                                             const str& index_directory) {
    u32 universe_size{};
    u64 catalog_size = 0;

    for (u32 memory_node = 0; memory_node < num_servers; ++memory_node) {
      const str binary_file = index_directory + name +
                              std::to_string(block_size_) + "_m" +
                              std::to_string(memory_node + 1) + "_of" +
                              std::to_string(num_servers) + "_meta.dat";
      Deserializer deserializer{binary_file};

      // do not change the order
      lib_assert(deserializer.read_u32() == memory_node, "wrong meta file");
      universe_size = deserializer.read_u32();
      const u32 num_init_blocks = deserializer.read_u32();

      lib_assert(deserializer.read_u32() == block_size_, "wrong meta file");

      // do this only once
      if (memory_node == 0) {
        remote_pointers_.resize(universe_size + 1);
        catalog_size += universe_size * sizeof(RemotePtr);
      }

      for (u32 i = 0; i < num_init_blocks; ++i) {
        const u32 term = deserializer.read_u32();
        const u32 offset = deserializer.read_u32();

        lib_assert(term < universe_size + 1, "invalid universe size");
        RemotePtr& r_ptr = remote_pointers_[term];

        // very first block is semantically null
        // (but this can only occur in the catalog)
        if (!(memory_node == 0 && i == 0)) {
          lib_assert(r_ptr.is_null(), "remote pointer already assigned");
        }

        r_ptr.memory_node = memory_node;
        r_ptr.offset = offset;
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

  void process_queries(Queue& query_queue,
                       query::Queries& queries,
                       MemoryRegionTokens& remote_access_tokens,
                       Configuration::Operation& operation,
                       u32 thread_id) {
    auto& compute_thread = compute_threads_[thread_id];
    auto result_handler = [&](u32) { compute_thread->local_num_result++; };
    auto poll = [&]() { compute_thread->poll_cq_and_handle(); };

    const auto allocate_block = [&]() -> RemotePtr {
      const u32 allocation_node = compute_thread->get_random_memory_node();
      const u64 next = free_lists_[allocation_node]->allocate(compute_thread);
      RemotePtr r_ptr{allocation_node, static_cast<u32>(next / block_size_)};

      return r_ptr;
    };

    start_latch_.arrive_and_wait();
    u32 q;  // idx to query

    // try pop queue
    while (query_queue.try_dequeue(q)) {
      compute_thread->processed_queries++;
      query::Query& query = queries[q];

      if (q % std::max<u32>(queries.size() / 10, 1) == 0) {
        std::cerr << "query " << query << std::endl;
      }

      if (query.type == QueryType::INSERT) {
        for (u32 k_idx = 0; k_idx < query.size(); ++k_idx) {
          bool success;
          RemotePtr& r_ptr = remote_pointers_[query.keys[k_idx]];

          do {
            success = r_ptr.find_block_and_insert(query.update_id,
                                                  k_idx % READ_BUFFER_LENGTH,
                                                  remote_access_tokens,
                                                  compute_thread,
                                                  allocate_block);
          } while (!success);
        }

      } else if (query.type == QueryType::READ) {
        lib_assert(query.size() <= READ_BUFFER_LENGTH,
                   "query exceeds read buffer size");

        compute_thread->t_read_list->start();
        for (u32 k_idx = 0; k_idx < query.size(); ++k_idx) {
          RemotePtr& r_ptr = remote_pointers_[query.keys[k_idx]];
          MRT& mrt = remote_access_tokens[r_ptr.memory_node];

          // prevent WR overflow
          while (compute_thread->post_balance == max_send_queue_wr_) {
            compute_thread->poll_cq_and_handle();
          }

          r_ptr.READ_block(k_idx, 0, mrt, compute_thread);
        }
        compute_thread->t_read_list->stop();

        if (operation == Configuration::Operation::intersection) {
          operations::block_intersection<true>(
            result_handler,
            poll,
            [&](u32 col, u32 next_row, u32 memory_node, u32 offset) {
              RemotePtr p{memory_node, offset};
              MRT& m = remote_access_tokens[memory_node];

              // prevent WR overflow
              while (compute_thread->post_balance == max_send_queue_wr_) {
                compute_thread->poll_cq_and_handle();
              }

              p.READ_block(col, next_row, m, compute_thread);
            },
            compute_thread->read_buffer,
            query.size());

        } else {
          lib_failure("not yet implemented");
        }
      }

      // flush completion queue
      // we could have terminated earlier but still have READs/WRITEs posted
      while (compute_thread->post_balance > 0) {
        compute_thread->poll_cq_and_handle();
      }
    }

    end_latch_.arrive_and_wait();
  }

  ComputeThreads& get_compute_threads() { return compute_threads_; }
  RemotePointers& get_remote_pointers() { return remote_pointers_; }

private:
  const u32 num_compute_threads_;
  const i32 max_send_queue_wr_;
  const u32 block_size_;

  ComputeThreads compute_threads_;
  RemotePointers remote_pointers_;
  HugePage<u32> local_buffer_;

  Latch start_latch_{};
  Latch end_latch_{};

  vec<u_ptr<freelist::FreeList>> free_lists_;  // per memory node
};

}  // namespace inv_index::block_based::dynamic

#endif  // INDEX_BLOCK_BASED_DYNAMIC_QUERY_HANDLER_HH