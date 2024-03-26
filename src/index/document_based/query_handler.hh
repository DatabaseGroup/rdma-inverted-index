#ifndef INDEX_DOCUMENT_BASED_QUERY_HANDLER_HH
#define INDEX_DOCUMENT_BASED_QUERY_HANDLER_HH

#include <algorithm>
#include <library/batched_read.hh>
#include <library/latch.hh>
#include <random>

#include "compute_thread.hh"
#include "data_processing/serializer/deserializer.hh"
#include "index/configuration.hh"
#include "index/operations.hh"
#include "index/query/query.hh"
#include "index/shared_context.hh"
#include "remote_pointer.hh"

namespace inv_index::document_based {

class DocumentBasedQueryHandler {
public:
  using Configuration = configuration::IndexConfiguration;
  using ComputeThreads = vec<u_ptr<ComputeThread>>;
  using SharedContext = SharedContext<ComputeThread>;
  using Queue = concurrent_queue<u32>;
  inline static const str name = "document";

private:
  struct ReadBufferInfo {
    using AddressType = u32*;

    vec<AddressType> begin_addresses;
    vec<AddressType> end_addresses;

    void reset() {
      begin_addresses.clear();
      end_addresses.clear();
    }

    void add_addresses(AddressType begin_address, u32 length) {
      begin_addresses.push_back(begin_address);
      end_addresses.push_back(begin_address + length);
    }
  };

public:
  DocumentBasedQueryHandler(u32 num_compute_threads, i32 max_send_queue_wr, u32)
      : num_compute_threads_(num_compute_threads),
        max_send_queue_wr_(max_send_queue_wr),
        mt_(std::random_device{}()) {
    // initialize latches
    start_latch_.init(static_cast<i32>(num_compute_threads));
    end_latch_.init(static_cast<i32>(num_compute_threads));
  }

  size_t allocate_worker_threads(Context& context,
                                 ClientConnectionManager& cm) {
    const u32 num_memory_nodes = cm.server_qps.size();
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
        id, max_send_queue_wr_, num_memory_nodes, buffer_size, local_buffer_));

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
    all_remote_pointers_.resize(num_servers);

    for (u32 memory_node = 0; memory_node < num_servers; ++memory_node) {
      const str binary_file = index_directory + name + "_m" +
                              std::to_string(memory_node + 1) + "_of" +
                              std::to_string(num_servers) + "_meta.dat";
      Deserializer deserializer{binary_file};
      RemotePointers& remote_pointers = all_remote_pointers_[memory_node];

      // do not change the order
      lib_assert(deserializer.read_u32() == memory_node, "wrong meta file");
      universe_size = deserializer.read_u32();
      const u32 num_lists = deserializer.read_u32();

      remote_pointers.resize(universe_size);
      catalog_size += universe_size * sizeof(RemotePtr);

      u64 offset = 0;
      for (u32 i = 0; i < num_lists; ++i) {
        const u32 term = deserializer.read_u32();
        const u32 list_size = deserializer.read_u32();

        RemotePtr& r_ptr = remote_pointers[term];
        lib_assert(r_ptr.is_null(), "remote pointer already assigned");

        r_ptr.length = list_size;
        r_ptr.offset = offset;

        offset += list_size * sizeof(u32);
      }
    }

#ifdef DEV_DEBUG
    for (u32 memory_node = 0; memory_node < num_servers; ++memory_node) {
      u32 idx = 0;

      for (auto& r_ptr : all_remote_pointers_[memory_node]) {
        std::cerr << "node: " << memory_node << ", idx: " << idx++ << " -> "
                  << r_ptr << std::endl;
      }
    }
#endif

    return {universe_size, catalog_size};
  }

  ComputeThreads& get_compute_threads() { return compute_threads_; }

  void READ_row_into_buffer(query::Query& query,
                            u32 buffer_id,
                            u32 memory_node,
                            u_ptr<ComputeThread>& compute_thread,
                            vec<ReadBufferInfo>& buffer_infos,
                            MRT& mrt) {
    QP& qp = compute_thread->ctx->qps[memory_node]->qp;
    u64 buffer_offset = 0;
    BatchedREAD batched_read{query.size()};

    for (u32 k_idx = 0; k_idx < query.size(); ++k_idx) {
      const query::Key key = query.keys[k_idx];
      RemotePtr& r_ptr = all_remote_pointers_[memory_node][key];

      ReadBufferInfo& buffer_info = buffer_infos[buffer_id];
      buffer_info.add_addresses(
        compute_thread->local_buffers[buffer_id] + buffer_offset, r_ptr.length);

      // still, we add the pointers to get the correct result of the operation
      if (r_ptr.length == 0) {
        continue;
      }

      if constexpr (BATCH_READ_REQUESTS) {
        lib_assert(
          buffer_offset + r_ptr.length <= compute_thread->local_buffer_length,
          "READ result exceeds local buffer size");

        batched_read.add_to_batch(
          reinterpret_cast<u64>(compute_thread->local_buffers[buffer_id]) +
            buffer_offset * sizeof(u32),
          mrt->address + r_ptr.offset,
          r_ptr.length * sizeof(u32),
          compute_thread->ctx->get_lkey(),
          mrt->rkey,
          encode_64bit(compute_thread->ctx_tid, buffer_id));

        compute_thread->rdma_reads_in_bytes += r_ptr.length * sizeof(u32);
        compute_thread->post_balance++;

      } else {
        while (compute_thread->post_balance >= max_send_queue_wr_) {
          compute_thread->poll_cq();
        }

        // we must send all signaled because otherwise, the latest could be a
        // remote pointer with zero length (and hence we would skip the READ)
        r_ptr.READ_list(buffer_offset, buffer_id, qp, mrt, compute_thread);
      }

      buffer_offset += r_ptr.length;
      ++compute_thread->posted_to_buffer[buffer_id];
    }

    if constexpr (BATCH_READ_REQUESTS) {
      while (compute_thread->post_balance +
               static_cast<i32>(batched_read.requests) >
             max_send_queue_wr_) {
        compute_thread->poll_cq();
      }

      batched_read.post_batch(qp);
    }
  }

  void process_queries(Queue& query_queue,
                       query::Queries& queries,
                       MemoryRegionTokens& remote_access_tokens,
                       Configuration::Operation& operation,
                       u32 thread_id) {
    auto& compute_thread = compute_threads_[thread_id];
    const u32 num_servers = compute_thread->num_servers;
    auto result_handler = [&](u32) { compute_thread->local_num_result++; };

    start_latch_.arrive_and_wait();
    u32 q;  // idx to query

    // try pop queue
    while (query_queue.try_dequeue(q)) {
      query::Query& query = queries[q];
      if (query.type != QueryType::READ) {
        continue;
      }

      compute_thread->processed_queries++;
      vec<ReadBufferInfo> buffer_infos(NUM_READ_BUFFERS);

      if (q % (queries.size() / 10) == 0) {
        std::cerr << "query " << query << std::endl;
      }

      // access memory nodes in random order
      vec<u32> memory_nodes(num_servers);
      std::iota(memory_nodes.begin(), memory_nodes.end(), 0);
      std::shuffle(memory_nodes.begin(), memory_nodes.end(), mt_);

      auto memory_node = memory_nodes.begin();
      u32 buffer_id = 0;

      // RDMA READ first row (of first memory node) into the first buffer
      compute_thread->t_read_list->start();
      READ_row_into_buffer(query,
                           buffer_id,
                           *memory_node,
                           compute_thread,
                           buffer_infos,
                           remote_access_tokens[*memory_node]);
      compute_thread->t_read_list->stop();

      // wait until completion of first row
      compute_thread->t_poll->start();
      while (compute_thread->posted_to_buffer[buffer_id] > 0) {
        compute_thread->poll_cq();
      }
      compute_thread->t_poll->stop();

      ++memory_node;
      ++buffer_id;

      // RDMA READ remaining rows into alternating buffers
      while (memory_node != memory_nodes.end()) {
        compute_thread->t_read_list->start();
        READ_row_into_buffer(query,
                             buffer_id,
                             *memory_node,
                             compute_thread,
                             buffer_infos,
                             remote_access_tokens[*memory_node]);
        compute_thread->t_read_list->stop();

        // perform_operation on previous buffer (already read entirely)
        compute_thread->t_operation->start();
        const u32 prev_buffer =
          (buffer_id == 0) ? NUM_READ_BUFFERS - 1 : buffer_id - 1;
        ReadBufferInfo& buffer_info = buffer_infos[prev_buffer];

        if (operation == Configuration::Operation::intersection) {
          operations::compute_intersection(result_handler,
                                           buffer_info.begin_addresses,
                                           buffer_info.end_addresses);
        } else {
          operations::compute_union(result_handler,
                                    buffer_info.begin_addresses,
                                    buffer_info.end_addresses);
        }
        compute_thread->t_operation->stop();

        // clear offsets
        buffer_infos[prev_buffer].reset();

        // wait until completion
        compute_thread->t_poll->start();
        while (compute_thread->posted_to_buffer[buffer_id] > 0) {
          compute_thread->poll_cq();
        }
        compute_thread->t_poll->stop();

        ++memory_node;
        buffer_id = (buffer_id + 1) % NUM_READ_BUFFERS;
      }

      // perform final operation on last buffer
      compute_thread->t_operation->start();
      const u32 prev_buffer =
        (buffer_id == 0) ? NUM_READ_BUFFERS - 1 : buffer_id - 1;
      auto& buffer_info = buffer_infos[prev_buffer];

      if (operation == Configuration::Operation::intersection) {
        operations::compute_intersection(result_handler,
                                         buffer_info.begin_addresses,
                                         buffer_info.end_addresses);
      } else {
        operations::compute_union(result_handler,
                                  buffer_info.begin_addresses,
                                  buffer_info.end_addresses);
      }
      compute_thread->t_operation->stop();
    }

    end_latch_.arrive_and_wait();
  }

private:
  const u32 num_compute_threads_;
  const i32 max_send_queue_wr_;

  ComputeThreads compute_threads_;
  vec<RemotePointers> all_remote_pointers_;  // for each memory node

  HugePage<u32> local_buffer_;
  vec<u_ptr<SharedContext>> shared_contexts_;

  Latch start_latch_{};
  Latch end_latch_{};

  std::mt19937 mt_;
};

}  // namespace inv_index::document_based

#endif  // INDEX_DOCUMENT_BASED_QUERY_HANDLER_HH
