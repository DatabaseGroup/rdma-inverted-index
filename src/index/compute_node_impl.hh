#ifndef INDEX_COMPUTE_NODE_IMPL_HH
#define INDEX_COMPUTE_NODE_IMPL_HH

#include "block_based/query_handler.hh"
#include "block_based_dynamic/verify.hh"
#include "index/query/distribute_queries.hh"

namespace inv_index {

template <class QueryHandler>
ComputeNode<QueryHandler>::ComputeNode(Configuration& config)
    : context_(config),
      cm_(context_, config),
      num_servers_(config.num_server_nodes()) {
  init_remote_tokens();
  cm_.connect();

  if (!config.disable_thread_pinning) {
    const u32 core = core_assignment_.get_available_core();
    pin_main_thread(core);
    print_status("pinned main thread to core " + std::to_string(core));
  }

  if (cm_.is_initiator) {
    // communicate the index file to the memory nodes
    u32 i = 0;

    for (QP& qp : cm_.server_qps) {
      str index_file = config.index_dir + QueryHandler::name;
      if constexpr (std::is_same<QueryHandler,
                                 block_based::BlockBasedQueryHandler>::value ||
                    DYNAMIC_BLOCK) {
        index_file += std::to_string(config.block_size);
      }
      index_file += "_m" + std::to_string(++i) + "_of" +
                    std::to_string(num_servers_) + "_index.dat";
      u32 len = index_file.size();
      qp->post_send_u32(config.num_threads, false);
      qp->post_send_u32(len, false);
      qp->post_send_inlined(index_file.data(), len, IBV_WR_SEND);
      context_.poll_send_cq_until_completion();

      if (DYNAMIC_BLOCK) {
        qp->post_send_u32(config.block_size, true);
        context_.poll_send_cq_until_completion();
      }
    }
  }

  vec<u32> free_list_offsets;
  struct IndexSizeInfos {
    size_t initial_index_size{0};
    size_t index_buffer_size{0};
    size_t freelist_offset{0};
  };

  // receive partial index sizes
  for (QP& qp : cm_.server_qps) {
    IndexSizeInfos index_sizes;
    LocalMemoryRegion index_size_region{
      context_,
      std::addressof(index_sizes),
      (DYNAMIC_BLOCK ? 3 : 2) * sizeof(size_t)};
    qp->post_receive(index_size_region);
    context_.receive();

    if (cm_.is_initiator) {
      statistics_.total_index_size.add(index_sizes.initial_index_size);
      statistics_.total_index_buffer_size.add(index_sizes.index_buffer_size);
    }

    if constexpr (DYNAMIC_BLOCK) {
      free_list_offsets.push_back(index_sizes.freelist_offset);
    }
  }

  exchange_infos_with_compute_nodes(config);
  receive_remote_access_tokens();

  QueryHandler query_handler{
    num_compute_threads_, config.max_send_queue_wr, block_size_};
  if constexpr (DYNAMIC_BLOCK) {
    query_handler.assign_free_lists(free_list_offsets, remote_access_tokens_);
  }

  print_status("read meta data and assign remote pointers");
  const auto [universe_size, catalog_size] =
    query_handler.assign_remote_pointers(num_servers_, index_directory_);

  if (cm_.is_initiator) {
    query::QueryStatistics query_stats =
      query::read_queries(config.query_file, queries_);
    lib_assert(query_stats.universe_size <= universe_size,
               "universe of query keys is too large");
    statistics_.num_queries.add(queries_.size());
    statistics_.universe_size.add(universe_size);
    statistics_.catalog_size.add(catalog_size);
    statistics_.num_read_queries.add(query_stats.num_reads);
    statistics_.num_insert_queries.add(query_stats.num_inserts);

    if (cm_.num_total_clients > 1) {
      auto t_distribute_queries = timing_.create_enroll("distribute_queries");
      t_distribute_queries->start();
      query::distribute_queries(
        queries_, context_, cm_.client_qps, cm_.num_total_clients);
      t_distribute_queries->stop();
    }
  } else {
    query::receive_queries(queries_, context_, cm_.initiator_qp);
  }

  // insert queries into working queue
  for (u32 idx = 0; idx < queries_.size(); ++idx) {
    query_queue_.enqueue(idx);
  }

  print_status("allocate worker threads and read buffers");
  const size_t read_buffers_size =
    query_handler.allocate_worker_threads(context_, cm_);
  statistics_.allocated_read_buffers_size.add(read_buffers_size);

  // notify memory nodes that we are ready
  cm_.synchronize();

  // run queries
  run_worker_threads(query_handler, !config.disable_thread_pinning);
  join_threads(query_handler);

#ifdef VERIFY
  if constexpr (DYNAMIC_BLOCK) {
    // wait to make sure that all the other compute nodes are done as well
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    print_status("verification");
    verify(queries_,
           query_handler.get_remote_pointers(),
           query_handler.get_compute_threads().front(),
           remote_access_tokens_);
  }
#endif

  // notify memory nodes that we are done
  terminate();

  if (cm_.is_initiator) {
    add_meta_statistics(config);
    statistics_.output_all(timing_.to_json());
  }
}

template <class QueryHandler>
void ComputeNode<QueryHandler>::init_remote_tokens() {
  remote_access_tokens_.resize(num_servers_);

  for (auto& mrt : remote_access_tokens_) {
    // ownership has the vector
    mrt = std::make_unique<MemoryRegionToken>();
  }
}

template <class QueryHandler>
void ComputeNode<QueryHandler>::exchange_infos_with_compute_nodes(
  Configuration& config) {
  print_status("exchange information with compute nodes");

  struct CInfo {
    u32 compute_threads;
    u32 operation;
    u32 directory_size;
    u32 block_size;
  };

  if (cm_.is_initiator) {
    num_compute_threads_ = config.num_threads;
    operation_ = config.get_operation();
    index_directory_ = config.index_dir;
    block_size_ = config.block_size;

    CInfo info{config.num_threads,
               operation_,
               static_cast<u32>(index_directory_.size()),
               block_size_};

    for (QP& qp : cm_.client_qps) {
      qp->post_send_inlined(std::addressof(info), sizeof(info), IBV_WR_SEND);
      qp->post_send_inlined(
        index_directory_.data(), index_directory_.size(), IBV_WR_SEND);
      context_.poll_send_cq_until_completion(2);
    }
  } else {
    CInfo info{};
    LocalMemoryRegion info_region{context_, std::addressof(info), sizeof(info)};
    cm_.initiator_qp->post_receive(info_region);
    context_.receive();

    num_compute_threads_ = info.compute_threads;
    operation_ = static_cast<Configuration::Operation>(info.operation);
    block_size_ = info.block_size;

    u32 index_dir_size = info.directory_size;
    index_directory_.resize(index_dir_size);
    LocalMemoryRegion string_region{
      context_, index_directory_.data(), index_dir_size};
    cm_.initiator_qp->post_receive(string_region);
    context_.receive();

    std::cerr << "num compute threads: " << num_compute_threads_ << std::endl;
    std::cerr << "operation: "
              << (operation_ == Configuration::Operation::intersection
                    ? "intersection"
                    : "union")
              << std::endl;
    std::cerr << "index directory: " << index_directory_ << std::endl;
  }
}

template <class QueryHandler>
void ComputeNode<QueryHandler>::receive_remote_access_tokens() {
  print_status("receive access tokens of remote memory regions");
  for (u32 memory_node = 0; memory_node < num_servers_; ++memory_node) {
    QP& qp = cm_.server_qps[memory_node];
    MRT& mrt = remote_access_tokens_[memory_node];

    LocalMemoryRegion token_region{
      context_, mrt.get(), sizeof(MemoryRegionToken)};
    qp->post_receive(token_region);
    context_.receive();
  }
}

template <class QueryHandler>
void ComputeNode<QueryHandler>::run_worker_threads(QueryHandler& query_handler,
                                                   bool pin_threads) {
  print_status("run worker threads");
  t_query_ = timing_.create_enroll("query_c0");

  for (auto& t : query_handler.get_compute_threads()) {
    u32 thread_id = t->get_id();

    if (thread_id != 0) {
      t->start(&QueryHandler::process_queries,
               &query_handler,
               std::ref(query_queue_),
               std::ref(queries_),
               std::ref(remote_access_tokens_),
               std::ref(operation_));

      if (pin_threads) {
        const u32 core = core_assignment_.get_available_core();
        t->set_affinity(core);
        print_status("pinned thread " + std::to_string(thread_id) +
                     " to core " + std::to_string(core));
      }
    }
  }

  // main thread now is also a worker thread and will release the latch
  t_query_->start();
  query_handler.process_queries(
    query_queue_, queries_, remote_access_tokens_, operation_, 0);
  t_query_->stop();
}

template <class QueryHandler>
void ComputeNode<QueryHandler>::join_threads(QueryHandler& query_handler) {
  print_status("join compute threads");
  u64 num_result = 0;
  u64 rdma_reads_in_bytes = 0;

  u64 sum_remote_allocations = 0;
  u64 sum_remote_deallocations = 0;
  u64 sum_block_repeated_reads = 0;
  u64 sum_list_repeated_reads = 0;

  u64 sum_locking_failed = 0;
  u64 sum_read_failed = 0;
  u64 sum_wait_for_write = 0;

  for (auto& t : query_handler.get_compute_threads()) {
    // no need for joining the main thread
    if (t->get_id() != 0) {
      t->join();
    }

    lib_assert(t->post_balance == 0, "incomplete READs");

    rdma_reads_in_bytes += t->rdma_reads_in_bytes;
    num_result += t->local_num_result;
    std::cerr << "t" << t->get_id()
              << " processed queries: " << t->processed_queries;
    if constexpr (DYNAMIC_BLOCK) {
      sum_remote_allocations += t->remote_allocations;
      sum_remote_deallocations += t->remote_deallocations;
      sum_block_repeated_reads += t->block_repeated_reads;
      sum_list_repeated_reads += t->list_repeated_reads;
      sum_read_failed += t->read_failed;
      sum_locking_failed += t->locking_failed;
      sum_wait_for_write += t->wait_for_write;
      std::cerr << ", remote allocations: " << t->remote_allocations
                << ", remote deallocations: " << t->remote_deallocations
                << ", block repeated READs: " << t->block_repeated_reads
                << ", list repeated READs: " << t->list_repeated_reads;
    }
    std::cerr << ", READ lists: " << t->t_read_list->get_ms()
              << ", polling: " << t->t_poll->get_ms()
              << ", operation: " << t->t_operation->get_ms() << std::endl;
  }

  // collect statistics
  gather_statistics(
    {num_result, rdma_reads_in_bytes},
    {&statistics_.num_result, &statistics_.rdma_reads_in_bytes});

  if constexpr (DYNAMIC_BLOCK) {
    gather_statistics({sum_remote_allocations,
                       sum_remote_deallocations,
                       sum_block_repeated_reads,
                       sum_list_repeated_reads,
                       sum_read_failed,
                       sum_wait_for_write,
                       sum_locking_failed},
                      {&statistics_.remote_allocations,
                       &statistics_.remote_deallocations,
                       &statistics_.block_repeated_reads,
                       &statistics_.list_repeated_reads,
                       &statistics_.read_failed,
                       &statistics_.wait_for_write,
                       &statistics_.locking_failed});
  }

  // collect timings
  auto t_query_max = gather_timings();

  if (cm_.is_initiator) {
    f64 query_time = t_query_max.value()->get_ms() / 1000.0;  // in sec

    statistics_.add_static_stat(
      "queries_per_sec",
      static_cast<u64>(statistics_.num_queries.count / query_time));
    statistics_.add_static_stat(
      "mb_per_sec",
      static_cast<f64>(statistics_.rdma_reads_in_bytes.count) / 1000000.0 /
        query_time);
  }
}

template <class QueryHandler>
void ComputeNode<QueryHandler>::add_meta_statistics(Configuration& config) {
  const auto name_from_path = [](const str& path) -> str {
    return path.substr(path.find_last_of('/') + 1);
  };

  statistics_.add_meta_stats(
    std::make_pair("compute_nodes", cm_.num_total_clients),
    std::make_pair("memory_nodes", num_servers_),
    std::make_pair("compute_threads",
                   cm_.num_total_clients * num_compute_threads_),
    std::make_pair("algorithm", QueryHandler::name + "-based"),
    std::make_pair("operation", config.operation),
    std::make_pair("threads_pinned",
                   config.disable_thread_pinning ? "false" : "true"),
    std::make_pair(
      "hyperthreading",
      core_assignment_.hyperthreading_enabled() ? "true" : "false"),
    std::make_pair(
      "index_directory",
      name_from_path(index_directory_.substr(0, index_directory_.size() - 1))),
    std::make_pair("query_file", name_from_path(config.query_file)));
  if constexpr (std::is_same<QueryHandler,
                             block_based::BlockBasedQueryHandler>::value) {
    statistics_.template add_meta_stat("block_size", config.block_size);
  }
}

template <class QueryHandler>
void ComputeNode<QueryHandler>::gather_statistics(vec<u64>&& raw_stats,
                                                  vec<CountItem*>&& ref_stats) {
  print_status("gather query statistics");

  if (cm_.is_initiator) {
    vec<u64> received_stats(raw_stats.size());

    for (QP& qp : cm_.client_qps) {
      LocalMemoryRegion region(
        context_, received_stats.data(), raw_stats.size() * sizeof(u64));
      qp->post_receive(region);
      context_.receive();

      for (size_t i = 0; i < raw_stats.size(); ++i) {
        raw_stats[i] += received_stats[i];
      }
    }

    for (size_t i = 0; i < raw_stats.size(); ++i) {
      ref_stats[i]->add(raw_stats[i]);
    }

  } else {
    cm_.initiator_qp->post_send_inlined(
      raw_stats.data(), raw_stats.size() * sizeof(u64), IBV_WR_SEND);
    context_.poll_send_cq_until_completion();
  }
}

template <class QueryHandler>
std::optional<timing::Timing::IntervalPtr>
ComputeNode<QueryHandler>::gather_timings() {
  print_status("gather timings");
  timespec query_time = t_query_->time_;

  if (cm_.is_initiator) {
    timespec max_query_time = query_time;

    for (u32 client_id = 1; client_id < cm_.num_total_clients; ++client_id) {
      QP& qp = cm_.client_qps[client_id - 1];
      LocalMemoryRegion region(context_, &query_time, sizeof(timespec));
      qp->post_receive(region);
      context_.receive();

      auto interval =
        timing_.create_enroll("query_c" + std::to_string(client_id));
      interval->time_ = query_time;

      if (interval->get_ms() > timing::Timing::get_ms(max_query_time)) {
        max_query_time = query_time;
      }
    }

    auto t_query_max = timing_.create_enroll("query_total");
    t_query_max->time_ = max_query_time;

    return t_query_max;

  } else {
    cm_.initiator_qp->post_send_inlined(
      &query_time, sizeof(timespec), IBV_WR_SEND);
    context_.poll_send_cq_until_completion();

    return {};
  }
}

template <class QueryHandler>
void ComputeNode<QueryHandler>::terminate() {
  bool done = true;  // dummy value

  // notify memory nodes
  for (QP& qp : cm_.server_qps) {
    qp->post_send_inlined(&done, sizeof(bool), IBV_WR_SEND);
  }

  context_.poll_send_cq_until_completion(num_servers_);
}

}  // namespace inv_index

#endif  // INDEX_COMPUTE_NODE_IMPL_HH
