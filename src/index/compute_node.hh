#ifndef INDEX_COMPUTE_NODE_HH
#define INDEX_COMPUTE_NODE_HH

#include <library/connection_manager.hh>
#include <optional>
#include <type_traits>

#include "block_based_dynamic/query_handler.hh"
#include "index/configuration.hh"
#include "index/core_assignment.hh"
#include "index/query/query.hh"
#include "index/statistics.hh"
#include "timing/timing.hh"

namespace inv_index {

template <class QueryHandler>
class ComputeNode {
  static inline constexpr bool DYNAMIC_BLOCK =
    std::is_same<QueryHandler,
                 block_based::dynamic::DynamicBlockBasedQueryHandler>::value;

public:
  using Configuration = configuration::IndexConfiguration;
  using Queue = concurrent_queue<u32>;  // idx to query
  using CoreAssignment = CoreAssignment<AssignmentPolicy::interleaved>;
  using Statistics = statistics::Statistics<DYNAMIC_BLOCK>;
  using CountItem = typename Statistics::template CountItem<u64>;

public:
  explicit ComputeNode(Configuration& config);

private:
  void init_remote_tokens();
  void exchange_infos_with_compute_nodes(Configuration& config);
  void receive_remote_access_tokens();
  void run_worker_threads(QueryHandler& query_handler, bool pin_threads);
  void join_threads(QueryHandler& query_handler);
  void add_meta_statistics(Configuration& config);
  void gather_statistics(vec<u64>&& raw_stats, vec<CountItem*>&& ref_stats);
  std::optional<timing::Timing::IntervalPtr> gather_timings();
  void terminate();

private:
  Context context_;
  ClientConnectionManager cm_;

  const u32 num_servers_;

  Configuration::Operation operation_{};
  u32 num_compute_threads_{};
  str index_directory_{};
  u32 block_size_{};

  MemoryRegionTokens remote_access_tokens_;
  CoreAssignment core_assignment_;

  query::Queries queries_;
  timing::Timing timing_;
  Statistics statistics_;

  timing::Timing::IntervalPtr t_query_{};
  Queue query_queue_;
};

}  // namespace inv_index

#include "compute_node_impl.hh"

#endif  // INDEX_COMPUTE_NODE_HH