#ifndef DATA_PROCESSING_PARTITIONER_TERM_BASED_HH
#define DATA_PROCESSING_PARTITIONER_TERM_BASED_HH

#include <library/types.hh>

#include "data_processing/serializer/deserializer.hh"

namespace partitioner {
// meta data: [ memory node | universe | num lists | term 1 | list len | ... ]
// index data: [ list 1 | list 2 | ... ]
class TermBasedPartitioner {
private:
  using Batch = vec<u32>;

public:
  TermBasedPartitioner(vec<Batch>& meta_batches,
                       vec<Batch>& index_batches,
                       u32 num_nodes)
    : meta_batches_(meta_batches),
      index_batches_(index_batches),
      num_nodes_(num_nodes) {}

  void partition(Deserializer& deserializer,
                 u32 num_lists,
                 const func<void(u32)>& print_status) {
    vec<u64> costs(num_nodes_, 0);

    for (u32 j = 0; j < num_lists; ++j) {
      const u32 term = deserializer.read_u32();
      const u32 list_size = deserializer.read_u32();
      print_status(term);

      // get node with the lowest cost
      const auto min_val = std::min_element(costs.begin(), costs.end());
      const u32 min_node = std::distance(costs.begin(), min_val);

      costs[min_node] += list_size;  // increase cost

      Batch& meta_batch = meta_batches_[min_node];
      Batch& index_batch = index_batches_[min_node];

      ++meta_batch[2];  // increase number of lists
      meta_batch.insert(meta_batch.end(), {term, list_size});

      for (u32 i = 0; i < list_size; ++i) {
        index_batch.push_back(deserializer.read_u32());
      }
    }
  }

  static str get_name() { return "term"; }

private:
  vec<Batch>& meta_batches_;
  vec<Batch>& index_batches_;
  const u32 num_nodes_;
};

}  // namespace partitioner

#endif  // DATA_PROCESSING_PARTITIONER_TERM_BASED_HH
