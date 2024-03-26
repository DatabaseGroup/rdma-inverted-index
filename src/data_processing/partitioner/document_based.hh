#ifndef DATA_PROCESSING_PARTITIONER_DOCUMENT_BASED_HH
#define DATA_PROCESSING_PARTITIONER_DOCUMENT_BASED_HH

#include <library/types.hh>

#include "data_processing/serializer/deserializer.hh"

namespace partitioner {

// meta data: [ memory node | universe | num lists | term 1 | list len | ... ]
//            -> all terms occur on all Ms
// index data: [ part of list 1 | part of list 2 | ... ]
class DocumentBasedPartitioner {
private:
  using Batch = vec<u32>;

public:
  DocumentBasedPartitioner(vec<Batch>& meta_batches,
                           vec<Batch>& index_batches,
                           u32 num_nodes)
      : meta_batches_(meta_batches),
        index_batches_(index_batches),
        num_nodes_(num_nodes) {}

  void partition(Deserializer& deserializer,
                 u32 num_lists,
                 const func<void(u32)>& print_status) {
    // all terms occur on all Ms
    for (Batch& meta_batch : meta_batches_) {
      meta_batch[2] = num_lists;
    }

    for (u32 j = 0; j < num_lists; ++j) {
      const u32 term = deserializer.read_u32();
      const u32 list_size = deserializer.read_u32();
      print_status(term);

      // add term to all Ms
      for (Batch& meta_batch : meta_batches_) {
        meta_batch.insert(meta_batch.end(), {term, 0});  // current len
      }

      for (u32 i = 0; i < list_size; ++i) {
        const u32 document = deserializer.read_u32();
        const u32 node = document % num_nodes_;

        meta_batches_[node].back()++;  // increase length
        index_batches_[node].push_back(document);
      }
    }
  }

  static str get_name() { return "document"; }

private:
  vec<Batch>& meta_batches_;
  vec<Batch>& index_batches_;
  const u32 num_nodes_;
};

}  // namespace partitioner

#endif  // DATA_PROCESSING_PARTITIONER_DOCUMENT_BASED_HH
