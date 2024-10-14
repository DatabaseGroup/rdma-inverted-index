#ifndef DATA_PROCESSING_PARTITIONER_PARTITIONING_STRATEGIES_HH
#define DATA_PROCESSING_PARTITIONER_PARTITIONING_STRATEGIES_HH

#include <iostream>
#include <type_traits>

#include "block_based.hh"
#include "document_based.hh"
#include "term_based.hh"
#include "timing/timing.hh"

namespace partitioner {
using Batch = vec<u32>;

void write_u32_buffer(const vec<u32>& buffer, std::ofstream& output_s) {
  if (!output_s.write(reinterpret_cast<const char*>(buffer.data()),
                      sizeof(u32) * buffer.size())) {
    std::cerr << "Cannot write to file" << std::endl;
    std::exit(EXIT_FAILURE);
  }
}

template <typename Partitioner>
void partition(Deserializer& deserializer,
               u32 num_nodes,
               const str& output_path,
               timing::Timing& timing,
               u32 block_size,
               vec<u32>& accessed,
               const bool updates) {
  vec<Batch> index_batches(num_nodes);
  vec<Batch> meta_batches(num_nodes);

  auto t_partition = timing.create_enroll("partition");
  auto t_write = timing.create_enroll("write_batches");

  const u32 universe_size = deserializer.read_u32();
  const u32 num_lists = deserializer.read_u32();

  // initialize batches and virtually reserve 80 GBs to avoid doubling capacity
  for (u32 memory_node = 0; memory_node < num_nodes; ++memory_node) {
    meta_batches[memory_node] = {memory_node, universe_size, 0};  // num lists
    // index_batches[memory_node].reserve(80 * 1e9 / sizeof(u32) / num_nodes);  // TODO: uncomment for very large datasets
  }

  std::cerr << "universe size: " << universe_size << std::endl;
  std::cerr << "num lists: " << num_lists << std::endl;

  const auto get_current_size = [&]() -> std::pair<size_t, size_t> {
    size_t total_size = 0;
    size_t total_capacity = 0;

    for (u32 memory_node = 0; memory_node < num_nodes; ++memory_node) {
      total_size += meta_batches[memory_node].size() * sizeof(u32);
      total_size += index_batches[memory_node].size() * sizeof(u32);

      total_capacity += meta_batches[memory_node].capacity() * sizeof(u32);
      total_capacity += index_batches[memory_node].capacity() * sizeof(u32);
    }

    return {total_size, total_capacity};
  };

  const auto print_status = [&](u32 term) {
    if (term % 100000 == 0) {
      const auto [current_size, current_capacity] = get_current_size();
      std::cerr << "partitioning list " << term << "/" << num_lists << " ("
                << (term / static_cast<double>(num_lists)) * 100 << "%)"
                << " - current size: " << current_size / 1e9
                << " GB, capacity: " << current_capacity / 1e9 << " GB"
                << std::endl;
    }
  };

  Partitioner p{meta_batches, index_batches, num_nodes};

  // block-based requires more memory, so we split the processing
  //  const auto write_partial_output = [&](u32 part) {
  //    t_write->start();
  //    for (u32 memory_node = 0; memory_node < num_nodes; ++memory_node) {
  //      Batch& index_batch = index_batches[memory_node];
  //
  //      std::cerr << "writing part " << part + 1 << " of index batch for node
  //      "
  //                << memory_node << " to file..." << std::endl;
  //
  //      const str filename =
  //        output_path + ((output_path.back() == '/') ? "" : "/") +
  //        p.get_name() +
  //        "_m" + std::to_string(memory_node + 1) + "_of" +
  //        std::to_string(num_nodes);
  //
  //      auto flags = std::ios::out | std::ios::binary;
  //
  //      if (part > 0) {
  //        flags |= std::ios_base::app;
  //      }
  //
  //      // write index data
  //      std::ofstream index_output(filename + "_index.dat", flags);
  //      write_u32_buffer(index_batch, index_output);
  //      index_output.close();
  //
  //      index_batch.clear();
  //    }
  //
  //    t_write->stop();
  //  };

  // partition
  t_partition->start();
  auto output_flags = std::ios::out | std::ios::binary;

  if constexpr (std::is_same<Partitioner, BlockBasedPartitioner>::value) {
    p.partition(
      deserializer, num_lists, block_size, accessed, updates, print_status);
    //                write_partial_output);
    //    output_flags |= std::ios_base::app;

  } else {
    p.partition(deserializer, num_lists, print_status);
  }
  t_partition->stop();

  // now write batches to files
  t_write->start();
  for (u32 memory_node = 0; memory_node < num_nodes; ++memory_node) {
    Batch& meta_batch = meta_batches[memory_node];
    Batch& index_batch = index_batches[memory_node];

    std::cerr << "writing batches for node " << memory_node << " to files..."
              << std::endl;
    std::cerr << "index size: " << index_batch.size() * sizeof(u32)
              << std::endl;
    std::cerr << "meta size: " << meta_batch.size() * sizeof(u32) << std::endl;

    const str filename = output_path +
                         ((output_path.back() == '/') ? "" : "/") +
                         p.get_name() + "_m" + std::to_string(memory_node + 1) +
                         "_of" + std::to_string(num_nodes);

    // write meta data
    std::ofstream meta_output(filename + "_meta.dat",
                              std::ios::out | std::ios::binary);
    write_u32_buffer(meta_batch, meta_output);
    meta_output.close();

    // write index data
    std::ofstream index_output(filename + "_index.dat", output_flags);
    write_u32_buffer(index_batch, index_output);
    index_output.close();
  }
  t_write->stop();
}
}  // namespace partitioner

#endif  // DATA_PROCESSING_PARTITIONER_PARTITIONING_STRATEGIES_HH
