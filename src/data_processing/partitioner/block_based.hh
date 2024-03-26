#ifndef DATA_PROCESSING_PARTITIONER_BLOCK_BASED_HH
#define DATA_PROCESSING_PARTITIONER_BLOCK_BASED_HH

#include <library/types.hh>

#include "data_processing/serializer/deserializer.hh"
#include "index/block_based_dynamic/remote_pointer.hh"
#include "index/crc.hh"

namespace partitioner {

// meta data:  [ memory node | universe | num init blocks | block size |
//               term1 | offset | ... ]
// index data: [ block[entries... | r_ptr] | ... ]
//               every cache line is versioned
// footer:
//  * read-only footer:
//     remote ptr (64): [ memory node | offset ]
//                      (32bit offset is sufficient since we address blocks)
//  * updates footer:
//     remote ptr (64): [ p_tag (16) | m_id (10) | offset(38) ]
//     flags (64):      [ cl version (32) | null (14) | b_tag (16) | lock (1) ]
//     (the last 64bit word is accessed with CAS, must be one word and
//      interpreted as 64bit word!!!, with the version we can detect changes)
class BlockBasedPartitioner {
private:
  using Batch = vec<u32>;
  constexpr static u32 TOMBSTONE = static_cast<u32>(-1);
  constexpr static u32 CACHE_LINE_SIZE = 64;
  constexpr static u32 INIT_CACHE_LINE_VERSION = 0;
  constexpr static u32 INIT_BLOCK_TAG = 0;

public:
  BlockBasedPartitioner(vec<Batch>& meta_batches,
                        vec<Batch>& index_batches,
                        u32 num_nodes)
      : meta_batches_(meta_batches),
        index_batches_(index_batches),
        num_nodes_(num_nodes) {}

private:
  static void add_footer(Batch& batch,
                         u32 next_node,
                         u32 next_offset,
                         bool updates) {
    // remote ptr (64): [ p_tag (16) | m_id (10) | offset(38) ]
    //     flags (64):  [ cl version (32) | null (14) | b_tag (16) | lock (1) ]
    if (updates) {
      const u64 r_ptr =
        inv_index::block_based::dynamic::RemotePtr::encode_remote_ptr(
          INIT_BLOCK_TAG, next_node, next_offset);

      // split r_ptr in two single words
      batch.push_back(r_ptr >> 32);
      batch.push_back((r_ptr << 32) >> 32);

      // [ cache line version (32) | null (14) | b_tag (16) |lock-bit (1) ]
      // b_tag starts with 0, free and lock are not set
      batch.push_back(INIT_CACHE_LINE_VERSION);
      batch.push_back(0);

    } else {
      // remote ptr (64): [ memory node | offset ]
      batch.push_back(next_node);
      batch.push_back(next_offset);
    }
  }

public:
  void partition(Deserializer& deserializer,
                 u32 num_lists,
                 u32 block_size,
                 vec<u32>& accessed,
                 bool updates,
                 const func<void(u32)>& print_status) {
    const u32 remote_ptr_entries = updates ? 4 : 2;
    //                 const func<void(u32)>& write_output) {
    const u32 block_entries = block_size / sizeof(u32);
    const bool accessed_only = !accessed.empty();
    name_ =
      (updates ? "dynamic_" : "") + str("block") + std::to_string(block_size);

    for (Batch& meta_batch : meta_batches_) {
      meta_batch.push_back(block_size);
    }

    u32 node = 0, next_list;
    vec<u32> offset_per_memory_node(num_nodes_, 0);
    u64 num_blocks = 0;

    const auto pop_heap = [&]() {
      std::pop_heap(accessed.begin(), accessed.end(), std::greater<>{});
      u32 top = accessed.back();
      accessed.pop_back();

      return top;
    };

    if (accessed_only) {
      std::make_heap(accessed.begin(), accessed.end(), std::greater<>{});
      next_list = pop_heap();
    }

    for (u32 j = 0; j < num_lists && deserializer.bytes_left(); ++j) {
      node = j > 0 ? (node + 1) % num_nodes_ : 0;
      //      if (j % (num_lists / 10) == 0 && j != 0) {
      //        write_output(part++);
      //      }
      const u32 term = deserializer.read_u32();
      const u32 list_size = deserializer.read_u32();
      print_status(term);

      Batch& meta_batch = meta_batches_[node];
      u32& offset = offset_per_memory_node[node];

      lib_assert(offset < static_cast<u32>(-1), "offset overflow");

      // skip this list
      if (accessed_only && term != next_list) {
        deserializer.jump(list_size * sizeof(u32));
        continue;
      }

      if (accessed_only) {
        if (accessed.empty()) {
          break;
        }

        next_list = pop_heap();
      }

      ++meta_batch[2];  // increase number of init blocks
      meta_batch.insert(meta_batch.end(), {term, offset++});

      u32 remaining_block_entries = block_entries;
      ++num_blocks;

      const auto cache_line_versioning = [&]() {
        if (updates && (block_size - remaining_block_entries * sizeof(u32)) %
                           CACHE_LINE_SIZE ==
                         0) {
          --remaining_block_entries;
          index_batches_[node].push_back(INIT_CACHE_LINE_VERSION);
        }
      };

      // build blocks
      for (u32 i = 0; i < list_size; ++i) {
        const u32 document = deserializer.read_u32();

        cache_line_versioning();

        // create new block
        if (remaining_block_entries == remote_ptr_entries) {
          const u32 next_node = (node + 1) % num_nodes_;
          u32& next_offset = offset_per_memory_node[next_node];

          add_footer(index_batches_[node], next_node, next_offset, updates);
          ++next_offset;

          node = next_node;
          remaining_block_entries = block_entries;
          cache_line_versioning();
          ++num_blocks;
        }

        index_batches_[node].push_back(document);
        --remaining_block_entries;
      }

      // fill up block with tombstones
      while (remaining_block_entries > remote_ptr_entries) {
        cache_line_versioning();

        index_batches_[node].push_back(TOMBSTONE);
        --remaining_block_entries;
      }

      // set null pointer
      if (remaining_block_entries > 0) {
        add_footer(index_batches_[node], 0, 0, updates);
        remaining_block_entries -= remote_ptr_entries;
      }
    }

    std::cerr << "num blocks: " << num_blocks << std::endl;
  }

  str get_name() const { return name_; }

private:
  vec<Batch>& meta_batches_;
  vec<Batch>& index_batches_;
  const u32 num_nodes_;
  str name_;
};

}  // namespace partitioner

#endif  // DATA_PROCESSING_PARTITIONER_BLOCK_BASED_HH
