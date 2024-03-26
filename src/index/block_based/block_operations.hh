#ifndef INDEX_BLOCK_BASED_BLOCK_OPERATIONS_HH
#define INDEX_BLOCK_BASED_BLOCK_OPERATIONS_HH

#include <library/types.hh>

#include "read_buffer.hh"

namespace operations {
using namespace inv_index::block_based;

template <bool cache_line_versions = false>
void block_intersection(const func<void(u32)>& result_handler,
                        const func<void()>& poll,
                        const func<void(u32, u32, u32, u32)>& post_READ,
                        ReadBuffer<cache_line_versions>& read_buffer,
                        u32 query_length) {
  using BufferBlock = typename ReadBuffer<cache_line_versions>::BufferBlock;

  const u32 tombstone = static_cast<u32>(-1);
  const u32 footer_size = cache_line_versions ? 4 : 2;
  const u32 block_entries = read_buffer.block_size / sizeof(u32) - footer_size;
  const u32 init_pos = cache_line_versions ? 1 : 0;

  if (query_length == 0) {
    return;
  }

  // special case where query length is 1
  if (query_length == 1) {
    u32 row = 0, pos = 0;

    while (true) {
      BufferBlock* current_block = &read_buffer.get_block(0, row);

      while (!current_block->is_ready()) {
        poll();
      }

      bool points_to_null = current_block->points_to_null();

      // a new block
      if (!points_to_null) {
        auto [memory_node, offset] = current_block->get_remote_ptr();
        u32 next_row = (row + 1) % inv_index::block_based::READ_BUFFER_DEPTH;
        post_READ(0, next_row, memory_node, offset);
      }

      while (pos < block_entries && current_block->buffer[pos] != tombstone) {
        // skip cache line versions
        if constexpr (cache_line_versions) {
          if ((pos * sizeof(u32)) % CACHE_LINE_SIZE == 0) {
            ++pos;
            continue;
          }
        }

        result_handler(current_block->buffer[pos]);
        ++pos;
      }

      if (pos == block_entries || current_block->buffer[pos] == tombstone) {
        if (points_to_null) {
          break;
        }
        row = (row + 1) % inv_index::block_based::READ_BUFFER_DEPTH;
        pos = 0;
      }
    }

    return;
  }

  u32 current_value, count, col = 0;
  vec<u32> row(query_length, 0);

  // first entry is a cache line version
  vec<u32> current_positions(query_length, init_pos);
  BufferBlock* current_block = &read_buffer.get_block(col, row[col]);

  while (!current_block->is_ready()) {
    poll();
  }

  // a new block
  if (!current_block->points_to_null()) {
    auto [memory_node, offset] = current_block->get_remote_ptr();
    u32 next_row = (row[col] + 1) % inv_index::block_based::READ_BUFFER_DEPTH;
    post_READ(col, next_row, memory_node, offset);
  }

  current_value = current_block->buffer[current_positions[col]++];
  count = 1;
  col = (col + 1) % query_length;

  while (true) {
    current_block = &read_buffer.get_block(col, row[col]);
    while (!current_block->is_ready()) {
      poll();
    }

    u32& current_pos = current_positions[col];

    // a new block
    if (current_pos == init_pos && !current_block->points_to_null()) {
      auto [mem, offset] = current_block->get_remote_ptr();
      u32 next_row = (row[col] + 1) % inv_index::block_based::READ_BUFFER_DEPTH;
      post_READ(col, next_row, mem, offset);
    }

    while (current_pos < block_entries &&
           // either current_pos hits a cache line version
           ((cache_line_versions &&
             (current_pos * sizeof(u32)) % CACHE_LINE_SIZE == 0) ||
            // or the following must hold to advance the position
            (current_block->buffer[current_pos] != tombstone &&
             current_block->buffer[current_pos] < current_value))) {
      ++current_pos;
    }

    // we reached the end of the block
    // case 1: the block is full, and we are at the end
    // case 2: the block has been split but no more items are in this block
    if (current_pos == block_entries ||
        current_block->buffer[current_pos] == tombstone) {
      // we can not reach another match since all the values in this block
      // are smaller than current_val, and the block has no successor
      if (current_block->points_to_null()) {
        break;
      }

      // go to the next block but in the same list
      row[col] = (row[col] + 1) % inv_index::block_based::READ_BUFFER_DEPTH;
      current_pos = init_pos;
      continue;
    }

    // found match
    else if (current_block->buffer[current_pos] == current_value) {
      ++count;
      ++current_pos;

      // skip cache line versions
      if constexpr (cache_line_versions) {
        if (current_pos != block_entries &&
            (current_pos * sizeof(u32)) % CACHE_LINE_SIZE == 0) {
          ++current_pos;
        }
      }

      // match found
      if (count == query_length) {
        result_handler(current_value);

        // now we need to determine the new current_value

        // if next value is the pointer, we have to READ the next block
        // check once more against the end of the block
        if (current_pos == block_entries ||
            current_block->buffer[current_pos] == tombstone) {
          // same reasoning as before
          if (current_block->points_to_null()) {
            break;
          }

          row[col] = (row[col] + 1) % inv_index::block_based::READ_BUFFER_DEPTH;
          current_pos = init_pos;

          current_block = &read_buffer.get_block(col, row[col]);
          while (!current_block->is_ready()) {
            poll();
          }

          // a new block
          if (!current_block->points_to_null()) {
            auto [m_next, o_next] = current_block->get_remote_ptr();
            u32 next_row =
              (row[col] + 1) % inv_index::block_based::READ_BUFFER_DEPTH;
            post_READ(col, next_row, m_next, o_next);
          }
        }

        // set new value
        current_value = current_block->buffer[current_pos];
        count = 1;
        ++current_pos;
      }

      // no match and not end of buffer
    } else if (current_block->buffer[current_pos] > current_value) {
      current_value = current_block->buffer[current_pos];
      count = 1;
      ++current_pos;
    }

    // go to next list
    col = (col + 1) % query_length;  // cycle
  }
}

}  // namespace operations

#endif  // INDEX_BLOCK_BASED_BLOCK_OPERATIONS_HH
