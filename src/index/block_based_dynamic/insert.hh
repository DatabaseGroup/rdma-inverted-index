#ifndef INDEX_BLOCK_BASED_DYNAMIC_INSERT_HH
#define INDEX_BLOCK_BASED_DYNAMIC_INSERT_HH

#include <library/types.hh>

#include "index/constants.hh"

namespace inv_index::block_based::dynamic {

// search the block, return the cache line
u32 binary_search_block(const u32* buffer, u32 value, u32 num_cache_lines) {
  u32 count = num_cache_lines;
  u32 first = 0;

  while (count > 0) {
    u32 it = first;
    u32 step = count / 2;
    it += step;

    u32 buffer_pos = (it + 1) * CACHE_LINE_ITEMS - 1;

    // if we are in the last cache line, we skip the footer
    if (it == num_cache_lines - 1) {
      buffer_pos -= DYNAMIC_FOOTER_SIZE / sizeof(u32);
    }

    if (value > buffer[buffer_pos]) {
      first = ++it;
      count -= step + 1;
    } else {
      count = step;
    }
  }

  return first;
}

// search the cache line, return the position in the cache line
u32 binary_search_cacheline(const u32* buffer,
                            u32 value,
                            u32 cache_line,
                            u32 num_cache_lines) {
  lib_assert(cache_line < num_cache_lines, "invalid cache line");
  u32 count = CACHE_LINE_ITEMS - 1;
  u32 first = 1;  // skip cache line versions

  // if we are in the last cache line, we skip the footer
  if (cache_line == num_cache_lines - 1) {
    count -= DYNAMIC_FOOTER_SIZE / sizeof(u32);
  }

  while (count > 0) {
    u32 it = first;
    u32 step = count / 2;
    it += step;

    if (value > buffer[cache_line * CACHE_LINE_ITEMS + it]) {
      first = ++it;
      count -= step + 1;
    } else {
      count = step;
    }
  }

  return first;
}

// we assert that there is at least one free space
void ordered_insert(u32* buffer, u32 value, u32 free_pos, u32 block_size) {
  const u32 num_cache_lines = block_size / CACHE_LINE_SIZE;
  const u32 cl = binary_search_block(buffer, value, num_cache_lines);
  const u32 pos_in_cl =
    binary_search_cacheline(buffer, value, cl, num_cache_lines);
  const u32 buffer_pos = cl * CACHE_LINE_ITEMS + pos_in_cl;

  // move all items by one except for cache line versions
  for (u32 i = free_pos; i > buffer_pos; --i) {
    if ((i - 1) % CACHE_LINE_ITEMS == 0) {
      buffer[i] = buffer[i - 2];

    } else if (i % CACHE_LINE_ITEMS != 0) {
      buffer[i] = buffer[i - 1];
    }
  }

  buffer[buffer_pos] = value;
}

}  // namespace inv_index::block_based::dynamic

#endif  // INDEX_BLOCK_BASED_DYNAMIC_INSERT_HH
