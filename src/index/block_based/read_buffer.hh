#ifndef INDEX_BLOCK_BASED_READ_BUFFER_HH
#define INDEX_BLOCK_BASED_READ_BUFFER_HH

#include <library/context.hh>
#include <library/hugepage.hh>
#include <library/memory_region.hh>
#include <library/utils.hh>

#include "index/constants.hh"
// #include "index/crc.hh"

namespace inv_index::block_based {

template <bool cache_line_versioning>
class ReadBuffer {
public:
  struct BufferBlock {
    const u32 block_length;

    // store temporary for re-reads: TODO
    u32 memory_node{};
    u32 remote_offset{};
    MRT* mrt{};

    bool ready{false};
    bool is_valid{false};
    bool just_writing{false};

    bool is_ready() const { return ready && !just_writing; }

    u32* buffer;
    BufferBlock* next{nullptr};

    BufferBlock(u32* buffer_ptr, u32 block_size)
        : block_length(block_size / sizeof(u32)), buffer(buffer_ptr) {}

    u64 get_address() const { return reinterpret_cast<u64>(buffer); }

    u64* get_last_word_ptr() const {
      // caution: last 64b word must be interpreted as 64b* (endianness)
      return reinterpret_cast<u64*>(buffer) + block_length / 2 - 1;
    }

    u64 get_last_word() const {
      // caution: last 64b word must be interpreted as 64b* (endianness)
      return *get_last_word_ptr();
    }

    u16 get_remote_ptr_tag() const {
      const u32 r_ptr_word1 = buffer[block_length - 4];
      return r_ptr_word1 >> 16;
    }

    u16 get_block_tag() const {
      return (get_last_word() << (32 + 14)) >> (64 - 16);
    }

    u64 get_raw_remote_ptr() const {
      return (static_cast<u64>(buffer[block_length - 4]) << 32) |
             buffer[block_length - 3];
    }

    void set_raw_remote_ptr(u64 r_ptr) {
      buffer[block_length - 4] = r_ptr >> 32;
      buffer[block_length - 3] = (r_ptr << 32) >> 32;
    }

    std::pair<u32, u32> get_remote_ptr() const {
      if constexpr (cache_line_versioning) {
        // we interpret the remote pointer with 2x 32bit half-words
        const u64 r_ptr = get_raw_remote_ptr();
        const u32 node = (r_ptr << 16) >> (64 - 10);
        const u32 offset = (r_ptr << 26) >> (64 - 38);
        return {node, offset};

      } else {
        return {buffer[block_length - 2], buffer[block_length - 1]};
      }
    }

    bool points_to_null() const {
      auto [node, offset] = get_remote_ptr();
      return node == 0 && offset == 0;
    }

    bool is_full() const {
      return buffer[block_length - 5] != static_cast<u32>(-1);
    }

    //    u64 get_encoded_crc() const {
    //      return (static_cast<u64>(buffer[block_length - 3]) << 32) |
    //             buffer[block_length - 2];
    //    }

    bool validate_cache_lines() {
      const u32 num_cache_lines = block_length * sizeof(u32) / CACHE_LINE_SIZE;
      const u32 version = buffer[0];

      for (u32 j = 1; j < num_cache_lines; ++j) {
        u32 idx = j * CACHE_LINE_SIZE / sizeof(u32);

        if (buffer[idx] != version) {
          is_valid = false;
          return false;
        }
      }

      is_valid = true;
      return true;
    }

    void increase_cache_line_versions() {
      const u32 num_cache_lines = block_length * sizeof(u32) / CACHE_LINE_SIZE;
      const u32 version = buffer[0] + 1;

      for (u32 j = 0; j < num_cache_lines; ++j) {
        u32 idx = j * CACHE_LINE_SIZE / sizeof(u32);
        buffer[idx] = version;
      }

      // also increase the version of the last word (but interpreted w/ 64bit)
      u64& last_word = *get_last_word_ptr();

      last_word = (last_word << 32) >> 32;  // remove old version
      last_word |= (static_cast<u64>(version) << 32);
    }

    std::tuple<u32, u32, u32> get_min_max() {
      const u32 min = buffer[1];
      lib_assert(min != static_cast<u32>(-1), "invalid state");

      u32 max_pos = block_length - 5;
      while (max_pos > 1) {
        // if we hit a cache line version or value is empty
        if ((max_pos % (CACHE_LINE_SIZE / sizeof(u32)) == 0) ||
            (buffer[max_pos] == static_cast<u32>(-1))) {
          max_pos--;
        } else {
          break;
        }
      }

      return {min, buffer[max_pos], max_pos};
    }

    bool is_locked() const { return get_last_word() & 1; }
    void set_lock() { *get_last_word_ptr() |= 1; }
    void set_unlock() { *get_last_word_ptr() &= ~1; }

    // returns the first free positions of the involved blocks
    std::pair<u32, u32> split_block(BufferBlock& target) {
      const u32 num_cache_lines = block_length * sizeof(u32) / CACHE_LINE_SIZE;
      const u32 read_until = block_length - DYNAMIC_FOOTER_SIZE / sizeof(u32);
      const u32 tombstone = static_cast<u32>(-1);

      u32 target_iter = 1;
      u32 move_from = num_cache_lines / 2 * CACHE_LINE_ITEMS + 1;

      for (u32 i = move_from; i < read_until; ++i) {
        if (i % CACHE_LINE_ITEMS == 0) {
          continue;
        }

        target.buffer[target_iter] = buffer[i];
        buffer[i] = tombstone;
        ++target_iter;

        if (target_iter % CACHE_LINE_ITEMS == 0) {
          ++target_iter;
        }
      }

      // invalidate remaining entries
      for (u32 j = target_iter; j < read_until; ++j) {
        if (j % CACHE_LINE_ITEMS != 0) {
          target.buffer[j] = tombstone;
        }
      }

      return {move_from, target_iter};
    }

    [[maybe_unused]] void print_block() const {
      std::cerr << "{ [ ";
      for (u32 i = 0; i < block_length - 2; ++i) {
        if (i != 0 && i % CACHE_LINE_ITEMS == 0) {
          std::cerr << "\b\b]\n  [ ";
        }

        std::cerr << i << ": " << buffer[i] << " | ";
      }
      // last word must be interpreted as 64b pointer (endianness)
      std::cerr << block_length - 1 << " (64b): "
                << *(reinterpret_cast<u64*>(buffer) + block_length / 2 - 1)
                << " ]\n}\n";
    }

    //    bool checksum_is_valid() const {
    //      const u64 computed_crc = crc64(reinterpret_cast<byte*>(buffer),
    //                                     (block_length - 3) * sizeof(u32));
    //      return computed_crc == get_encoded_crc();
    //    }
  };

  ReadBuffer(u32 block_size, HugePage<u32>& local_buffer)
      : block_size(block_size),
        buffer_blocks_(READ_BUFFER_LENGTH),
        posted_blocks_(READ_BUFFER_LENGTH) {
    const size_t total_buffer_size =
      block_size * READ_BUFFER_LENGTH * READ_BUFFER_DEPTH;
    u32* buffer_ptr = local_buffer.get_slice(total_buffer_size);

    for (auto& buffer_block_col : buffer_blocks_) {
      buffer_block_col.reserve(READ_BUFFER_DEPTH);

      for (u32 j = 0; j < READ_BUFFER_DEPTH; ++j) {
        // allocate block
        BufferBlock& block =
          buffer_block_col.emplace_back(buffer_ptr, block_size);
        buffer_ptr += block_size / sizeof(u32);

        // link previous block
        if (j > 0) {
          buffer_block_col[j - 1].next = &block;
        }

        // link final block to first block
        if (j == READ_BUFFER_DEPTH - 1) {
          block.next = &buffer_block_col.front();
        }
      }
    }
  }

  ReadBuffer& operator=(const ReadBuffer&) = delete;
  BufferBlock& get_block(u32 col, u32 row) { return buffer_blocks_[col][row]; }

  void set_block_ready(u32 col, u32 row) {
    buffer_blocks_[col][row].ready = true;
  }

public:
  const u32 block_size;

private:
  vec<vec<BufferBlock>> buffer_blocks_;
  vec<u32> posted_blocks_;
};

}  // namespace inv_index::block_based

#endif  // INDEX_BLOCK_BASED_READ_BUFFER_HH
