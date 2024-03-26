#ifndef INDEX_BLOCK_BASED_DYNAMIC_REMOTE_POINTER_HH
#define INDEX_BLOCK_BASED_DYNAMIC_REMOTE_POINTER_HH

#include <library/memory_region.hh>
#include <ostream>

#include "compute_thread.hh"
#include "insert.hh"

namespace inv_index::block_based::dynamic {

struct RemotePtr {
  u32 memory_node{0};
  u32 offset{0};  // 32b are sufficient to address the blocks in a memory node
                  // (we assume that the block size is sufficiently large)
                  // offset is just the number of the block (not an address)

  static inline u32 block_size;
  using BufferBlock = ReadBuffer<true>::BufferBlock;

  // this only works for remote pointers contained in blocks (not in the
  // catalog), because (0, 0) is always the very first block due to the
  // partitioning scheme, so no block can point to a previous block
  inline bool is_null() const { return memory_node == 0 && offset == 0; }

  // [ p_tag (16) | m_id (10) | offset(38) ]
  static u64 encode_remote_ptr(u16 tag, u32 m_id, u32 offset) {
    u64 d_word = 0;

    auto encode = [&](u64 enc, u32 bits) {
      const u64 zeroed = (enc << (64 - bits)) >> (64 - bits);
      d_word |= zeroed;
    };

    encode(tag, 16);

    d_word <<= 10;
    encode(m_id, 10);

    d_word <<= 38;
    encode(offset, 38);

    return d_word;
  }

  void READ_block(u32 col, u32 row, MRT& mrt, u_ptr<ComputeThread>& thread) {
    auto& block = thread->read_buffer.get_block(col, row);
    const u64 wr_id = encode_64bit(col, row);

    READ_block(block, wr_id, thread->buffer_region.get_lkey(), mrt, thread);
  }

  void READ_block(BufferBlock& block,
                  u64 wr_id,
                  u32 lkey,
                  MRT& mrt,
                  u_ptr<ComputeThread>& thread) {
    block.ready = false;
    // TODO
    block.memory_node = memory_node;
    block.remote_offset = offset;
    block.mrt = &mrt;

    thread->post_balance++;
    thread->rdma_reads_in_bytes += block_size;

    QP& qp = thread->qps[memory_node]->qp;

    qp->post_send(block.get_address(),
                  block_size,
                  lkey,
                  IBV_WR_RDMA_READ,
                  true,
                  false,
                  mrt.get(),
                  offset * static_cast<u64>(block_size),
                  0,
                  wr_id);

    // because offset is block-wise
    // caution: offset * block_size must be u64
  }

  static void WRITE_block(BufferBlock& block,
                          u64 wr_id,
                          u32 lkey,
                          QP& qp,
                          u32 remote_offset,
                          MRT& mrt,
                          u_ptr<ComputeThread>& thread) {
    block.increase_cache_line_versions();
    block.just_writing = true;
    thread->post_balance++;

    qp->post_send(block.get_address(),
                  block_size,
                  lkey,
                  IBV_WR_RDMA_WRITE,
                  true,
                  false,
                  mrt.get(),
                  remote_offset * static_cast<u64>(block_size),
                  0,
                  wr_id);
  }

  static void WRITE_and_unlock_block(u32 col,
                                     u32 row,
                                     QP& qp,
                                     u32 remote_offset,
                                     MRT& mrt,
                                     u_ptr<ComputeThread>& thread) {
    auto& block = thread->read_buffer.get_block(col, row);
    block.set_unlock();

    WRITE_block(block,
                encode_64bit(col, row),
                thread->buffer_region.get_lkey(),
                qp,
                remote_offset,
                mrt,
                thread);

    // I think we can skip this
    //    while (thread->post_balance > 0) {
    //      thread->poll_cq_and_handle();
    //    }
  }

  static bool LOCK_block(BufferBlock& block,
                         QP& qp,
                         MRT& mrt,
                         u32 remote_offset,
                         u_ptr<ComputeThread>& thread) {
    const u64 compare = block.get_last_word();
    const u64 swap = compare | 1;

    thread->post_balance++;
    thread->post_balance_CAS++;
    qp->post_CAS(thread->cas_region,
                 mrt.get(),
                 static_cast<u64>(remote_offset + 1) * block_size - sizeof(u64),
                 compare,
                 swap);

    while (thread->post_balance_CAS > 0) {
      thread->poll_cq_and_handle();
    }

    if (thread->cas_buffer != compare) {
      return false;  // failure
    }

    // block is locked now
    block.set_lock();
    return true;  // success
  }

  template <typename FAllocate, typename FInsert>
  void allocate_and_write_block(BufferBlock& block,
                                MemoryRegionTokens& remote_access_tokens,
                                u_ptr<ComputeThread>& thread,
                                FAllocate allocate_block,
                                FInsert inserter) {
    RemotePtr r = allocate_block();
    auto& allocation_block = thread->allocation_block;

    while (allocation_block.just_writing) {
      // wait
    }

    // read block (we must keep the block tag to detect ABA issues)
    r.READ_block(allocation_block,
                 WR_READ_NO_HANDLE,
                 thread->allocation_region.get_lkey(),
                 remote_access_tokens[r.memory_node],
                 thread);
    while (thread->post_balance > 0) {
      thread->poll_cq_and_handle();
    }

    // assert that block has been cleaned during de-allocation

    const auto [b1_free_pos, b2_free_pos] = block.split_block(allocation_block);
    inserter(b1_free_pos, b2_free_pos, allocation_block.buffer);

    allocation_block.set_raw_remote_ptr(block.get_raw_remote_ptr());

    const u16 a_block_tag = allocation_block.get_block_tag();
    block.set_raw_remote_ptr(
      encode_remote_ptr(a_block_tag, r.memory_node, r.offset));

    WRITE_block(allocation_block,
                WR_WRITE_ALLOCATION_BLOCK,
                thread->allocation_region.get_lkey(),
                thread->qps[r.memory_node]->qp,
                r.offset,
                remote_access_tokens[r.memory_node],
                thread);

    // wait until done with writing
    while (thread->post_balance > 0) {
      thread->poll_cq_and_handle();
    }
  }

  // TODO: move to a separate class?
  template <typename F>
  bool find_block_and_insert(u32 id,
                             u32 col,
                             MemoryRegionTokens& remote_access_tokens,
                             u_ptr<ComputeThread>& thread,
                             F allocate_block) {
    u32 row = 0;
    READ_block(col, row, remote_access_tokens[memory_node], thread);

    u16 expected_tag = 0;  // first block has always a zero tag
    u32 node = memory_node;
    u32 offs = offset;

    while (true) {
      while (thread->post_balance > 0) {
        thread->poll_cq_and_handle();
      }

      auto& block = thread->read_buffer.get_block(col, row);

      if (!block.is_valid) {
        // optimistic read failed, re-start READing the current block
        RemotePtr p{node, offs};
        p.READ_block(col, row, remote_access_tokens[node], thread);
        thread->block_repeated_reads++;
        thread->read_failed++;

        continue;
      }

      // happens if the block has been re-used meanwhile (invalid r_pointer)
      // we must restart the whole operation (re-start READing the list)
      if (block.get_block_tag() != expected_tag) {
        thread->list_repeated_reads++;
        return false;
      }

      expected_tag = block.get_remote_ptr_tag();
      auto [next_node, next_offs] = block.get_remote_ptr();

      // post next READ
      if (!block.points_to_null()) {
        RemotePtr p{next_node, next_offs};
        u32 next_row = (row + 1) % READ_BUFFER_DEPTH;
        p.READ_block(col, next_row, remote_access_tokens[next_node], thread);
      }

      auto [min, max, max_pos] = block.get_min_max();

      // we cannot overwrite a cache line version
      const u32 insert_pos =
        ((max_pos + 1) % (CACHE_LINE_SIZE / sizeof(u32)) == 0) ? max_pos + 2
                                                               : max_pos + 1;

      QP& qp = thread->qps[node]->qp;
      MRT& mrt = remote_access_tokens[node];

      if (max < id) {
        if (block.points_to_null()) {
          if (!LOCK_block(block, qp, mrt, offs, thread)) {
            // locking failed, we must reREAD the block
            RemotePtr p{node, offs};
            p.READ_block(col, row, remote_access_tokens[node], thread);
            thread->block_repeated_reads++;
            thread->locking_failed++;

            continue;
          }

          // case 1: block is full: allocate new block, divide items among the
          //         blocks, and append item to the newly allocated block
          if (block.is_full()) {
            const auto inserter =
              [&](u32, u32 free_pos, u32* allocation_block_buffer) {
                allocation_block_buffer[free_pos] = id;
              };

            allocate_and_write_block(
              block, remote_access_tokens, thread, allocate_block, inserter);

            // now we can write and unlock the initial block
            WRITE_and_unlock_block(col, row, qp, offs, mrt, thread);

            // case 2: block is not full: just append item to then end
          } else {
            block.buffer[insert_pos] = id;
            WRITE_and_unlock_block(col, row, qp, offs, mrt, thread);
          }

          break;  // end of loop, we are done
        }
        // if max < id and block is not null, we move on

        // value is in between
      } else {
        if (!LOCK_block(block, qp, mrt, offs, thread)) {
          // locking failed, we must reREAD the block
          RemotePtr p{node, offs};
          p.READ_block(col, row, remote_access_tokens[node], thread);
          thread->block_repeated_reads++;
          thread->locking_failed++;

          continue;
        }

        // case 3: block is full: allocate new block, divide items among the
        //         blocks, and insert the item ordered in one of the blocks
        if (block.is_full()) {
          const auto inserter = [&](u32 b1_free_pos,
                                    u32 b2_free_pos,
                                    u32* allocation_block_buffer) {
            // insert into first block
            if (id < allocation_block_buffer[1]) {
              ordered_insert(block.buffer, id, b1_free_pos, block_size);

              // insert into second block
            } else {
              ordered_insert(
                allocation_block_buffer, id, b2_free_pos, block_size);
            }
          };

          allocate_and_write_block(
            block, remote_access_tokens, thread, allocate_block, inserter);

          // now we can write and unlock the initial block
          WRITE_and_unlock_block(col, row, qp, offs, mrt, thread);

        } else {
          // case 4: block is not full: just insert the item ordered
          ordered_insert(block.buffer, id, insert_pos, block_size);
          WRITE_and_unlock_block(col, row, qp, offs, mrt, thread);
        }

        break;  // end of loop, we are done
      }

      node = next_node;
      offs = next_offs;
      row = (row + 1) % READ_BUFFER_DEPTH;
    }

    return true;
  }

  friend std::ostream& operator<<(std::ostream& os, const RemotePtr& r) {
    os << "[node: " << r.memory_node << ", offset: " << r.offset << "]";

    if (r.is_null()) {
      os << " (possibly null)";
    }

    return os;
  }
};

using RemotePointers = vec<RemotePtr>;

}  // namespace inv_index::block_based::dynamic

#endif  // INDEX_BLOCK_BASED_DYNAMIC_REMOTE_POINTER_HH
