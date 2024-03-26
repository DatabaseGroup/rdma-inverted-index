#ifndef INDEX_MEMORY_NODE_HH
#define INDEX_MEMORY_NODE_HH

#include <fstream>
#include <library/connection_manager.hh>
#include <library/detached_qp.hh>
#include <library/hugepage.hh>
#include <library/utils.hh>
#include <timing/timing.hh>

#include "configuration.hh"
#include "constants.hh"
#include "core_assignment.hh"

namespace inv_index {

template <bool dynamic = false>
class MemoryNode {
  using Configuration = configuration::IndexConfiguration;
  using CoreAssignment = CoreAssignment<AssignmentPolicy::interleaved>;

public:
  explicit MemoryNode(Configuration& config)
      : context_(config),
        cm_(context_, config),
        num_clients_(config.num_clients),
        index_region_(context_) {
    auto t_read_index = timing_.create_enroll("read_index_into_memory");
    cm_.connect_to_clients();

    if (!config.disable_thread_pinning) {
      const u32 core = core_assignment_.get_available_core();
      pin_main_thread(core);
      print_status("pinned main thread to core " + std::to_string(core));
    }

    // receive index location
    str index_file = receive_index_location();
    std::cerr << "index file: " << index_file << std::endl;

    if constexpr (dynamic) {
      print_status("receive block size");
      block_size_ = cm_.initiator_qp->receive_u32(context_);
      std::cerr << "block size: " << block_size_ << std::endl;
    }

    auto [file_stream, index_size] = open_index_file(index_file);
    auto free_list_offset = allocate_memory(index_size);

    t_read_index->start();
    read_index_into_memory(file_stream, index_size);
    t_read_index->stop();

    // communicate index size, buffer size, and possibly free list offset
    vec<size_t> sizes = {index_size, index_buffer_.buffer_size};
    std::cerr << "index size: " << index_size << std::endl;
    std::cerr << "total index buffer size: " << sizes.back() << std::endl;

    if constexpr (dynamic) {
      lib_assert(free_list_offset.has_value(), "...");
      std::cerr << "freelist offset: " << free_list_offset.value() << std::endl;
      sizes.push_back(free_list_offset.value());
    }

    for (QP& qp : cm_.client_qps) {
      qp->post_send_inlined(
        sizes.data(), sizes.size() * sizeof(size_t), IBV_WR_SEND);
      context_.poll_send_cq_until_completion();
    }

    print_status("register memory and distribute access token");
    index_region_.register_memory(
      index_buffer_.get_full_buffer(), index_buffer_.buffer_size, true);
    MemoryRegionToken token = index_region_.createToken();

    for (QP& qp : cm_.client_qps) {
      qp->post_send_inlined(std::addressof(token), sizeof(token), IBV_WR_SEND);
      context_.poll_send_cq_until_completion();
    }

    // connect for each compute thread a new QP
    print_status("connect QPs of compute threads");
    vec<u_ptr<DetachedQP>> qps;

    // TODO: QP sharing not yet implemented for the dynamic block based index

    const u32 qps_per_node = dynamic
                               ? num_compute_threads_
                               : std::min<u32>(num_compute_threads_, MAX_QPS);
    qps.reserve(num_clients_ * qps_per_node);

    for (QP& client_qp : cm_.client_qps) {
      for (u32 thread_id = 0; thread_id < qps_per_node; ++thread_id) {
        auto& qp = qps.emplace_back(std::make_unique<DetachedQP>(context_));
        qp->connect(context_, context_.get_lid(), client_qp);
      }
    }

    // notify compute nodes that we are ready
    cm_.synchronize();

    // wait until we get notifications from all compute nodes to terminate
    idle();
    index_buffer_.deallocate();

    std::cout << timing_ << std::endl;
  }

private:
  str receive_index_location() {
    print_status("receive index file location");
    str index_file;

    num_compute_threads_ = cm_.initiator_qp->receive_u32(context_);
    u32 len = cm_.initiator_qp->receive_u32(context_);

    index_file.resize(len);
    LocalMemoryRegion string_region{context_, index_file.data(), len};
    cm_.initiator_qp->post_receive(string_region);
    context_.receive();

    return index_file;
  }

  std::pair<std::ifstream, size_t> open_index_file(const str& index_file) {
    std::ifstream file(index_file, std::ios::binary);
    lib_assert(file.good(), "file \"" + index_file + "\" does not exist");

    file.unsetf(std::ios::skipws);  // w/out that we are missing data
    size_t file_size;

    // determine file size and set cursor back to the beginning
    file.seekg(0, std::ios::end);
    file_size = file.tellg();
    file.seekg(0, std::ios::beg);

    return {std::move(file), file_size};
  }

  // head1 (64) | head2 (64) | ... | block0-next (32) | block1-next (32) | ...
  void initialize_freelist(size_t num_index_blocks, size_t total_blocks) {
    print_status("initialize free list");
    lib_assert(total_blocks < static_cast<u32>(-1),
               "cannot address all blocks with 4B");

    const u64 begin_addr =
      reinterpret_cast<u64>(index_buffer_.get_full_buffer()) +
      total_blocks * block_size_;

    // set up heads
    for (u32 i = 0; i < block_based::FREELIST_PARTITIONS; ++i) {
      *(reinterpret_cast<u64*>(begin_addr) + i) = num_index_blocks + i;
    }

    u32* free_list_ptr = reinterpret_cast<u32*>(
      begin_addr + sizeof(u64) * block_based::FREELIST_PARTITIONS);

    for (u32 i = 0; i < total_blocks; ++i) {
      // those blocks are occupied
      if (i < num_index_blocks) {
        *free_list_ptr++ = static_cast<u32>(-1);  // nullptr

      } else {
        // set next pointer to next block wrt the number of heads
        u32 point_to = i + block_based::FREELIST_PARTITIONS;
        point_to = point_to < total_blocks ? point_to : static_cast<u32>(-1);

        *free_list_ptr++ = point_to;
      }
    }
  }

  std::optional<u32> allocate_memory(size_t index_size) {
    auto t_allocate = timing_.create_enroll("allocate_index_buffer");
    std::cerr << "index file size: " << index_size << std::endl;
    std::optional<u32> free_list_offset;

    t_allocate->start();
    if constexpr (dynamic) {
      const size_t num_index_blocks = index_size / block_size_;
      std::cerr << "num index blocks: " << num_index_blocks << std::endl;
      const size_t available_memory = index_buffer_.get_memory_size();

      const size_t additional_blocks = 1000000;  // TODO: for now
      const size_t total_blocks = num_index_blocks + additional_blocks;
      std::cerr << "num total blocks: " << total_blocks << std::endl;

      const size_t free_list_size =
        block_based::FREELIST_PARTITIONS * sizeof(u64) +  // heads (each 8B)
        total_blocks * sizeof(u32);
      const size_t allocation_size =
        total_blocks * block_size_ + free_list_size;
      lib_assert(allocation_size <= available_memory,
                 "block allocation failed");

      // pre-allocate all available huge pages
      //      index_buffer_.allocate(index_buffer_.get_memory_size());
      index_buffer_.allocate(allocation_size);
      index_buffer_.touch_memory();

      initialize_freelist(num_index_blocks, total_blocks);
      free_list_offset = total_blocks;
    } else {
      // just allocate the size of index
      index_buffer_.allocate(index_size);
    }
    t_allocate->stop();

    return free_list_offset;
  }

  void read_index_into_memory(std::ifstream& file_stream, size_t file_size) {
    print_status("read index into memory");
    auto t_read = timing_.create_enroll("read_file");

    t_read->start();
    file_stream.read(reinterpret_cast<char*>(index_buffer_.get_full_buffer()),
                     file_size);
    t_read->stop();
  }

  void idle() {
    print_status("idle");

    // dummy region
    bool done;
    LocalMemoryRegion region{context_, &done, sizeof(bool)};

    for (QP& qp : cm_.client_qps) {
      qp->post_receive(region);
    }

    // wait
    context_.receive(num_clients_);
  }

private:
  Context context_;
  ServerConnectionManager cm_;
  CoreAssignment core_assignment_;

  const u32 num_clients_;
  u32 num_compute_threads_{};

  HugePage<byte> index_buffer_;
  MemoryRegion index_region_;
  timing::Timing timing_;
  u32 block_size_;
};

}  // namespace inv_index

#endif  // INDEX_MEMORY_NODE_HH
