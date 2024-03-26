#include "context.hh"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>

#include "queue_pair.hh"
#include "utils.hh"

Context::Context(Configuration& config,
                 const i32 device_idx,
                 bool create_shared_rcq)
    : config_(config) {
  i32 num_devices = 0;
  IBDeviceList device_list = ibv_get_device_list(&num_devices);

  lib_assert(num_devices > 0, "No InfiniBand devices found");
  lib_assert(device_list != nullptr, "Device list is null");
  lib_assert(0 <= device_idx && device_idx < num_devices,
             "Device " + std::to_string(device_idx) + " not found");

  device_ = device_list[device_idx];

  std::cerr << num_devices << " device(s) found" << std::endl;
  std::cerr << "Selected device: " << ibv_get_device_name(device_) << std::endl;

  context_ = ibv_open_device(device_);
  lib_assert(device_ && context_, "Cannot open device");

  // allocate protection domain
  protection_domain_ = ibv_alloc_pd(context_);

  // query port
  lib_assert(
    ibv_query_port(context_, config_.device_port, &port_attributes_) == 0,
    "Cannot query port " + std::to_string(config_.device_port));

  // create completion queues
  send_cq_ =
    ibv_create_cq(context_, config_.max_send_queue_wr, nullptr, nullptr, 0);
  receive_cq_ =
    ibv_create_cq(context_, config_.max_recv_queue_wr, nullptr, nullptr, 0);

  lib_assert(send_cq_ && receive_cq_, "Cannot create completion queues");

  if (create_shared_rcq) {
    ibv_srq_init_attr attributes{};
    attributes.srq_context = context_;
    attributes.attr.max_wr = config_.max_recv_queue_wr;
    attributes.attr.max_sge = 1;
    shared_receive_cq_ = ibv_create_srq(protection_domain_, &attributes);

    lib_assert(shared_receive_cq_,
               "Cannot create shared receive completion queue");
  }
}

Context::~Context() {
  lib_assert(!shared_receive_cq_ || ibv_destroy_srq(shared_receive_cq_) == 0,
             "Cannot destroy shared receive completion queue");
  lib_assert(ibv_destroy_cq(receive_cq_) == 0,
             "Cannot destroy receive completion queue");
  lib_assert(ibv_destroy_cq(send_cq_) == 0,
             "Cannot destroy send completion queue");
  lib_assert(ibv_dealloc_pd(protection_domain_) == 0,
             "Cannot deallocate protection domain");
  lib_assert(ibv_close_device(context_) == 0, "Cannot close device.");

  close_server_socket();
}

void Context::bind_to_port(u32 tcp_port) {
  server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
  lib_assert(server_socket_ >= 0, "Cannot open socket.");

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(tcp_port);

  // activate reuse address option
  i32 option_val = 1;
  lib_assert(setsockopt(server_socket_,
                        SOL_SOCKET,
                        SO_REUSEADDR,
                        &option_val,
                        sizeof(option_val)) == 0,
             "Cannot set socket option to reuse address");

  lib_assert(
    bind(server_socket_, (sockaddr*)&address, sizeof(sockaddr_in)) == 0,
    "Cannot bind to port " + std::to_string(tcp_port));

  lib_assert(listen(server_socket_, 128) == 0, "Cannot listen on socket");
}

void Context::close_server_socket() const {
  if (server_socket_ >= 0) {
    close(server_socket_);
  }
}

std::pair<QP, u32> Context::wait_for_connection() {
  QP queue_pair = std::make_unique<QueuePair>(this);

  QPInfo receive_buffer{}, send_buffer{get_lid(), queue_pair->get_qp_num()};
  ssize_t qp_size = sizeof(QPInfo);

  i32 tcp_socket = accept(server_socket_, nullptr, nullptr);
  lib_assert(tcp_socket >= 0, "Cannot open socket.");

  lib_debug("Exchange QP information with client");
  lib_assert(recv(tcp_socket, &receive_buffer, qp_size, 0) == qp_size,
             "Received an incorrect number of bytes");
  lib_assert(send(tcp_socket, &send_buffer, qp_size, 0) == qp_size,
             "Transmitted an incorrect number of bytes");

  std::cerr << "pairing: " << queue_pair->get_qp_num() << " -- "
            << receive_buffer.qp_number << std::endl;

  queue_pair->transition_to_rtr(receive_buffer);
  queue_pair->transition_to_rts();

  // TODO: set remote user data

  close(tcp_socket);

  return {std::move(queue_pair), receive_buffer.node_id};
}

QP Context::connect_to_server(const str& address, u32 tcp_port, u32 node_id) {
  QP queue_pair = std::make_unique<QueuePair>(this);

  QPInfo send_buffer{get_lid(), queue_pair->get_qp_num(), node_id},
    receive_buffer{};
  ssize_t qp_size = sizeof(QPInfo);

  sockaddr_in remote_address{};
  remote_address.sin_family = AF_INET;
  remote_address.sin_port = htons(tcp_port);
  inet_pton(AF_INET, address.c_str(), &(remote_address.sin_addr));

  i32 tcp_socket = socket(AF_INET, SOCK_STREAM, 0);
  lib_assert(tcp_socket >= 0, "Cannot open socket.");

  lib_debug("Connect to server with address " + address);
  while (connect(tcp_socket, (sockaddr*)&remote_address, sizeof(sockaddr_in)) !=
         0) {
    // wait until server opens a connection
  }

  lib_debug("Exchange QP information with server");
  lib_assert(send(tcp_socket, &send_buffer, qp_size, 0) == qp_size,
             "Transmitted an incorrect number of bytes");
  lib_assert(recv(tcp_socket, &receive_buffer, qp_size, 0) == qp_size,
             "Received an incorrect number of bytes");

  std::cerr << "pairing: " << queue_pair->get_qp_num() << " -- "
            << receive_buffer.qp_number << std::endl;

  queue_pair->transition_to_rtr(receive_buffer);
  queue_pair->transition_to_rts();
  close(tcp_socket);

  return queue_pair;
}

void Context::post_shared_receive(MemoryRegion& region) {
  ibv_recv_wr work_request{};
  ibv_sge scatter_gather_entry{};
  ibv_recv_wr* bad_work_request{nullptr};

  lib_assert(shared_receive_cq_, "No shared receive CQ exists");

  scatter_gather_entry.addr = region.get_address();
  scatter_gather_entry.length = region.get_size_in_bytes();
  scatter_gather_entry.lkey = region.get_lkey();

  work_request.wr_id = reinterpret_cast<u64>(&region);
  work_request.next = nullptr;
  work_request.sg_list = &scatter_gather_entry;
  work_request.num_sge = 1;

  lib_assert(ibv_post_srq_recv(
               get_shared_receive_cq(), &work_request, &bad_work_request) == 0,
             "Cannot post shared receive request");
  lib_debug("Shared receive request successfully posted");
}

// static function
i32 Context::poll_recv_cq(ibv_wc* work_completion,
                          const i32 max_cqes,
                          ibv_cq* recv_cq,
                          ReceiveInfo* recv_info) {
  // caution: work_completion and recv_info must be arrays of size max_cqes
  i32 num_entries = ibv_poll_cq(recv_cq, max_cqes, work_completion);

  if (num_entries > 0) {
    // verify completion status
    for (i32 i = 0; i < num_entries; ++i) {
      lib_assert(work_completion[i].status == IBV_WC_SUCCESS,
                 "Receive request failed");
      lib_debug("Receive request completed");

      if (recv_info && work_completion[i].opcode == IBV_WC_RECV) {
        recv_info[i].mr =
          reinterpret_cast<MemoryRegion*>(work_completion[i].wr_id);
        recv_info[i].bytes_written = work_completion[i].byte_len;
      }
    }

  } else if (num_entries < 0) {
    lib_failure("Cannot poll receive completion queue");
  }

  return num_entries;
}

i32 Context::poll_recv_cq(ibv_wc* work_completion,
                          const i32 max_cqes,
                          ReceiveInfo* recv_info) {
  lib_assert(max_cqes <= config_.max_recv_queue_wr,
             "expected number of WCs exceeds number of max WRs");

  return poll_recv_cq(work_completion, max_cqes, receive_cq_, recv_info);
}

ReceiveInfo Context::receive() {
  ibv_wc work_completion{};
  ReceiveInfo recv_info{};
  i32 num_entries;

  do {
    num_entries = poll_recv_cq(&work_completion, 1, &recv_info);
  } while (num_entries == 0);

  return recv_info;
}

// receive exactly n completion events
void Context::receive(i32 n) {
  vec<ibv_wc> work_completions(n);
  i32 num_entries = 0;

  do {
    num_entries += poll_recv_cq(work_completions.data(), n);
  } while (num_entries < n);
}

// static function
i32 Context::poll_send_cq(ibv_wc* work_completion,
                          const i32 max_cqes,
                          ibv_cq* send_cq,
                          const func<void(u64)>& id_handler) {
  // caution: work_completion must be an array of size max_cqes
  i32 num_entries = ibv_poll_cq(send_cq, max_cqes, work_completion);

  if (num_entries > 0) {
    // verify completion status
    for (i32 i = 0; i < num_entries; ++i) {
      lib_assert(work_completion[i].status == IBV_WC_SUCCESS,
                 "Send request failed");

      id_handler(work_completion[i].wr_id);
    }
    lib_debug("Send request completed");

  } else if (num_entries < 0) {
    lib_failure("Cannot poll completion queue");
  }

  return num_entries;
}

// static function
i32 Context::poll_send_cq(ibv_wc* work_completion,
                          const i32 max_cqes,
                          ibv_cq* send_cq) {
  return poll_send_cq(work_completion, max_cqes, send_cq, [](u64) {});
}

i32 Context::poll_send_cq(ibv_wc* work_completion, const i32 max_cqes) {
  lib_assert(max_cqes <= config_.max_send_queue_wr,
             "expected number of WCs exceeds number of max WRs");

  return poll_send_cq(work_completion, max_cqes, send_cq_);
}

i32 Context::poll_send_cq_until_completion() {
  ibv_wc work_completion{};
  i32 num_entries;

  do {
    num_entries = poll_send_cq(&work_completion, 1);
  } while (num_entries == 0);

  return num_entries;
}

// poll completion until we get exactly n completion events
void Context::poll_send_cq_until_completion(i32 n) {
  vec<ibv_wc> work_completions(n);
  i32 num_entries = 0;

  do {
    num_entries += poll_send_cq(work_completions.data(), n);
  } while (num_entries < n);
}
