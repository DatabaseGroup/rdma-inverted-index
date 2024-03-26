#include "queue_pair.hh"

#include "utils.hh"

// delegating ctor
QueuePair::QueuePair(Context* context, bool use_shared_receive_cq)
    : QueuePair(context,
                context->get_send_cq(),
                context->get_receive_cq(),
                use_shared_receive_cq) {}

QueuePair::QueuePair(Context* context,
                     ibv_cq* send_cq,
                     ibv_cq* recv_cq,
                     bool use_shared_receive_cq)
    : context_(context),
      lid_(context->get_lid()),
      use_shared_receive_cq_(use_shared_receive_cq) {
  ibv_qp_init_attr init_attributes =
    get_qp_initial_attributes(send_cq, recv_cq);
  queue_pair_ =
    ibv_create_qp(context->get_protection_domain(), &init_attributes);
  lib_assert(queue_pair_, "Cannot create queue pair");

  transition_to_init();
}

QueuePair::~QueuePair() {
  lib_assert(ibv_destroy_qp(queue_pair_) == 0, "Cannot destroy queue pair.");
}

ibv_qp_init_attr QueuePair::get_qp_initial_attributes(ibv_cq* send_cq,
                                                      ibv_cq* recv_cq) {
  ibv_qp_init_attr attributes{};
  const i32 max_sge_elements = 1;

  // FYI: if a shared rcq is used, no normal receive request RR can be posted
  if (use_shared_receive_cq_) {
    attributes.srq = context_->get_shared_receive_cq();
  }

  attributes.send_cq = send_cq;
  attributes.recv_cq = recv_cq;
  attributes.cap.max_send_wr = context_->get_config().max_send_queue_wr;
  attributes.cap.max_send_sge = max_sge_elements;
  attributes.cap.max_recv_wr = context_->get_config().max_recv_queue_wr;
  attributes.cap.max_recv_sge = max_sge_elements;
  attributes.cap.max_inline_data = INLINE_SIZE;
  attributes.qp_type = IBV_QPT_RC;
  // if 1, all WRs will generate CQEs, if 0, only flagged WRs generate CQEs
  attributes.sq_sig_all = 0;

  return attributes;
}

// transition state of queue pair from RESET to INIT:
// basic information set, ready for posting to receive queue.
void QueuePair::transition_to_init() {
  ibv_qp_attr attributes{};

  attributes.qp_state = IBV_QPS_INIT;
  attributes.pkey_index = 0;
  attributes.port_num = context_->get_config().device_port;
  attributes.qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
                               IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE |
                               IBV_ACCESS_REMOTE_ATOMIC;
  lib_assert(ibv_modify_qp(queue_pair_,
                           &attributes,
                           IBV_QP_STATE | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS |
                             IBV_QP_PKEY_INDEX) == 0,
             "Cannot change state of queue pair to INIT");
  lib_debug("Transitioned state to INIT successfully");
}

void QueuePair::transition_to_rtr(const QPInfo& remote_buffer) {
  ibv_qp_attr attributes{};

  attributes.qp_state = IBV_QPS_RTR;
  attributes.path_mtu = IBV_MTU_4096;
  attributes.dest_qp_num = remote_buffer.qp_number;
  attributes.rq_psn = 0;
  attributes.max_dest_rd_atomic = 16;
  attributes.min_rnr_timer = 12;
  attributes.ah_attr.is_global = 0;
  attributes.ah_attr.dlid = remote_buffer.lid;
  attributes.ah_attr.sl = 0;
  attributes.ah_attr.src_path_bits = 0;
  attributes.ah_attr.port_num = context_->get_config().device_port;

  lib_assert(
    ibv_modify_qp(queue_pair_,
                  &attributes,
                  IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                    IBV_QP_RQ_PSN | IBV_QP_MIN_RNR_TIMER |
                    IBV_QP_MAX_DEST_RD_ATOMIC) == 0,
    "Cannot change state of queue pair to RTR");
  lib_debug("Transitioned state to RTR successfully");
}

void QueuePair::transition_to_rts() {
  ibv_qp_attr attributes{};

  attributes.qp_state = IBV_QPS_RTS;
  attributes.timeout = 14;
  attributes.retry_cnt = 7;
  attributes.rnr_retry = 7;
  attributes.sq_psn = 0;
  attributes.max_rd_atomic = 16;

  lib_assert(ibv_modify_qp(queue_pair_,
                           &attributes,
                           IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                             IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                             IBV_QP_MAX_QP_RD_ATOMIC) == 0,
             "Cannot change state of queue pair to RTS");
  lib_debug("Transitioned state to RTS successfully");
}

void QueuePair::post_receive(MemoryRegion& region) {
  post_receive(region, region.get_size_in_bytes());
}

void QueuePair::post_receive(MemoryRegion& region,
                             u32 size_in_bytes,
                             u64 wr_id,
                             u64 local_offset) {
  ibv_recv_wr work_request{};
  ibv_sge scatter_gather_entry{};

  // points to the RR that failed to be posted (if not successful)
  ibv_recv_wr* bad_work_request{nullptr};

  scatter_gather_entry.addr = region.get_address() + local_offset;
  scatter_gather_entry.length = size_in_bytes;
  scatter_gather_entry.lkey = region.get_lkey();

  work_request.wr_id = wr_id;
  work_request.next = nullptr;
  work_request.sg_list = &scatter_gather_entry;
  work_request.num_sge = 1;

  // post receive request to receive queue
  lib_assert(ibv_post_recv(queue_pair_, &work_request, &bad_work_request) == 0,
             "Cannot post receive request");
  lib_debug("Receive request successfully posted");
}

u32 QueuePair::receive_u32(Context& context) {
  u32 value;

  LocalMemoryRegion region{context, std::addressof(value), sizeof(u32)};
  post_receive(region);
  context.receive();

  return value;
}

void QueuePair::post_send_inlined(const void* address,
                                  u32 size_in_bytes,
                                  enum ibv_wr_opcode opcode,
                                  bool signaled,
                                  MemoryRegionToken* token,
                                  u64 remote_offset,
                                  u64 local_offset,
                                  u64 wr_id) {
  post_send(reinterpret_cast<u64>(address),
            size_in_bytes,
            0,
            opcode,
            signaled,
            true,
            token,
            remote_offset,
            local_offset,
            wr_id);
}

void QueuePair::post_send_u32(u32& value, bool signaled) {
  post_send(reinterpret_cast<u64>(std::addressof(value)),
            sizeof(u32),
            0,
            IBV_WR_SEND,
            signaled,
            true,
            nullptr,
            0,
            0,
            0);
}

void QueuePair::post_send(MemoryRegion& region,
                          enum ibv_wr_opcode opcode,
                          bool signaled,
                          MemoryRegionToken* token,
                          u64 remote_offset,
                          u64 local_offset) {
  post_send(region.get_address(),
            region.get_size_in_bytes(),
            region.get_lkey(),
            opcode,
            signaled,
            false,
            token,
            remote_offset,
            local_offset,
            0);
}

void QueuePair::post_send(MemoryRegion& region,
                          u32 size_in_bytes,
                          enum ibv_wr_opcode opcode,
                          bool signaled,
                          MemoryRegionToken* token,
                          u64 remote_offset,
                          u64 local_offset) {
  post_send(region.get_address(),
            size_in_bytes,
            region.get_lkey(),
            opcode,
            signaled,
            false,
            token,
            remote_offset,
            local_offset,
            0);
}

void QueuePair::post_send_with_id(MemoryRegion& region,
                                  u32 size_in_bytes,
                                  enum ibv_wr_opcode opcode,
                                  u64 wr_id,
                                  bool signaled,
                                  MemoryRegionToken* token,
                                  u64 remote_offset,
                                  u64 local_offset) {
  post_send(region.get_address(),
            size_in_bytes,
            region.get_lkey(),
            opcode,
            signaled,
            false,
            token,
            remote_offset,
            local_offset,
            wr_id);
}

void QueuePair::post_send(u64 address,
                          u32 size,
                          u32 lkey,
                          enum ibv_wr_opcode opcode,
                          bool signaled,
                          bool inlined,
                          MemoryRegionToken* token,
                          u64 remote_offset,
                          u64 local_offset,
                          u64 wr_id) {
  lib_assert(!inlined || size <= INLINE_SIZE, "Request cannot be inlined");
  lib_assert(size <= MESSAGE_SIZE, "Message size too large");

  ibv_send_wr work_request{};
  ibv_sge scatter_gather_entry{};

  // points to the SR that failed to be posted (if not successful)
  struct ibv_send_wr* bad_work_request;

  scatter_gather_entry.addr = address + local_offset;
  scatter_gather_entry.length = size;
  scatter_gather_entry.lkey = lkey;

  work_request.opcode = opcode;
  work_request.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
  work_request.send_flags |= inlined ? IBV_SEND_INLINE : 0;
  work_request.wr_id = wr_id;
  work_request.next = nullptr;
  work_request.sg_list = &scatter_gather_entry;
  work_request.num_sge = 1;

  if (opcode != IBV_WR_SEND) {
    lib_assert(token, "MemoryRegionToken does not exist");
    work_request.wr.rdma.remote_addr = token->address + remote_offset;
    work_request.wr.rdma.rkey = token->rkey;
  }

  // post send request to send queue
  lib_assert(ibv_post_send(queue_pair_, &work_request, &bad_work_request) == 0,
             "Cannot post send request");

  switch (opcode) {
  case IBV_WR_SEND:
    lib_debug("SEND request successfully posted");
    break;
  case IBV_WR_RDMA_READ:
    lib_debug("RDMA_READ request successfully posted");
    break;
  case IBV_WR_RDMA_WRITE:
    lib_debug("RDMA_WRITE request successfully posted");
    break;
  default:
    lib_failure("Unknown request posted");
    break;
  }
}

void QueuePair::post_CAS(MemoryRegion& local_region,
                         MemoryRegionToken* remote_token,
                         u64 remote_offset,
                         u64 compare_to,
                         u64 swap_with,
                         bool signaled,
                         u64 wr_id) {
  ibv_send_wr work_request{};
  ibv_sge sge{};

  struct ibv_send_wr* bad_work_request;

  sge.addr = local_region.get_address();
  sge.length = 8;
  sge.lkey = local_region.get_lkey();

  work_request.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  work_request.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
  work_request.wr_id = wr_id;
  work_request.next = nullptr;
  work_request.sg_list = &sge;
  work_request.num_sge = 1;

  auto& atomic = work_request.wr.atomic;
  atomic.remote_addr = remote_token->address + remote_offset;
  atomic.rkey = remote_token->rkey;

  atomic.compare_add = compare_to;
  atomic.swap = swap_with;

  lib_assert(ibv_post_send(queue_pair_, &work_request, &bad_work_request) == 0,
             "Cannot post CAS request");
}
