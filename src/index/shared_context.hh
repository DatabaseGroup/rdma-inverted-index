#ifndef INDEX_SHARED_CONTEXT_HH
#define INDEX_SHARED_CONTEXT_HH

#include <library/connection_manager.hh>
#include <library/detached_qp.hh>

template <typename T>
struct SharedContext {
  Context context;
  vec<u_ptr<DetachedQP>> qps;  // per memory node
  u_ptr<LocalMemoryRegion> memory_region;
  vec<T*> registered_threads;

  SharedContext(Context& channel_context,
                ClientConnectionManager& cm,
                HugePage<u32>& buffer)
      : context(channel_context.get_config()) {
    qps.reserve(cm.server_qps.size());
    for (QP& server_qp : cm.server_qps) {
      auto& qp = qps.emplace_back(std::make_unique<DetachedQP>(
        context, context.get_send_cq(), context.get_receive_cq()));
      qp->connect(channel_context, context.get_lid(), server_qp);
    }

    // register full buffer
    memory_region = std::make_unique<LocalMemoryRegion>(
      context, buffer.get_full_buffer(), buffer.buffer_size);
  }

  void register_thread(T* thread) {
    registered_threads.push_back(thread);
    thread->ctx = this;
    thread->ctx_tid = registered_threads.size() - 1;
  }

  ibv_cq* get_cq() { return context.get_send_cq(); }
  u32 get_lkey() const { return memory_region->get_lkey(); }
};

#endif  // INDEX_SHARED_CONTEXT_HH
