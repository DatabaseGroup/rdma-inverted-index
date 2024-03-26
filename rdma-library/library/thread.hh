#ifndef RDMA_LIBRARY_THREAD_HH
#define RDMA_LIBRARY_THREAD_HH

#include <sched.h>

#include <atomic>
#include <mutex>
#include <thread>

#include "types.hh"
#include "utils.hh"

class Thread {
public:
  explicit Thread(u32 id) : thread_id_(id){};

  template <typename... Args>
  void start(Args&&... args) {
    t_ptr_ =
      std::make_unique<std::thread>(std::forward<Args>(args)..., thread_id_);
  }

  void join() const { t_ptr_->join(); }
  void set_done() { done_ = true; }
  inline bool is_done() const { return done_; }
  u32 get_id() const { return thread_id_; }

#ifdef __unix__
  void set_affinity(u32 core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    lib_assert(pthread_setaffinity_np(
                 t_ptr_->native_handle(), sizeof(cpu_set_t), &cpuset) == 0,
               "cannot pin thread " + std::to_string(thread_id_));
  }
#else
  void set_affinity(u32) {}
#endif

private:
  const u32 thread_id_;
  u_ptr<std::thread> t_ptr_;

  // we need an atomic (or volatile) here to prevent the visibility problem,
  // i.e., updating done_ (e.g., with set_done()) from thread t1 becomes not
  // visible to the local cache of t2 that is reading done_ in a closed loop
  std::atomic<bool> done_{false};
};

#ifdef __unix__
inline void pin_main_thread(u32 core_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  lib_assert(sched_setaffinity(0, sizeof(cpuset), &cpuset) == 0,
             "cannot pin main thread");
}
#else
inline void pin_main_thread(u32) {}
#endif

#endif  // RDMA_LIBRARY_THREAD_HH
