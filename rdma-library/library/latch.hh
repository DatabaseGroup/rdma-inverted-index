#ifndef RDMA_LIBRARY_LATCH_HH
#define RDMA_LIBRARY_LATCH_HH

#include <atomic>

#include "types.hh"

class Latch {
public:
  explicit Latch(i32 num_threads) : count_{num_threads} {};
  Latch() = default;

  void init(i32 num_threads) { count_.store(num_threads); }

  void arrive_and_wait() {
    i32 current = --count_;

    while (current != 0) {
      current = count_.load();
    }
  }

private:
  std::atomic<i32> count_;
};

#endif  // RDMA_LIBRARY_LATCH_HH
