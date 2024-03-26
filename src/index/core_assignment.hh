#ifndef INDEX_CORE_ASSIGNMENT_HH
#define INDEX_CORE_ASSIGNMENT_HH

#include <library/thread.hh>
#include <numeric>

// Specifically designed for our machines
// Please adjust accordingly
// Note that our NIC is attached to NUMA node 1
//
// w/out hyper-threading:
// NUMA node0 CPU(s):    0-7
// NUMA node1 CPU(s):    8-15
//
// w/ hyper-threading
// NUMA node0 CPU(s):   0-7,16-23
// NUMA node1 CPU(s):   8-15,24-31
//
// Strict policy: pin threads in the following order: 8-15, 0-7, 24-31, 16-23
// Interleaved policy: 8,0,9,1,...,24,16,25,17,...

enum AssignmentPolicy { interleaved, strict };

template <enum AssignmentPolicy>
class CoreAssignment {
public:
  CoreAssignment() : cores_(num_cores_) {
    print_hardware_info();
    set_core_sequence();
  }

  u32 get_available_core() { return cores_[assigned_cores_++ % cores_.size()]; }

  bool hyperthreading_enabled() const {
    return num_cores_ == physical_cores_per_socket_ * num_sockets_ * 2;
  }

private:
  inline void set_core_sequence();
  void print_hardware_info() const;

private:
  const u32 num_cores_ = std::thread::hardware_concurrency();
  const u32 num_sockets_ = 2;
  const u32 physical_cores_per_socket_ = num_cores_ > 16
                                           ? num_cores_ / (2 * num_sockets_)
                                           : num_cores_ / num_sockets_;
  u32 assigned_cores_{0};
  vec<u32> cores_;
};

#include "core_assignment_impl.hh"

#endif  // INDEX_CORE_ASSIGNMENT_HH
