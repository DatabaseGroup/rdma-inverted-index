#ifndef INDEX_CORE_ASSIGNMENT_IMPL_HH
#define INDEX_CORE_ASSIGNMENT_IMPL_HH

template <>
inline void CoreAssignment<AssignmentPolicy::strict>::set_core_sequence() {
  std::cerr << "strict policy" << std::endl;

  std::iota(cores_.begin() + 0 * physical_cores_per_socket_,
            cores_.begin() + 1 * physical_cores_per_socket_,
            1 * physical_cores_per_socket_);
  std::iota(cores_.begin() + 1 * physical_cores_per_socket_,
            cores_.begin() + 2 * physical_cores_per_socket_,
            0 * physical_cores_per_socket_);

  // with hyper-threading
  if (hyperthreading_enabled()) {
    std::iota(cores_.begin() + 2 * physical_cores_per_socket_,
              cores_.begin() + 3 * physical_cores_per_socket_,
              3 * physical_cores_per_socket_);
    std::iota(cores_.begin() + 3 * physical_cores_per_socket_,
              cores_.begin() + 4 * physical_cores_per_socket_,
              2 * physical_cores_per_socket_);
  }
}

template <>
inline void CoreAssignment<AssignmentPolicy::interleaved>::set_core_sequence() {
  std::cerr << "interleaved policy" << std::endl;

  vec<u32> node1_cores(num_cores_ / num_sockets_);
  vec<u32> node0_cores(num_cores_ / num_sockets_);

  std::iota(node1_cores.begin(),
            node1_cores.begin() + physical_cores_per_socket_,
            1 * physical_cores_per_socket_);
  std::iota(node0_cores.begin(),
            node0_cores.begin() + physical_cores_per_socket_,
            0 * physical_cores_per_socket_);

  // with hyper-threading
  if (hyperthreading_enabled()) {
    std::iota(node1_cores.begin() + physical_cores_per_socket_,
              node1_cores.end(),
              3 * physical_cores_per_socket_);
    std::iota(node0_cores.begin() + physical_cores_per_socket_,
              node0_cores.end(),
              2 * physical_cores_per_socket_);
  }

  // set interleaved (interchangeably pick from node 1 and node 0)
  for (u32 i = 0, j = 0; i < num_cores_ / num_sockets_; ++i) {
    cores_[j++] = node1_cores[i];
    cores_[j++] = node0_cores[i];
  }
}

template <enum AssignmentPolicy P>
void CoreAssignment<P>::print_hardware_info() const {
  std::cerr << "num_cores: " << num_cores_ << std::endl;
  std::cerr << "physical cores per socket: " << physical_cores_per_socket_
            << std::endl;
  std::cerr << "hyperthreading "
            << (hyperthreading_enabled() ? "enabled" : "disabled") << std::endl;
}

#endif  // INDEX_CORE_ASSIGNMENT_IMPL_HH