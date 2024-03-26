#ifndef RDMA_LIBRARY_UTILS_HH
#define RDMA_LIBRARY_UTILS_HH

#include <chrono>
#include <iostream>

#include "types.hh"

using Timepoint = std::chrono::time_point<std::chrono::high_resolution_clock>;
using ToSeconds = std::chrono::duration<f64, std::chrono::seconds::period>;
using ToMicroSeconds =
  std::chrono::duration<f64, std::chrono::microseconds::period>;

// why using macros? for std::string&& (rvalues) always _M_dispose() is called,
// even if the body is empty; using const char* also does not help because we
// cannot concatenate strings then
#define lib_assert(cond, msg)        \
  do {                               \
    if (!(cond)) {                   \
      std::cerr << msg << std::endl; \
      std::exit(EXIT_FAILURE);       \
    }                                \
  } while (0)

#ifdef LIB_DEBUG
#define lib_debug(msg)             \
  do {                             \
    std::cerr << msg << std::endl; \
  } while (0)
#else
#define lib_debug(msg) \
  do {                 \
  } while (0)
#endif

void lib_failure(const str&& message);
str get_ip(const str& node_name);

template <typename T>
void touch_memory(u_ptr<T[]>& buffer, size_t buffer_len) {
  for (size_t i = 0; i < buffer_len; ++i) {
    buffer[i] = 0;
  }
}

template <typename F, typename T>
T punning(F from) {
  lib_assert(sizeof(F) == sizeof(T), "Cannot pun types of different sizes");
  return *reinterpret_cast<T*>(&from);
}

inline u64 encode_64bit(u64 a, u64 b) { return (a << 32) | b; }

inline std::pair<u32, u32> decode_64bit(u64 word) {
  u32 a = word >> 32;
  u32 b = (word << 32) >> 32;

  return {a, b};
}

template <typename T>
void ignore_unused_parameter(const T&) {}

void print_status(str&& status);

#endif  // RDMA_LIBRARY_UTILS_HH
