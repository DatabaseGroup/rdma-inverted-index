#ifndef RDMA_LIBRARY_TYPES_HH
#define RDMA_LIBRARY_TYPES_HH

#include <oneapi/tbb/concurrent_queue.h>
#include <oneapi/tbb/concurrent_vector.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "extern/concurrentqueue.hh"

using i16 = int16_t;
using u16 = uint16_t;

using i32 = int32_t;
using u32 = uint32_t;

using i64 = int64_t;
using u64 = uint64_t;

using f32 = float;
using f64 = double;

using byte = uint8_t;

using str = std::string;
using size_t = std::size_t;

using intptr_t = std::intptr_t;

template <typename T>
using u_ptr = std::unique_ptr<T>;

template <typename T>
using s_ptr = std::shared_ptr<T>;

template <typename T>
using func = std::function<T>;

template <typename T>
using vec = std::vector<T>;

template <typename T>
using concurrent_vec = oneapi::tbb::concurrent_vector<T>;

template <typename T>
using concurrent_queue = moodycamel::ConcurrentQueue<T>;
// using concurrent_queue = oneapi::tbb::concurrent_queue<T>;

#endif  // RDMA_LIBRARY_TYPES_HH
