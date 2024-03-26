#ifndef RDMA_LIBRARY_SPAN_HH
#define RDMA_LIBRARY_SPAN_HH

#include "types.hh"

template <typename T>
class span {
public:
  span(T* ptr, size_t len) : ptr_{ptr}, len_{len} {}

  T& operator[](u32 i) { return *(ptr_ + i); }
  T& operator[](u32 i) const { return *(ptr_ + i); }
  size_t size() const { return len_; }
  T* begin() { return ptr_; }
  T* end() { return ptr_ + len_; }

private:
  T* ptr_;
  size_t len_;
};

#endif  // RDMA_LIBRARY_SPAN_HH
