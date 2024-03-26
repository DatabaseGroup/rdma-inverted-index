#include "memory_region.hh"

#include "utils.hh"

MemoryRegion::MemoryRegion(Context& context,
                           void* data,
                           const size_t size_in_bytes,
                           bool remote_access)
    : context_(context) {
  register_memory(data, size_in_bytes, remote_access);
}

MemoryRegion::MemoryRegion(Context& context) : context_(context) {}

void MemoryRegion::register_memory(void* data,
                                   const size_t size_in_bytes,
                                   bool remote_access) {
  int access = (remote_access)
                 ? IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
                     IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_LOCAL_WRITE
                 : IBV_ACCESS_LOCAL_WRITE;
  memory_region_ =
    ibv_reg_mr(context_.get_protection_domain(), data, size_in_bytes, access);

  lib_assert(memory_region_, "Cannot register memory region");

  data_ = data;
  size_in_bytes_ = size_in_bytes;
  is_registered_ = true;
}

MemoryRegion::MemoryRegion(Context& context,
                           void* data,
                           const size_t size_in_bytes)
    : MemoryRegion(context, data, size_in_bytes, true) {}

LocalMemoryRegion::LocalMemoryRegion(Context& context,
                                     void* data,
                                     const size_t size_in_bytes)
    : MemoryRegion(context, data, size_in_bytes, false) {}

MemoryRegion::~MemoryRegion() {
  if (is_registered_) {
    lib_assert(ibv_dereg_mr(memory_region_) == 0,
               "Cannot deregister memory region.");
  }
}

MemoryRegionToken MemoryRegion::createToken() const {
  return MemoryRegionToken{get_address(), get_lkey(), get_rkey()};
}
