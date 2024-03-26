#ifndef RDMA_LIBRARY_MEMORY_REGION_HH
#define RDMA_LIBRARY_MEMORY_REGION_HH

#include <infiniband/verbs.h>

#include "context.hh"
#include "types.hh"

struct MemoryRegionToken {
  u64 address;
  u32 lkey;
  u32 rkey;
};

// must be on the heap s.t. the address does not change after vector movements
using MRT = u_ptr<MemoryRegionToken>;
using MemoryRegionTokens = vec<MRT>;

// forward declaration
class Context;

class MemoryRegion {
protected:
  MemoryRegion(Context& context,
               void* data,
               size_t size_in_bytes,
               bool remote_access);

public:
  MemoryRegion(Context& context, void* data, size_t size_in_bytes);
  explicit MemoryRegion(Context& context);

  ~MemoryRegion();
  MemoryRegion(const MemoryRegion&) = delete;
  MemoryRegion& operator=(const MemoryRegion&) = delete;

  void register_memory(void* data, size_t size_in_bytes, bool remote_access);
  MemoryRegionToken createToken() const;

  u64 get_address() const { return reinterpret_cast<u64>(data_); }
  size_t get_size_in_bytes() const { return size_in_bytes_; }
  u32 get_lkey() const { return memory_region_->lkey; }
  u32 get_rkey() const { return memory_region_->rkey; }

private:
  Context& context_;
  void* data_{nullptr};
  size_t size_in_bytes_{0};
  ibv_mr* memory_region_{nullptr};
  bool is_registered_{false};
};

class LocalMemoryRegion : public MemoryRegion {
public:
  LocalMemoryRegion(Context& context, void* data, size_t size_in_bytes);
};

// must be on the heap s.t. the address does not change after vector movements
using LocalMemoryRegions = vec<u_ptr<LocalMemoryRegion>>;
using MemoryRegions = vec<u_ptr<MemoryRegion>>;

#endif  // RDMA_LIBRARY_MEMORY_REGION_HH
