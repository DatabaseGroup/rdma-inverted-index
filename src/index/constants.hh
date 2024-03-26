#ifndef INDEX_CONSTANTS_HH
#define INDEX_CONSTANTS_HH

#include <library/types.hh>

namespace inv_index {
constexpr static u64 COMPUTE_NODE_MAX_MEMORY = 10ul * 1073741824ul;  // 10 GB
constexpr static u32 MAX_QPS = 4;  // max number of QPs per compute node

namespace document_based {
constexpr static u32 NUM_READ_BUFFERS = 2;
constexpr static bool BATCH_READ_REQUESTS = true;
}  // namespace document_based

namespace block_based {
constexpr static u32 READ_BUFFER_LENGTH = 32;  // number of max query terms
constexpr static u32 READ_BUFFER_DEPTH = 2;  // available blocks per query term
constexpr static u32 CACHE_LINE_SIZE = 64;
constexpr static u32 CACHE_LINE_ITEMS = CACHE_LINE_SIZE / sizeof(u32);
constexpr static u32 DYNAMIC_FOOTER_SIZE = 16;
constexpr static u32 FREELIST_PARTITIONS = 16;
}  // namespace block_based

}  // namespace inv_index

#endif  // INDEX_CONSTANTS_HH
