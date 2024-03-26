#ifndef INDEX_BLOCK_BASED_WR_IDS_HH
#define INDEX_BLOCK_BASED_WR_IDS_HH

#include <library/types.hh>

namespace inv_index::block_based {

// TODO: move other constants (dynamic WR_IDs)

// [ thread offset in shared context | buffer column | buffer row ]
// [ 4 bits ------------------------ | 30 bits ----- | 30 bits -- ]
static u64 encode_wr_id(u64 ctx_tid, u64 col, u64 row) {
  return ((ctx_tid << 60) | col << 30) | row;
}

static std::tuple<u32, u32, u32> decode_wr_id(u64 word) {
  u32 ctx_tid = word >> 60;
  u32 col = (word << 4) >> 34;
  u32 row = (word << 34) >> 34;

  return {ctx_tid, col, row};
}

}  // namespace inv_index::block_based

#endif  // INDEX_BLOCK_BASED_WR_IDS_HH
