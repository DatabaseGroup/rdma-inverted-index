#ifndef INDEX_OPERATIONS_HH
#define INDEX_OPERATIONS_HH

#include <library/types.hh>
#include <queue>

namespace operations {
using ListIterator = u32*;

void compute_intersection(const func<void(u32)>& result_handler,
                          vec<ListIterator>& begin_iterators,
                          vec<ListIterator>& end_iterators) {
  if (begin_iterators.front() == end_iterators.front()) {
    return;
  }

  const u32 num_lists = begin_iterators.size();
  u32 curr_value, count;
  u32 iter_idx = 1;

  auto reset_value = [&](ListIterator& iter) {
    curr_value = *iter;
    count = 1;
    ++iter;
  };

  // handle special case of single list
  if (num_lists == 1) {
    for (auto iter = begin_iterators.front(); iter != end_iterators.front();
         ++iter) {
      result_handler(*iter);
    }

    return;
  }

  reset_value(begin_iterators.front());

  while (true) {
    auto& iter = begin_iterators[iter_idx];
    auto& end_iter = end_iterators[iter_idx];

    while (iter != end_iter && *iter < curr_value) {
      ++iter;
    }

    if (iter == end_iter) {
      break;
    }

    if (*iter == curr_value) {
      ++count;
      ++iter;

      if (count == num_lists) {
        result_handler(curr_value);

        if (iter == end_iter) {
          break;
        }

        reset_value(iter);
      }

    } else if (*iter > curr_value) {
      reset_value(iter);
    }

    iter_idx = (iter_idx + 1) % num_lists;  // cycle
  }
}

void compute_union(const func<void(u32)>& result_handler,
                   vec<ListIterator>& begin_iterators,
                   vec<ListIterator>& end_iterators) {
  using HeapEntry = std::pair<u32, u32>;  // (list entry, list index)
  struct HeapCompare {
    bool operator()(HeapEntry& lhs, HeapEntry& rhs) {
      return lhs.first > rhs.first;
    }
  };

  if (begin_iterators.empty()) {
    return;
  }

  std::priority_queue<HeapEntry, vec<HeapEntry>, HeapCompare> min_heap;
  u32 last_element = -1;

  // initialize min heap
  for (u32 i = 0; i < begin_iterators.size(); ++i) {
    ListIterator head = begin_iterators[i];

    if (head != end_iterators[i]) {
      min_heap.push({*head, i});
      ++head;
    }
  }

  while (!min_heap.empty()) {
    auto [min_value, idx] = min_heap.top();
    u32*& min_node = begin_iterators[idx];
    min_heap.pop();

    // do not insert duplicates
    if (last_element != min_value) {
      result_handler(min_value);
      last_element = min_value;
    }

    if (++min_node != end_iterators[idx]) {
      min_heap.push({*min_node, idx});
    }
  }
}

}  // namespace operations

#endif  // INDEX_OPERATIONS_HH