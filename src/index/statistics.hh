#ifndef INDEX_STATISTICS_HH
#define INDEX_STATISTICS_HH

#include <library/types.hh>
#include <library/utils.hh>
#include <ostream>

#include "extern/nlohmann/json.hh"

namespace statistics {

template <bool dynamic = false>
class Statistics {
public:
  using json = nlohmann::json;

  template <typename T>
  struct CountItem {
    str name;
    T count{0};

    explicit CountItem(str&& name) : name(name) {}
    void add(T v) { count += v; }
    void inc() { ++count; }
  };

public:
  Statistics() {
    items_ = {std::ref(universe_size),
              std::ref(num_queries),
              std::ref(num_result),
              std::ref(total_index_size),
              std::ref(total_index_buffer_size),
              std::ref(rdma_reads_in_bytes),
              std::ref(allocated_read_buffers_size),
              std::ref(catalog_size),
              std::ref(num_read_queries),
              std::ref(num_insert_queries)};

    if constexpr (dynamic) {
      items_.insert(items_.end(),
                    {std::ref(remote_allocations),
                     std::ref(remote_deallocations),
                     std::ref(block_repeated_reads),
                     std::ref(locking_failed),
                     std::ref(read_failed),
                     std::ref(wait_for_write),
                     std::ref(list_repeated_reads)});
    }
  }

  void set(const str& key, u64 v) { stats_[key] = v; }

  template <typename T>
  T get(const str& key) {
    return stats_[key].get<T>();
  }

  str serialize() const { return stats_.dump(); }

  void merge_json(const json& other) {
    for (auto& [key, value] : other.items()) {
      if (value.is_number()) {
        set(key, get<u64>(key) + value.get<u64>());
      } else {
        lib_failure("invalid type in statistics object");
      }
    }
  }

  void merge_stats(const json& other) {
    for (auto& [key, value] : other.items()) {
      if (value.is_number()) {
        // find item
        for (auto& item : items_) {
          if (item.get().name == key) {
            item.get().add(value);
            break;
          }
        }
      } else {
        lib_failure("invalid type in statistics object");
      }
    }
  }

  void add_timings(const json& timings) { stats_["timings"] = timings; }

  template <typename T>
  void add_meta_stat(const str&& key, T v) {
    stats_["meta"][key] = v;
  }

  template <typename T>
  void add_meta_stats(T pair) {
    add_meta_stat(pair.first, pair.second);
  }

  template <typename T, typename... Args>
  void add_meta_stats(T pair, Args... args) {
    add_meta_stat(pair.first, pair.second);
    add_meta_stats(args...);
  }

  void output_all(const json& timings) {
    convert_to_json();
    add_timings(timings);
    std::cerr << std::endl << "statistics:" << std::endl;
    std::cout << *this << std::endl;
  }

  template <typename T>
  void add_static_stat(const str&& key, T v) {
    stats_[key] = v;
  }

  friend std::ostream& operator<<(std::ostream& os, Statistics& s) {
    return os << s.stats_.dump(2);
  }

  [[maybe_unused]] json& convert_to_json() {
    for (auto& item : items_) {
      stats_[item.get().name] = item.get().count;
    }

    return stats_;
  }

public:
  // tracked in items container
  CountItem<u64> universe_size{"universe_size"};
  CountItem<u64> num_queries{"num_queries"};
  CountItem<u64> num_result{"num_result"};
  CountItem<u64> total_index_size{"total_initial_index_size"};
  CountItem<u64> total_index_buffer_size{"total_index_buffer_size"};
  CountItem<u64> rdma_reads_in_bytes{"rdma_reads_in_bytes"};
  CountItem<u64> allocated_read_buffers_size{"allocated_read_buffers_size"};
  CountItem<u64> catalog_size{"catalog_size"};

  CountItem<u64> remote_allocations{"remote_allocations"};
  CountItem<u64> remote_deallocations{"remote_deallocations"};
  CountItem<u64> block_repeated_reads{"block_repeated_reads"};
  CountItem<u64> list_repeated_reads{"list_repeated_reads"};

  CountItem<u64> num_read_queries{"num_read_queries"};
  CountItem<u64> num_insert_queries{"num_insert_queries"};

  CountItem<u64> locking_failed{"locking_failed"};
  CountItem<u64> read_failed{"read_failed"};
  CountItem<u64> wait_for_write{"wait_for_write"};

protected:
  vec<std::reference_wrapper<CountItem<u64>>> items_;

private:
  json stats_;
};

}  // namespace statistics

#endif  // INDEX_STATISTICS_HH
