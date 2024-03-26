#ifndef INDEX_QUERY_HH
#define INDEX_QUERY_HH

#include <fstream>
#include <iostream>
#include <library/types.hh>

enum class QueryType : u32 { READ, INSERT, DELETE };

namespace query {
using Key = u32;
using Keys = vec<u32>;

struct Query {
  u32 id;
  QueryType type;
  u32 update_id;
  Keys keys;

  Query(u32 id, QueryType type, u32 update_id, Keys&& keys)
      : id(id), type(type), update_id(update_id), keys(std::move(keys)) {}
  size_t size() const { return keys.size(); }
  size_t raw_size() const { return sizeof(id) + sizeof(Key) * size(); }

  friend std::ostream& operator<<(std::ostream& os, const Query& query) {
    str t = query.type == QueryType::READ
              ? " [read]"
              : (query.type == QueryType::INSERT
                   ? " [insert: " + std::to_string(query.update_id) + "]"
                   : " [delete: " + std::to_string(query.update_id) + "]");
    os << query.id << t << " (len=" << query.size() << "): [";
    for (const auto& key : query.keys) {
      os << key << " ";
    }
    os << "\b]";

    return os;
  }
};
using Queries = vec<Query>;

struct QueryStatistics {
  u64 num_reads{0};
  u64 num_inserts{0};
  u64 num_deletes{0};
  u32 universe_size{0};
};

QueryStatistics read_queries(const str& filename, Queries& queries) {
  print_status("read queries");
  QueryStatistics stats;
  u32& universe_size = stats.universe_size;

  size_t total_size = 0;
  u32 query_id = 0;

  try {
    std::ifstream input_s(filename, std::ios_base::in);

    if (input_s.is_open()) {
      str line;

      while (std::getline(input_s, line)) {
        QueryType q_type{};
        Keys keys;

        char* token = std::strtok(const_cast<char*>(line.c_str()), ":");
        switch (*token) {
        case 'r':
          q_type = QueryType::READ;
          stats.num_reads++;
          break;
        case 'i':
          q_type = QueryType::INSERT;
          stats.num_inserts++;
          break;
        case 'd':
          q_type = QueryType::DELETE;
          stats.num_deletes++;
          break;

        default:
          lib_failure("invalid query type: " + std::to_string(*token));
        }

        u32 update_id = 0;
        if (q_type != QueryType::READ) {
          token = std::strtok(nullptr, " ");
          lib_assert(token != nullptr, "cannot parse query");
          update_id = std::stoi(token);
        }

        token = std::strtok(nullptr, " ");
        while (token != nullptr) {
          keys.push_back(std::stoi(token));
          token = std::strtok(nullptr, " ");
        }

        // assume ordered keys
        Key max_key = keys.back();
        universe_size = (max_key > universe_size) ? max_key : universe_size;
        queries.emplace_back(query_id++, q_type, update_id, std::move(keys));
        total_size += queries.back().raw_size();
      }
    } else {
      lib_failure("Cannot open '" + filename + "'");
    }
  } catch (const std::exception& e) {
    lib_failure(e.what());
  }

  queries.shrink_to_fit();
  std::cerr << "size of queries: " << total_size << " Bytes" << std::endl;

  return stats;
}
}  // namespace query

#endif  // INDEX_QUERY_HH
