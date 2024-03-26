#ifndef INDEX_CONFIGURATION_HH
#define INDEX_CONFIGURATION_HH

#include <iomanip>
#include <iostream>
#include <library/configuration.hh>

namespace configuration {

class IndexConfiguration : public Configuration {
public:
  u32 num_threads{0};
  str index_dir{};  // location of the partitioned index files
  str query_file{};
  str operation{};
  bool disable_thread_pinning{};
  u32 block_size{};

  enum Operation { intersection, union_op };

public:
  IndexConfiguration(int argc, char** argv) {
    add_options();
    process_program_options(argc, argv);
    validate_program_options(argv);
    if (index_dir.back() != '/') {
      index_dir.append("/");
    }
    operator<<(std::cerr, *this);
  }

  Operation get_operation() const {
    return operation == str("intersection") ? Operation::intersection
                                            : Operation::union_op;
  }

private:
  void add_options() {
    desc.add_options()("index-dir,d",
                       po::value<str>(&index_dir),
                       "Location of the partitioned index files.")(
      "query-file,q",
      po::value<str>(&query_file),
      "Input file containing queries for the index.")(
      "threads,t",
      po::value<u32>(&num_threads),
      "Number of threads per compute node")(
      "operation,o",
      po::value<str>(&operation),
      R"(Operation performed on lists: either "intersection" or "union".)")(
      "disable-thread-pinning,p",
      po::bool_switch(&disable_thread_pinning)->default_value(false),
      "Disables pinning compute threads to physical cores if set.")(
      "block-size,b",
      po::value<u32>(&block_size)->default_value(1024),
      "Block size in bytes (only used by [dynamic_]block_index).");
  }

  void validate_program_options(char** argv) {
    if (is_initiator) {
      if (index_dir.empty()) {
        std::cerr << "[ERROR]: Directory of partitioned index files must be "
                     "passed to the initiator"
                  << std::endl;
        exit_with_help_message(argv);
      }

      if (query_file.empty()) {
        std::cerr << "[ERROR]: Input query file must be passed to the initiator"
                  << std::endl;
        exit_with_help_message(argv);
      }

      if (num_threads == 0) {
        std::cerr
          << "[ERROR]: The number of threads must be passed to the initiator"
          << std::endl;
        exit_with_help_message(argv);
      }

      if (operation.empty()) {
        std::cerr << "[ERROR]: Operation must be specified" << std::endl;
        exit_with_help_message(argv);

      } else if (operation != str("intersection") &&
                 operation != str("union")) {
        std::cerr << "[ERROR]: Invalid operation" << std::endl;
        exit_with_help_message(argv);
      }

      if (block_size < 12) {
        std::cerr << "[ERROR]: Block size must be minimum 12 bytes"
                  << std::endl;
        exit_with_help_message(argv);
      }
    }
  }

public:
  friend std::ostream& operator<<(std::ostream& os,
                                  const IndexConfiguration& config) {
    const int32_t width = 30;
    const int32_t max_width = width * 2;
    const char filler = '=';

    os << static_cast<const Configuration&>(config);
    if (config.is_initiator) {
      os << std::left << std::setfill(' ');
      os << std::setw(width) << "index directory: " << config.index_dir
         << std::endl;
      os << std::setw(width) << "query file: " << config.query_file
         << std::endl;
      os << std::setw(width) << "operation: " << config.operation << std::endl;
      os << std::setw(width) << "number of threads: " << config.num_threads
         << std::endl;
      os << std::setw(width) << "threads pinned: "
         << (config.disable_thread_pinning ? "false" : "true") << std::endl;
      os << std::setw(width) << "block size: " << config.block_size
         << std::endl;
      os << std::setfill(filler) << std::setw(max_width) << "" << std::endl;
    }
    return os;
  }
};

}  // namespace configuration

#endif  // INDEX_CONFIGURATION_HH
