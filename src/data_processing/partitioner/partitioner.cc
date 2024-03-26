#include <boost/program_options.hpp>
#include <set>

#include "partitioning_strategies.hh"

int main(int argc, char** argv) {
  str input_file, output_path, strategy, query_file;
  u32 num_nodes, block_size;
  bool accessed_only;
  bool updates;

  const auto exit_message = [&]() {
    std::cerr << "Try " << argv[0] << " --help" << std::endl;
    std::exit(EXIT_FAILURE);
  };

  try {
    namespace po = boost::program_options;
    po::options_description desc{"Allowed options"};
    po::variables_map vm;

    desc.add_options()("help,h", "Show help message")(
      "input-file,i", po::value<str>(&input_file), "binary input file")(
      "output-path,o", po::value<str>(&output_path), "output path")(
      "strategy,s",
      po::value<str>(&strategy),
      "partitioning strategy: can be either \"term\", \"document\", or "
      "\"block\"")("block-size,b",
                   po::value<u32>(&block_size)->default_value(2048),
                   "block size in bytes (block-based partitioning only)")(
      "nodes,n", po::value<u32>(&num_nodes), "number of memory nodes")(
      "query-file,q",
      po::value<str>(&query_file),
      "only used when option -a is used")(
      "accessed-only,a",
      po::bool_switch(&accessed_only)->default_value(false),
      "partition only lists that are accessed (given in the query file)")(
      "updates,u",
      po::bool_switch(&updates)->default_value(false),
      "include meta data into blocks for updates (crc, tags, etc.)");

    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
      std::cerr << desc << std::endl;
      std::exit(EXIT_FAILURE);
    }

    po::notify(vm);

    if (input_file.empty() || output_path.empty() || strategy.empty() ||
        num_nodes == 0) {
      std::cerr << "[ERROR]: input file, output path, strategy, and number of "
                   "nodes must be given"
                << std::endl;
      exit_message();
    }

    if (accessed_only && query_file.empty()) {
      std::cerr << "[ERROR]: query file must be given if option -a is enabled"
                << std::endl;
      exit_message();
    }

    if (strategy != str("term") && strategy != str("document") &&
        strategy != str("block")) {
      std::cerr << "[ERROR]: partitioning strategy must be either \"term\", "
                   "\"document\", or \"block\""
                << std::endl;
      exit_message();
    }

    if (strategy == str("block") && block_size < 12) {
      std::cerr << "[ERROR]: block size must be at least 12 bytes" << std::endl;
      exit_message();
    }

  } catch (const std::exception& e) {
    std::cerr << "[ERROR]: " << e.what() << std::endl;
    exit_message();
  }

  std::cerr << "binary input file: " << input_file << std::endl;
  std::cerr << "output path: " << output_path << std::endl;
  std::cerr << "strategy: " << strategy << std::endl;
  std::cerr << "num memory nodes: " << num_nodes << std::endl;

  if (strategy == str("block")) {
    std::cerr << "block size: " << block_size << std::endl;
  }

  timing::Timing timing;
  Deserializer deserializer{input_file};

  vec<u32> accessed;

  if (accessed_only) {
    std::set<u32> accessed_keys;

    try {
      std::ifstream input_s(query_file, std::ios_base::in);

      if (input_s.is_open()) {
        str line, key;

        while (std::getline(input_s, line)) {
          std::stringstream line_s(line);

          // read line and separate by whitespace
          while (std::getline(line_s, key, ' ')) {
            accessed_keys.insert(std::stoi(key));
          }
        }
      } else {
        lib_failure("Cannot open '" + query_file + "'");
      }
    } catch (const std::exception& e) {
      lib_failure(e.what());
    }

    accessed.resize(accessed_keys.size());
    std::copy(accessed_keys.begin(), accessed_keys.end(), accessed.begin());
  }

  if (strategy == str("term")) {
    using Partitioner = partitioner::TermBasedPartitioner;
    partitioner::partition<Partitioner>(deserializer,
                                        num_nodes,
                                        output_path,
                                        timing,
                                        block_size,
                                        accessed,
                                        updates);

  } else if (strategy == str("document")) {
    using Partitioner = partitioner::DocumentBasedPartitioner;
    partitioner::partition<Partitioner>(deserializer,
                                        num_nodes,
                                        output_path,
                                        timing,
                                        block_size,
                                        accessed,
                                        updates);

  } else if (strategy == str("block")) {
    using Partitioner = partitioner::BlockBasedPartitioner;
    partitioner::partition<Partitioner>(deserializer,
                                        num_nodes,
                                        output_path,
                                        timing,
                                        block_size,
                                        accessed,
                                        updates);

  } else {
    std::cerr << "invalid strategy" << std::endl;
    return EXIT_FAILURE;
  }

  std::cout << timing << std::endl;

  return EXIT_SUCCESS;
}
