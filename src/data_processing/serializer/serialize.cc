#include <boost/program_options.hpp>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include <library/types.hh>

class Serializer {
private:
  std::ifstream input_s;
  std::ofstream output_s;
  size_t written_size{0};

private:
  void write_u32(u32 num) {
    if (!output_s.write(reinterpret_cast<const char*>(&num), sizeof(u32))) {
      std::cerr << "Cannot write to file" << std::endl;
      std::exit(EXIT_FAILURE);
    }
    written_size += sizeof(u32);
  }

  void write_u32_buffer(const vec<u32>& buffer) {
    if (!output_s.write(reinterpret_cast<const char*>(buffer.data()),
                        sizeof(u32) * buffer.size())) {
      std::cerr << "Cannot write to file" << std::endl;
      std::exit(EXIT_FAILURE);
    }
    written_size += sizeof(u32) * buffer.size();
  }

public:
  Serializer(const str& input_file, const str& output_file)
      : input_s(input_file, std::ios_base::in),
        output_s(output_file, std::ios::out | std::ios::binary) {}

  void serialize(bool sort = true) {
    try {
      if (input_s.is_open()) {
        str line;

        // get universe size
        if (std::getline(input_s, line)) {
          const u32 universe_size = std::stoi(line);
          std::cerr << "universe size: " << universe_size << std::endl;
          write_u32(universe_size);
        }

        // get num lists
        if (std::getline(input_s, line)) {
          const u32 num_lists = std::stoi(line);
          std::cerr << "num lists: " << num_lists << std::endl;
          write_u32(num_lists);
        }

        while (std::getline(input_s, line)) {
          char* token = std::strtok(const_cast<char*>(line.c_str()), ":");
          write_u32(std::stoi(token));
          token = std::strtok(nullptr, " ");

          vec<u32> tokens;
          while (token != nullptr) {
            tokens.push_back(std::stoi(token));
            token = std::strtok(nullptr, " ");
          }

          if (sort) {
            std::sort(tokens.begin(), tokens.end());
          }

          write_u32(tokens.size());
          write_u32_buffer(tokens);
        }
      } else {
        std::cerr << "Cannot open input file" << std::endl;
        std::exit(EXIT_SUCCESS);
      }
    } catch (const std::exception& e) {
      std::cerr << e.what() << std::endl;
      std::exit(EXIT_SUCCESS);
    }

    // super important! otherwise, the final bytes are not written
    output_s.close();
    input_s.close();

    std::cerr << "total size: " << written_size << " Bytes" << std::endl;
  }
};

int main(int argc, char** argv) {
  str input_file;
  str output_file;

  const auto exit_message = [&]() {
    std::cerr << "Try " << argv[0] << " --help" << std::endl;
    std::exit(EXIT_FAILURE);
  };

  try {
    namespace po = boost::program_options;
    po::options_description desc{"Allowed options"};
    po::variables_map vm;

    desc.add_options()("help,h", "Show help message")(
      "input-file,i", po::value<str>(&input_file), "input file")(
      "output-file,o", po::value<str>(&output_file), "output file");

    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
      std::cerr << desc << std::endl;
      std::exit(EXIT_FAILURE);
    }

    po::notify(vm);

    if (input_file.empty() || output_file.empty()) {
      std::cerr << "[ERROR]: input and output file must be given" << std::endl;
      exit_message();
    }

  } catch (const std::exception& e) {
    std::cerr << "[ERROR]: " << e.what() << std::endl;
    exit_message();
  }

  std::cerr << "input file: " << input_file << std::endl;
  std::cerr << "output file: " << output_file << std::endl;
  std::cerr << "serialize..." << std::endl;

  Serializer s{input_file, output_file};
  s.serialize();

  return EXIT_SUCCESS;
}
