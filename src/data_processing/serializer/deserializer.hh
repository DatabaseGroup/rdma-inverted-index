#ifndef DATA_PROCESSING_SERIALIZER_DESERIALIZER_HH
#define DATA_PROCESSING_SERIALIZER_DESERIALIZER_HH

#include <fstream>
#include <iostream>
#include <library/utils.hh>

class Deserializer {
public:
  explicit Deserializer(const str& binary_file)
      : binary_s_(binary_file, std::ios::in | std::ios::binary) {
    lib_assert(binary_s_.good(), "file \"" + binary_file + "\" does not exist");

    binary_s_.seekg(0, std::ios::end);
    file_size_ = binary_s_.tellg();
    binary_s_.seekg(0, std::ios::beg);
  }

  u32 read_u32() {
    u32 integer;

    if (!binary_s_.read(reinterpret_cast<char*>(&integer), sizeof(u32))) {
      std::cerr << "Cannot read file" << std::endl;
      std::exit(EXIT_FAILURE);
    }
    return integer;
  }

  bool bytes_left() { return binary_s_.tellg() < file_size_; }
  void jump(u32 num_bytes) { binary_s_.ignore(num_bytes); }

private:
  std::ifstream binary_s_;
  i64 file_size_;
};

#endif  // DATA_PROCESSING_SERIALIZER_DESERIALIZER_HH
