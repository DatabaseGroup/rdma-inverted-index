#include "utils.hh"

#include <map>

void lib_failure(const str&& message) {
  std::cerr << "[ERROR]: " << message << std::endl;
  std::exit(EXIT_FAILURE);
}

std::string get_ip(const str& node_name) {
  std::map<str, str> node_to_ip{
    {"cluster1", "10.10.5.20"},
    {"cluster2", "10.10.5.21"},
    {"cluster3", "10.10.5.22"},
    {"cluster4", "10.10.5.23"},
    {"cluster5", "10.10.5.24"},
    {"cluster6", "10.10.5.25"},
    {"cluster7", "10.10.5.26"},
    {"cluster8", "10.10.5.27"},
    {"cluster9", "10.10.5.28"},
    {"cluster10", "10.10.5.29"},
  };

  lib_assert(node_to_ip.find(node_name) != node_to_ip.end(),
             "Invalid node name: " + node_name);

  return node_to_ip[node_name];
}

void print_status(str&& status) {
  std::cerr << "[STATUS]: " << status << std::endl;
}