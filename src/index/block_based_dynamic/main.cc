#include "index/compute_node.hh"
#include "index/configuration.hh"
#include "index/memory_node.hh"
#include "query_handler.hh"

int main(int argc, char** argv) {
  configuration::IndexConfiguration config{argc, argv};
  using namespace inv_index;

  if (config.is_server) {
    constexpr bool dynamic = true;
    MemoryNode<dynamic> memory_node{config};

  } else {
    using QueryHandler = block_based::dynamic::DynamicBlockBasedQueryHandler;
    ComputeNode<QueryHandler> compute_node{config};
  }

  return EXIT_SUCCESS;
}
