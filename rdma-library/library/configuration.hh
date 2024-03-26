#ifndef RDMA_LIBRARY_CONFIGURATION_HH
#define RDMA_LIBRARY_CONFIGURATION_HH

#include <boost/program_options.hpp>

#include "types.hh"

namespace configuration {

namespace po = boost::program_options;

class Configuration {
public:
  i32 max_send_queue_wr{1024};
  i32 max_recv_queue_wr{1024};
  i32 max_poll_cqes{16};
  u32 port{1234};
  u32 device_port{1};
  bool is_server{false};
  vec<str> server_nodes;
  vec<str> client_nodes;
  u32 num_clients{1};
  bool is_initiator{false};

protected:
  po::options_description desc{"Allowed options"};

public:
  Configuration();
  Configuration(int argc, char** argv);

  u32 num_server_nodes() const { return server_nodes.size(); }
  u32 num_client_nodes() const { return client_nodes.size(); }
  friend std::ostream& operator<<(std::ostream& os,
                                  const Configuration& config);

protected:
  static void exit_with_help_message(char** argv);
  void process_program_options(int argc, char** argv);

private:
  void create_rdma_options();
};

}  // namespace configuration

#endif  // RDMA_LIBRARY_CONFIGURATION_HH
