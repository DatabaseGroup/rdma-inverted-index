#include "configuration.hh"

#include <boost/program_options.hpp>
#include <iomanip>
#include <iostream>

namespace configuration {

Configuration::Configuration() { create_rdma_options(); }

Configuration::Configuration(int argc, char** argv) : Configuration() {
  process_program_options(argc, argv);
  operator<<(std::cerr, *this);
}

void Configuration::create_rdma_options() {
  desc.add_options()("help,h", "Show help message")(
    "is-server,s",
    po::bool_switch(&is_server)->default_value(is_server),
    "Program acts as server if set")(
    "servers",
    po::value<vec<str>>(&server_nodes)->multitoken(),
    "A list of server nodes to which a client connects, e.g., \"cluster3\"")(
    "clients",
    po::value<vec<str>>(&client_nodes)->multitoken(),
    "A list of client nodes to which the initiator connects, e.g., "
    "\"cluster4 cluster5\"")(
    "initiator,i",
    po::bool_switch(&is_initiator)->default_value(is_initiator),
    "Program acts as initiating client if set")(
    "num-clients,c",
    po::value<u32>(&num_clients)->default_value(num_clients),
    "Number of clients that connect to each server (relevant only for "
    "server nodes)");

  // configuration options
  desc.add_options()(
    "port", po::value<u32>(&port)->default_value(port), "TCP port")(
    "ib-port",
    po::value<u32>(&device_port)->default_value(device_port),
    "Port of infiniband device")(
    "max-poll-cqes",
    po::value<i32>(&max_poll_cqes)->default_value(max_poll_cqes),
    "Number of outstanding RDMA operations allowed (hardware-specific)")(
    "max-send-wrs",
    po::value<i32>(&max_send_queue_wr)->default_value(max_send_queue_wr),
    "Maximum number of outstanding send work requests")(
    "max-receive-wrs",
    po::value<i32>(&max_recv_queue_wr)->default_value(max_recv_queue_wr),
    "Maximum number of outstanding receive work requests");
}

void Configuration::exit_with_help_message(char** argv) {
  std::cerr << "Try " << argv[0] << " --help" << std::endl;
  std::exit(EXIT_FAILURE);
}

void Configuration::process_program_options(int argc, char** argv) {
  try {
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
      std::cerr << desc << std::endl;
      std::exit(EXIT_FAILURE);
    }

    po::notify(vm);

    if (!is_server && server_nodes.empty()) {
      std::cerr << "[ERROR]: --servers <arg-list> must be given if "
                   "--server is not set"
                << std::endl;
      exit_with_help_message(argv);
    }

    if (is_server && is_initiator) {
      std::cerr << "[ERROR]: a server cannot be the initiator" << std::endl;
      exit_with_help_message(argv);
    }

    if (!is_initiator && !client_nodes.empty()) {
      std::cerr << "[ERROR]: --clients <arg-list> is only required by the "
                   "initiating client"
                << std::endl;
      exit_with_help_message(argv);
    }

  } catch (const std::exception& e) {
    std::cerr << "[ERROR]: " << e.what() << std::endl;
    exit_with_help_message(argv);
  }
}

std::ostream& operator<<(std::ostream& os, const Configuration& config) {
  const int32_t width = 30;
  const int32_t max_width = width * 2;
  const char filler = '=';

  os << std::setfill(filler) << std::setw(max_width) << "" << std::endl
     << std::setfill(' ') << std::setw(width)
     << (config.is_server ? "SERVER" : "CLIENT") << std::endl
     << std::setfill(filler) << std::setw(max_width) << "" << std::endl;

  os << std::left << std::setfill(' ');
  if (!config.is_server) {
    os << std::setw(width) << "connect to: "
       << "[";
    for (const str& node : config.server_nodes) {
      os << node << ", ";
    }
    os << "\b\b]" << std::endl;
    os << std::boolalpha << std::setw(width)
       << "is initiator: " << config.is_initiator << std::endl;
    if (config.is_initiator) {
      if (!config.client_nodes.empty()) {
        os << std::setw(width) << "client nodes: "
           << "[";
        for (const str& node : config.client_nodes) {
          os << node << ", ";
        }
        os << "\b\b]" << std::endl;
      }
    }
  } else {
    os << std::setw(width) << "num clients: " << config.num_clients
       << std::endl;
  }
  os << std::setw(width) << "TCP port: " << config.port << std::endl
     << std::setw(width) << "IB port: " << config.device_port << std::endl
     << std::setw(width) << "max outstanding CQEs: " << config.max_poll_cqes
     << std::endl
     << std::setw(width)
     << "max send work requests: " << config.max_send_queue_wr << std::endl
     << std::setw(width)
     << "max receive work requests: " << config.max_recv_queue_wr << std::endl;

  os << std::setfill(filler) << std::setw(max_width) << "" << std::endl;

  return os;
}

}  // namespace configuration
