#include "connection_manager.hh"

#include <iostream>
#include "utils.hh"

ConnectionManager::ConnectionManager(Context& context,
                                     const Configuration& config)
    : context_(context), config_(config) {}

ServerConnectionManager::ServerConnectionManager(Context& context,
                                                 const Configuration& config)
    : ConnectionManager(context, config),
      client_qps(config.num_clients),
      initiator_qp(client_qps.front()) {}

ClientConnectionManager::ClientConnectionManager(Context& context,
                                                 const Configuration& config)
    : ConnectionManager(context, config), is_initiator(config.is_initiator) {
  // reserve memory for queue pairs
  server_qps.reserve(config.num_server_nodes());
  client_qps.reserve(config.num_client_nodes());  // legal no-op if 0
}

void ServerConnectionManager::connect_to_clients() {
  context_.bind_to_port(config_.port);

  // connect queue pairs and order them by client ids
  for (u32 i = 0; i < config_.num_clients; ++i) {
    auto [qp, client_id] = context_.wait_for_connection();
    client_qps[client_id] = std::move(qp);
  }

  context_.close_server_socket();
}

void ClientConnectionManager::connect() {
  connect_among_clients();
  distribute_client_ids();
  connect_to_servers();
}

void ClientConnectionManager::connect_among_clients() {
  if (is_initiator) {
    for (const str& node : config_.client_nodes) {
      std::cerr << "connect to client " << node << std::endl;
      // clients act as "server" (they wait for a connection)
      client_qps.emplace_back(
        context_.connect_to_server(get_ip(node), config_.port));
    }

  } else {
    std::cerr << "connect to initiator" << std::endl;
    context_.bind_to_port(config_.port);

    // connect queue pair to initiator
    initiator_qp = context_.wait_for_connection().first;

    context_.close_server_socket();
  }
}

void ClientConnectionManager::distribute_client_ids() {
  // initiator distributes client ids, number of clients, and threshold
  if (is_initiator) {
    client_id = 0;
    num_total_clients = config_.num_client_nodes() + 1;  // incl. initiator

    for (u32 i = 0; i < config_.num_client_nodes(); ++i) {
      QP& qp = client_qps[i];
      u32 id = i + 1;
      vec<u64> content{id, num_total_clients};
      qp->post_send_inlined(content.data(), 2 * sizeof(u64), IBV_WR_SEND);
    }
    context_.poll_send_cq_until_completion(config_.num_client_nodes());

  } else {
    vec<u64> content(2);
    LocalMemoryRegion region{context_, content.data(), 2 * sizeof(u64)};
    initiator_qp->post_receive(region);
    context_.receive();

    // unpack content
    client_id = content[0];
    num_total_clients = content[1];
  }

  std::cerr << "client id: " << client_id << std::endl;
  std::cerr << "number of clients: " << num_total_clients << std::endl;
}

void ClientConnectionManager::connect_to_servers() {
  for (const str& node : config_.server_nodes) {
    std::cerr << "connect to server " << node << std::endl;
    server_qps.emplace_back(
      context_.connect_to_server(get_ip(node), config_.port, client_id));
  }
}

void ClientConnectionManager::synchronize() {
  for (QP& qp : server_qps) {
    bool ready;
    LocalMemoryRegion region(context_, &ready, sizeof(bool));
    qp->post_receive(region);
    context_.receive();
  }
}

void ServerConnectionManager::synchronize() {
  bool ready = true;

  for (QP& qp : client_qps) {
    qp->post_send_inlined(&ready, sizeof(bool), IBV_WR_SEND);
  }

  context_.poll_send_cq_until_completion(client_qps.size());
}
