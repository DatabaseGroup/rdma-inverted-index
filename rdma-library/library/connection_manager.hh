
#ifndef RDMA_LIBRARY_CONNECTION_MANAGER_HH
#define RDMA_LIBRARY_CONNECTION_MANAGER_HH

#include "configuration.hh"
#include "context.hh"
#include "memory_region.hh"
#include "queue_pair.hh"

class ConnectionManager {
public:
  using Configuration = configuration::Configuration;

public:
  Context& get_context() const { return context_; }

protected:
  ConnectionManager(Context& context, const Configuration& config);

protected:
  Context& context_;
  const Configuration& config_;
};

class ServerConnectionManager : public ConnectionManager {
public:
  ServerConnectionManager(Context& context, const Configuration& config);
  void connect_to_clients();
  void synchronize();

public:
  QPs client_qps;
  QP& initiator_qp;
};

class ClientConnectionManager : public ConnectionManager {
public:
  ClientConnectionManager(Context& context, const Configuration& config);
  void connect();
  void synchronize();

private:
  void connect_among_clients();
  void distribute_client_ids();
  void connect_to_servers();

public:
  const bool is_initiator;
  u32 client_id{};
  u32 num_total_clients{};
  QPs server_qps;
  QPs client_qps;  // relevant only for the initiator
  QP initiator_qp;  // relevant only for non-initiators
};

#endif  // RDMA_LIBRARY_CONNECTION_MANAGER_HH
