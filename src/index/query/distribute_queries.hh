#ifndef INDEX_DISTRIBUTE_QUERIES_HH
#define INDEX_DISTRIBUTE_QUERIES_HH

#include <library/queue_pair.hh>
#include <library/utils.hh>

#include "query.hh"

namespace query {
using Batch = vec<u32>;
using Batches = vec<Batch>;

void parse_batch(Batch& batch, Queries& queries) {
  auto batch_iter = batch.begin();
  const u32 num_queries = *batch_iter++;
  queries.reserve(num_queries);

  for (u32 i = 0; i < num_queries; ++i) {
    const u32 id = *batch_iter++;
    const QueryType t = static_cast<QueryType>(*batch_iter++);
    const u32 update_id = *batch_iter++;
    const u32 num_keys = *batch_iter++;

    query::Keys keys;
    for (u32 j = 0; j < num_keys; ++j) {
      keys.push_back(*batch_iter++);
    }

    queries.emplace_back(id, t, update_id, std::move(keys));
  }
}

void distribute_queries(Queries& queries,
                        Context& context,
                        QPs& client_qps,
                        u32 num_total_clients) {
  print_status("distribute queries");
  Batches batches(num_total_clients, {0});

  for (query::Query& q : queries) {
    u32 client_id = q.id % num_total_clients;

    auto& batch = batches[client_id];
    ++batch[0];  // increase number of queries

    batch.insert(batch.end(),
                 {q.id,
                  static_cast<u32>(q.type),
                  q.update_id,
                  static_cast<u32>(q.size())});
    batch.insert(batch.end(), q.keys.begin(), q.keys.end());
  }

  queries.clear();
  queries.shrink_to_fit();

  for (u32 client = 1; client < num_total_clients; ++client) {
    Batch& batch = batches[client];
    size_t batch_size = batch.size();
    LocalMemoryRegion region{context, batch.data(), batch_size * sizeof(u32)};

    QP& qp = client_qps[client - 1];
    qp->post_send_inlined(
      std::addressof(batch_size), sizeof(size_t), IBV_WR_SEND, false);
    qp->post_send(region, IBV_WR_SEND);
    context.poll_send_cq_until_completion();
  }

  parse_batch(batches[0], queries);  // re-assign queries (only partially)
}

void receive_queries(Queries& queries, Context& context, QP& initiator_qp) {
  print_status("receive queries");
  size_t batch_size;
  LocalMemoryRegion size_region{
    context, std::addressof(batch_size), sizeof(size_t)};

  initiator_qp->post_receive(size_region);
  context.receive();

  Batch batch(batch_size);
  LocalMemoryRegion batch_region{
    context, batch.data(), batch_size * sizeof(u32)};
  initiator_qp->post_receive(batch_region);
  context.receive();

  parse_batch(batch, queries);
}

}  // namespace query

#endif  // INDEX_DISTRIBUTE_QUERIES_HH
