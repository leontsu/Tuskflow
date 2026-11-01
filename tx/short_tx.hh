#pragma once

// Short-lived transactions of the random-graph workload
// (tasks/storedprocedures/RandomNodeTraversalReadsTx / ...MixedTx):
// from a Zipf-chosen start node, hop (hops - 1) times, then read
// (ReadTx) or write (MixedTx) (ops - hops) Zipf-chosen out-neighbours.
// And of the LDBC workload (tasks/storedprocedures/ldbc/
// RandomFriendMessagesReadTx / ...UpdateTx): from a start Person, hop
// (hops - 1) times over knows, then read or write the Messages behind
// (ops - hops) chosen relationships of the Person's in-neighbourhood.

#include <cstdint>
#include <vector>

#include "util/zipf.hh"
#include "storage/ldbc_loader.hh"
#include "tx/transaction.hh"

namespace tuskflow {

struct ShortTxConfig {
  uint32_t number_of_nodes;              // start nodes
  const std::vector<int32_t>* mappings;  // DBConfig.node/personMappingsForZipf
  int ops;
  int hops;
  bool use_zipf;
  // LDBC: the graph's metadata; start nodes are the Persons, from
  // ranges[PERSON_TYPE].getLeft().  nullptr for the random graph.
  const LdbcGraph* ldbc = nullptr;
  uint32_t first_node = 0;
  // BoMB: S2 for a read-only draw, S1 otherwise; the long transaction is L1.
  const struct BombGraph* bomb = nullptr;
};

class ShortTx : public Tx {
public:
  ShortTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
          const ShortTxConfig& cfg);

protected:
  // Alg.1 l.10-12 (Java runLocally): run the logic in epoch mode; the
  // reservations are made as it reads and writes.
  void run_locally() override;

  uint32_t get_next_id();
  uint32_t pick_index(const ZipfRejectionInversion* zipf_n, size_t size);
  uint32_t single_read_op(const ZipfRejectionInversion* zipf_n,
                          const IntList& out);
  uint32_t get_next_neighbour(uint32_t node_id);
  // Stage (epoch) or apply in place (2PL) "New property" = node_id.
  void write_new_property(uint32_t node_id, Node* node, bool local);

  ShortTxConfig cfg_;
  ZipfRejectionInversion zipf_;  // over the start nodes
};

// RandomNodeTraversalReadsTx
class ReadTx : public ShortTx {
public:
  using ShortTx::ShortTx;

private:
  bool logic(bool local) override;  // readTxLogic
  bool read_neighbours(uint32_t node_id);
};

// RandomNodeTraversalMixedTx
class MixedTx : public ShortTx {
public:
  using ShortTx::ShortTx;

private:
  bool logic(bool local) override;  // mixedTxLogic(local)
  bool updates_neighbours_of_node(uint32_t node_id, bool local);
  bool update_node(uint32_t node_id, bool local);
};

// Common part of the two LDBC transactions.
class FriendMessagesTx : public ShortTx {
public:
  using ShortTx::ShortTx;

protected:
  // getNextNeighbour + serialScan: one hop over knows.  The out-neighbourhood
  // is reserved, its relationships are scanned without reservation.
  uint32_t get_next_ldbc_neighbour(uint32_t node_id);
  bool is_has_creator(const Relationship& rel) const {
    return rel.type() == kCommentHasCreatorPerson ||
           rel.type() == kPostHasCreatorPerson;
  }
  bool is_message_label(int label) const {
    return label == cfg_.ldbc->comment_label || label == cfg_.ldbc->post_label;
  }
};

// RandomFriendMessagesReadTx
class FriendMessagesReadTx : public FriendMessagesTx {
public:
  using FriendMessagesTx::FriendMessagesTx;

private:
  bool logic(bool local) override;  // readTxLogic
  bool read_messages(uint32_t node_id);
};

// RandomFriendMessagesUpdateTx
class FriendMessagesUpdateTx : public FriendMessagesTx {
public:
  using FriendMessagesTx::FriendMessagesTx;

private:
  bool logic(bool local) override;  // updateTxLogic(local)
  bool updates_messages_of_node(uint32_t node_id, bool local);
  bool update_message(uint32_t node_id, bool local);
};

}  // namespace tuskflow
