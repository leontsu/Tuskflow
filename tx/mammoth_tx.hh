#pragma once

// The mammoth of the random-graph workload
// (tasks/storedprocedures/AllNodesLongRunningTx + AbstractLongRunningTransaction):
// writes "Property" = nodeId to every node, in id order, `budget` nodes per
// epoch.  It never aborts: every epoch it installs what it staged and, while
// nodes remain, is scheduled again (Alg.1 l.7-9).
// AllMessagesLongRunningTx is LDBC Q0 (tasks/storedprocedures/ldbc/): the
// same scan, over the Messages only ("Likes" = nodeId; the budget counts
// Messages).

#include <atomic>
#include <chrono>
#include <cstdint>

#include "storage/ldbc_loader.hh"
#include "tx/transaction.hh"

namespace tuskflow {

class AllNodesLongRunningTx : public Tx {
public:
  AllNodesLongRunningTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
                        uint32_t number_of_nodes, uint64_t budget);

  void set_variable_for_completion(std::atomic<int>* has_finished) {
    has_finished_ = has_finished;
  }
  // labelIndexesUpdated for the tag optimization: the Messages (LDBC Q0),
  // nullptr when every node is written.
  const LdbcGraph* ldbc() const { return ldbc_; }

protected:
  // Alg.1 l.10-12 with a budget (Java runLocally +
  // executeWithoutPartitions): mark the next `budget` nodes VISITED
  // (ReserveMammothWrite) and stage their new values.
  void run_locally() override;
  // Alg.1 l.22-23: the mammoth installs without HasConflicts
  // (AbstractLongRunningTransaction.tryToRunOnDatabase).
  bool try_to_run_on_database() override;
  bool has_more_work() const override { return has_finished_->load() != 0; }
  bool is_mammoth() const override { return true; }
  void prepare_retry() override;
  void on_finished() override;
  // The mammoth always runs in epochs (2PL execution is not ported).
  bool run_on_database_with_retries() override;
  bool logic(bool) override { return false; }

  static uint64_t elapsed_ns(std::chrono::steady_clock::time_point t0);

  uint32_t number_of_nodes_;
  uint64_t budget_;
  std::atomic<int>* has_finished_ = nullptr;
  bool has_more_work_ = true;
  uint64_t last_node_id_ = 0;
  const LdbcGraph* ldbc_ = nullptr;  // only Messages are visited when set
  const std::string* property_key_ = &kPropertyKey;
  // Phase 2 cost breakdown.
  std::chrono::steady_clock::time_point commit_t0_;
  uint64_t merged_tsc_ = 0;
};

// AllMessagesLongRunningTx (LDBC Q0)
class AllMessagesLongRunningTx : public AllNodesLongRunningTx {
public:
  AllMessagesLongRunningTx(int64_t tid, GraphStorage* storage,
                           ResultQueue* output, const LdbcGraph* ldbc,
                           uint64_t budget)
      : AllNodesLongRunningTx(tid, storage, output, ldbc->number_of_nodes,
                              budget) {
    ldbc_ = ldbc;
    property_key_ = &kLikesKey;
  }
};

// R2 (Saros comparison, 2026-10-27): pick a Person P with at least one friend
// (knows, both directions), take up to `friends` of them uniformly without
// replacement, read every Message they created (hasCreator), and update a score
// property of P and of each chosen friend.  Reads are reserved PENDING_READ
// (short transactions may read but not write them in this epoch), writes
// VISITED, as the paper prescribes for a mammoth that is not yet done with a
// record it read.  Far below the epoch budget, so it finishes in one epoch.
class FriendScoresLongRunningTx : public AllNodesLongRunningTx {
public:
  FriendScoresLongRunningTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
                            const LdbcGraph* ldbc, uint64_t budget, uint64_t friends,
                            uint64_t seed, int client = -1, uint64_t clients = 1);
  uint64_t reads() const { return reads_; }

protected:
  void run_locally() override;

private:
  uint32_t person_ = 0;
  std::vector<uint32_t> friends_;
  uint64_t reads_ = 0;
};

// LDBC Q2 (2026-10-31): the original's FindExpertsLongRunningTx (a variant of the LDBC BI query "Experts in
// social circle") with the BI query's Country parameter, as the Saros driver's --ldbc_mode=q2. From a random hub
// Person (--q2_hub_factor), the candidates are the Persons first reached at knows distance --q2_min_path..
// --q2_max_path (both directions) who live in this client's Country; the source Tags are the Tags of the hub's
// Messages; each candidate gets the count of its Messages' Tags that are source Tags ("BI-10"). The traversal
// reads graph structure, which no transaction modifies, so only the candidate writes are reserved. Long client
// i takes the i-th of --q2_countries. --q2_stmt_delay_us = d: a round trip after the search and after each
// update; one update per epoch once the previous round trip has elapsed (as BombL1Tx).
// Builds Q2's static Person data (Countries, friends, hubs) once; main() calls it after loading.
void q2_prepare(GraphStorage* storage, const LdbcGraph* ldbc);

class FindExpertsLongRunningTx : public AllNodesLongRunningTx {
public:
  FindExpertsLongRunningTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
                           const LdbcGraph* ldbc, uint64_t budget, uint64_t seed, int client);
  uint64_t candidates() const { return cand_.size(); }

protected:
  void run_locally() override;

private:
  uint32_t source_ = 0;            // node id of the hub
  int country_ = 0;
  bool searched_ = false;
  std::vector<uint32_t> cand_;     // node ids
  std::vector<uint8_t> src_tags_;  // per Tag index: a Tag of the hub's Messages
  size_t next_ = 0;
  double ready_us_ = 0;
  std::vector<uint32_t> pending_;  // written in the previous slice, PENDING_WRITE
};

}  // namespace tuskflow
