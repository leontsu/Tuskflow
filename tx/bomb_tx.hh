#pragma once

// BoMB transactions (Saros comparison, 2026-10-29), as ccbench include/bomb.hh
// runs them with --bomb_use_cache (static BoM), --bomb_mc_update_size=1 and
// --bomb_interactive_ms=d (a pause of d microseconds per statement):
//   S1 UpdateMaterialCostMaster: read and update one material cost (f, m);
//      one pause.
//   S2 IssueJournalVoucher: read the product costs of factory f, pause,
//      insert one journal voucher per product, pause.
//   L1 UpdateProductCostMaster (the mammoth): read every leaf material cost
//      of the factory's products, then write each product cost, pausing after
//      each write.
// S1 and S2 run like the LDBC short transactions: logic(true) in epochs,
// logic(false) under 2PL.  An aborted epoch attempt runs its logic again in
// the next epoch (Alg. 1 l.8-9), pauses included.  The port's replay of the
// previous attempt (Java runPreviousWork, "not in Algorithm 1") re-installs the
// values read then; S1 and S2 compute their writes from what they read, so a
// replay after the mammoth moved on commits stale reads (cycles in ser_check).
// The interactive L1 is spread over epochs: one product-cost write per epoch
// once the pause after the previous write has elapsed; epochs in between run
// without it.  Its material reads are reserved VISITED (TuskFlow's best case:
// the mammoth is done with them) or PENDING_READ until it commits
// (--bomb_l1_read_state).  A product-cost write is PENDING_WRITE in the epoch
// that stages it ("the mammoth is still updating the record", TuskFlow Fig. 4)
// and VISITED from the next epoch, once installed.  Marking it VISITED while
// it is staged (as the Q0 mammoth does) lets a short transaction of that epoch
// read the installed VISITED records and the old value of the one being
// written, and count as after the mammoth (cycles in ser_check); the last
// slice, which commits the mammoth, still writes VISITED.
// Journal vouchers have fresh keys and never conflict; they are appended to a
// per-thread store when the transaction commits.

#include <atomic>
#include <cstdint>
#include <vector>

#include "storage/bomb_loader.hh"
#include "tx/mammoth_tx.hh"
#include "tx/transaction.hh"

namespace tuskflow {

// Per-type counters, printed at the end of the run.
enum BombType { kBombS1 = 0, kBombS2 = 1, kBombL1 = 2, kBombTypes = 3 };
struct BombStats {
  static constexpr int kBuckets = 400;  // 40 per decade, 1 us .. 10^10 us
  std::atomic<uint64_t> commits[kBombTypes]{};
  std::atomic<uint64_t> aborts[kBombTypes]{};
  std::atomic<uint64_t> lat[kBombTypes][kBuckets]{};
  std::atomic<uint64_t> jv_rows{0};
  void record(BombType t, double latency_ms);
  double percentile(BombType t, double q) const;  // ms, bucket midpoint
};
extern BombStats g_bomb;

class BombShortTx : public Tx {
public:
  BombShortTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
              const BombGraph* g, BombType type);

protected:
  void run_locally() override;
  bool try_to_run_on_database() override;
  bool run_on_database_with_retries() override;
  void prepare_retry() override;
  virtual void on_commit() {}
  static void pause();
  int64_t read_property(uint32_t node_id, const std::string& key);

  const BombGraph* g_;
  BombType bomb_type_;
  double submit_ms_;
};

class BombS1Tx : public BombShortTx {
public:
  BombS1Tx(int64_t tid, GraphStorage* storage, ResultQueue* output, const BombGraph* g);

private:
  bool logic(bool local) override;
  uint32_t f_, m_;
};

class BombS2Tx : public BombShortTx {
public:
  BombS2Tx(int64_t tid, GraphStorage* storage, ResultQueue* output, const BombGraph* g);

private:
  bool logic(bool local) override;
  void on_commit() override;
  uint32_t f_;
  std::vector<int64_t> amounts_;  // one journal voucher per product
};

class BombL1Tx : public AllNodesLongRunningTx {
public:
  BombL1Tx(int64_t tid, GraphStorage* storage, ResultQueue* output, const BombGraph* g,
           uint32_t factory);

protected:
  void run_locally() override;
  void on_finished() override;

private:
  const BombGraph* g_;
  uint32_t f_;
  double submit_ms_;
  bool reads_done_ = false;
  uint32_t next_write_ = 0;
  double ready_us_ = 0;          // the pause after the previous write ends
  std::vector<int64_t> costs_;
  std::vector<uint32_t> pending_;  // written in the previous slice, PENDING_WRITE
};

}  // namespace tuskflow
