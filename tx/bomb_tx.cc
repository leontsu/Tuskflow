#include "tx/bomb_tx.hh"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <thread>

#include "config.hh"
#include "monitor/counters.hh"
#include "util/concurrency.hh"
#include "util/thread_random.hh"

namespace tuskflow {

BombStats g_bomb;

namespace {

double now_us() {
  return std::chrono::duration<double, std::micro>(
             std::chrono::steady_clock::now().time_since_epoch()).count();
}

int64_t property_of(const Node& n, int property_id) {
  Property* const* props = n.properties().properties();
  for (int i = 0; i < n.properties().size(); ++i)
    if (props[i]->propertyId == property_id) return props[i]->value;
  return 0;
}

// Journal vouchers: fresh keys, never read; kept so that inserting one costs
// an allocation, as an insert does.
struct JournalVoucher { int64_t id, amount; };
thread_local std::vector<JournalVoucher> tl_vouchers;
thread_local int64_t tl_voucher_seq = 0;

}  // namespace

void BombStats::record(BombType t, double latency_ms) {
  commits[t].fetch_add(1, std::memory_order_relaxed);
  double us = latency_ms * 1000.0;
  int b = us < 1 ? 0 : static_cast<int>(std::floor(std::log10(us) * 40));
  lat[t][std::min(std::max(b, 0), kBuckets - 1)].fetch_add(1, std::memory_order_relaxed);
}

double BombStats::percentile(BombType t, double q) const {
  uint64_t tot = 0;
  for (int b = 0; b < kBuckets; ++b) tot += lat[t][b].load();
  if (tot == 0) return NAN;
  uint64_t acc = 0;
  for (int b = 0; b < kBuckets; ++b) {
    acc += lat[t][b].load();
    if (acc >= q * tot) return std::pow(10.0, (b + 0.5) / 40.0) / 1000.0;
  }
  return NAN;
}

// --- short transactions ----------------------------------------------------

BombShortTx::BombShortTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
                         const BombGraph* g, BombType type)
    : Tx(tid, storage, output), g_(g), bomb_type_(type), submit_ms_(now_millis()) {}

void BombShortTx::run_locally() {
  protocol_->start_client(tid_, epoch_.load(std::memory_order_relaxed));
  logic(true);
}

bool BombShortTx::try_to_run_on_database() {
  if (!Tx::try_to_run_on_database()) return false;
  on_commit();
  g_bomb.record(bomb_type_, now_millis() - submit_ms_);
  return true;
}

bool BombShortTx::run_on_database_with_retries() {
  if (!Tx::run_on_database_with_retries()) return false;
  on_commit();
  g_bomb.record(bomb_type_, now_millis() - submit_ms_);
  return true;
}

void BombShortTx::prepare_retry() {
  Tx::prepare_retry();
  g_bomb.aborts[bomb_type_].fetch_add(1, std::memory_order_relaxed);
}

void BombShortTx::pause() {
  if (FLAGS_bomb_interactive_us)
    std::this_thread::sleep_for(std::chrono::microseconds(FLAGS_bomb_interactive_us));
}

int64_t BombShortTx::read_property(uint32_t node_id, const std::string& key) {
  Node* n = read_node(node_id);
  Node* c = n->copy();  // under the node's lock
  int64_t v = property_of(*c, PropertyStore::get_mapping(key));
  delete c;
  return v;
}

BombS1Tx::BombS1Tx(int64_t tid, GraphStorage* storage, ResultQueue* output, const BombGraph* g)
    : BombShortTx(tid, storage, output, g, kBombS1),
      f_(static_cast<uint32_t>(tl_rng().random_int(0, g->factories - 1))),
      m_(static_cast<uint32_t>(tl_rng().random_int(0, g->materials - 1))) {}

// run_update_material_cost_master: read the material cost, write it back
// with price + 1 (and quantity + 1), one pause.
bool BombS1Tx::logic(bool local) {
  ser_fresh_attempt();
  const uint32_t id = g_->mc_node(f_, m_);
  int64_t price = read_property(id, kBombPriceKey);
  Node* node = prepare_update_node(id);
  Node* copy = node->copy();
  copies_.push_back(copy);
  if (local) {
    copy->add_property(kBombPriceKey, price + 1);
    Protocol::update_last_write_set_entry(write_set_, nullptr, copy);
  } else {
    uint32_t v = node->add_property_in_place(kBombPriceKey, price + 1, storage_->version_ptr(id));
    if (SerLog::on()) ser_writes_.push_back({id, v});
    Protocol::update_last_write_set_entry(write_set_, copy, node);
  }
  pause();
  return true;
}

BombS2Tx::BombS2Tx(int64_t tid, GraphStorage* storage, ResultQueue* output, const BombGraph* g)
    : BombShortTx(tid, storage, output, g, kBombS2),
      f_(static_cast<uint32_t>(tl_rng().random_int(0, g->factories - 1))) {}

// run_issue_journal_voucher: scan the factory's product costs, pause, one
// journal voucher per product, pause.
bool BombS2Tx::logic(bool) {
  ser_fresh_attempt();
  amounts_.clear();
  for (uint32_t p = 0; p < g_->products; ++p)
    amounts_.push_back(999 * read_property(g_->pc_node(f_, p), kBombCostKey) / 10);
  pause();
  // after the inserts, which are applied when the transaction commits; with
  // --bomb_s2_round_trips = n they are issued in (n - 1) statements
  for (uint64_t i = 1; i < std::max<uint64_t>(FLAGS_bomb_s2_round_trips, 2); ++i) pause();
  return true;
}

void BombS2Tx::on_commit() {
  for (int64_t a : amounts_) tl_vouchers.push_back({++tl_voucher_seq, a});
  g_bomb.jv_rows.fetch_add(amounts_.size(), std::memory_order_relaxed);
}

// --- L1, the mammoth -----------------------------------------------------------

BombL1Tx::BombL1Tx(int64_t tid, GraphStorage* storage, ResultQueue* output, const BombGraph* g,
                   uint32_t factory)
    : AllNodesLongRunningTx(tid, storage, output, g->number_of_nodes, FLAGS_lr_budget),
      g_(g), f_(factory), submit_ms_(now_millis()), costs_(g->products, 0) {}

void BombL1Tx::run_locally() {
  auto t0 = std::chrono::steady_clock::now();
  protocol_->start_client(tid_, epoch_.load(std::memory_order_relaxed));
  const uint64_t d = FLAGS_bomb_interactive_us;
  if (!reads_done_) {  // _run_update_product_cost_master_using_cache: every leaf material cost
    const LongRunningState rs = FLAGS_bomb_l1_read_state == "pending"
                                    ? LongRunningState::PENDING_READ
                                    : LongRunningState::VISITED;
    const int price_id = PropertyStore::get_mapping(kBombPriceKey);
    const int qty_id = PropertyStore::get_mapping(kBombQuantityKey);
    for (uint32_t p = 0; p < g_->products; ++p)
      for (uint32_t m : g_->leaves[p]) {
        const uint32_t id = g_->mc_node(f_, m);
        protocol_->reserve_mammoth_read(read_set_, new_key(), ResourceType::NODE, id, rs);
        uint32_t ver = 0;
        Node* c = storage_->get_node(id)->copy(storage_->version_ptr(id), &ver);
        if (SerLog::on()) ser_reads_.push_back({id, ver});
        const int64_t q = std::max<int64_t>(property_of(*c, qty_id), 1);
        costs_[p] += property_of(*c, price_id) * 1000 / q;
        delete c;
      }
    reads_done_ = true;
    ready_us_ = 0;
  }
  // Writes installed at the end of the previous slice are VISITED from now on.
  for (uint32_t id : pending_)
    protocol_->mark_mammoth_write(ResourceType::NODE, id, LongRunningState::VISITED);
  pending_.clear();
  // The product-cost writes, a pause after each: all at once when d = 0, else
  // one per epoch once the previous pause has elapsed.
  while (next_write_ < g_->products && now_us() >= ready_us_) {
    const uint32_t id = g_->pc_node(f_, next_write_);
    const bool last_slice = d == 0;  // d = 0: every write and the commit in this slice
    protocol_->reserve_mammoth_write(write_set_, new_key(), ResourceType::NODE, id,
                                     last_slice ? LongRunningState::VISITED
                                                : LongRunningState::PENDING_WRITE);
    if (!last_slice) pending_.push_back(id);
    uint32_t ver = 0;
    Node* new_val = storage_->get_node(id)->copy(storage_->version_ptr(id), &ver);
    if (SerLog::on()) ser_reads_.push_back({id, ver});
    copies_.push_back(new_val);
    new_val->add_property(kBombCostKey, costs_[next_write_]);
    Protocol::update_last_write_set_entry(write_set_, nullptr, new_val);
    ++next_write_;
    if (d == 0) continue;
    ready_us_ = now_us() + static_cast<double>(d);
    break;
  }
  if (next_write_ == g_->products && now_us() >= ready_us_ && has_more_work_) {
    has_finished_->fetch_sub(1);
    has_more_work_ = false;
  }
  g_lr_phase1_ns.v.fetch_add(elapsed_ns(t0), std::memory_order_relaxed);
}

void BombL1Tx::on_finished() {
  AllNodesLongRunningTx::on_finished();
  g_bomb.record(kBombL1, now_millis() - submit_ms_);
}

}  // namespace tuskflow
