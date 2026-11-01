#include "tx/mammoth_tx.hh"

#include <x86intrin.h>

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <random>
#include <vector>
#include <chrono>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>

#include "config.hh"
#include "monitor/counters.hh"

namespace tuskflow {

AllNodesLongRunningTx::AllNodesLongRunningTx(int64_t tid, GraphStorage* storage,
                                             ResultQueue* output,
                                             uint32_t number_of_nodes,
                                             uint64_t budget)
    : Tx(tid, storage, output), number_of_nodes_(number_of_nodes),
      budget_(budget) {}

void AllNodesLongRunningTx::run_locally() {
  auto t0 = std::chrono::steady_clock::now();
  protocol_->start_client(tid_, epoch_.load(std::memory_order_relaxed));
  // executeWithoutPartitions
  int64_t budget = static_cast<int64_t>(budget_);
  uint64_t node_id = last_node_id_;
  const bool bd = FLAGS_lr_breakdown;
  uint64_t c_res = 0, c_get = 0, c_copy = 0, c_rest = 0;
  while (budget > 0 && node_id < number_of_nodes_) {
    if (ldbc_ != nullptr && !ldbc_->is_message(static_cast<uint32_t>(node_id))) {
      ++node_id;  // LdbcMetadata.messageIds.contains(nodeId)
      continue;
    }
    uint64_t t_0 = bd ? __rdtsc() : 0;
    protocol_->reserve_mammoth_write(write_set_, new_key(), ResourceType::NODE,
                                     static_cast<int64_t>(node_id),
                                     LongRunningState::VISITED);
    uint64_t t_1 = bd ? __rdtsc() : 0;
    Node* node = storage_->get_node(static_cast<uint32_t>(node_id));
    if (node == nullptr)
      std::printf("[DBG] node %" PRIu64 " not found\n", node_id);
    uint64_t t_2 = bd ? __rdtsc() : 0;
    uint32_t ver_read = 0;
    Node* new_val = node->copy(storage_->version_ptr(static_cast<uint32_t>(node_id)),
                               &ver_read);
    if (SerLog::on())
      ser_reads_.push_back({static_cast<uint32_t>(node_id), ver_read});
    uint64_t t_3 = bd ? __rdtsc() : 0;
    copies_.push_back(new_val);
    new_val->add_property(*property_key_, static_cast<int64_t>(node_id));
    Protocol::update_last_write_set_entry(write_set_, nullptr, new_val);
    if (bd) {
      uint64_t t_4 = __rdtsc();
      c_res += t_1 - t_0;
      c_get += t_2 - t_1;
      c_copy += t_3 - t_2;
      c_rest += t_4 - t_3;
    }
    ++node_id;
    --budget;
  }
  if (bd) {
    g_lr_cyc_reserve.v.fetch_add(c_res);
    g_lr_cyc_get.v.fetch_add(c_get);
    g_lr_cyc_copy.v.fetch_add(c_copy);
    g_lr_cyc_rest.v.fetch_add(c_rest);
  }
  last_node_id_ = node_id;
  if (node_id >= number_of_nodes_ && has_more_work_) {
    has_finished_->fetch_sub(1);
    has_more_work_ = false;
  }
  g_lr_phase1_ns.v.fetch_add(elapsed_ns(t0), std::memory_order_relaxed);
}

bool AllNodesLongRunningTx::try_to_run_on_database() {
  commit_t0_ = std::chrono::steady_clock::now();
  uint64_t m0 = __rdtsc();
  for (RWKey* w : write_set_) {  // "Writes must always succeed"
    uint32_t v = storage_->update_node(*w->newVal);
    if (SerLog::on()) ser_writes_.push_back({static_cast<uint32_t>(w->id), v});
  }
  if (SerLog::on()) {
    SerLog::emit(tid_, kSerMammoth, ser_reads_, ser_writes_);
    ser_fresh_attempt();
  }
  merged_tsc_ = __rdtsc();
  g_lr_cyc_merge.v.fetch_add(merged_tsc_ - m0);
  return true;
}

// More nodes remain: readSet/writeSet.clear(); the JVM collects the copies of
// this epoch.
void AllNodesLongRunningTx::prepare_retry() {
  release_allocations();
  g_lr_cyc_free.v.fetch_add(__rdtsc() - merged_tsc_);
  g_lr_phase2_ns.v.fetch_add(elapsed_ns(commit_t0_), std::memory_order_relaxed);
}

void AllNodesLongRunningTx::on_finished() {
  g_lr_phase2_ns.v.fetch_add(elapsed_ns(commit_t0_), std::memory_order_relaxed);
  if (output_ != nullptr) output_->add(new Result{true});
}

bool AllNodesLongRunningTx::run_on_database_with_retries() {
  std::fprintf(stderr, "EXECUTE_AND_COMMIT (2PL) is not implemented\n");
  std::abort();
}

uint64_t AllNodesLongRunningTx::elapsed_ns(std::chrono::steady_clock::time_point t0) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - t0)
          .count());
}

FriendScoresLongRunningTx::FriendScoresLongRunningTx(
    int64_t tid, GraphStorage* storage, ResultQueue* output, const LdbcGraph* ldbc,
    uint64_t budget, uint64_t friends, uint64_t seed, int client, uint64_t clients)
    : AllNodesLongRunningTx(tid, storage, output, ldbc->number_of_nodes, budget) {
  ldbc_ = ldbc;
  property_key_ = &kLikesKey;
  std::mt19937_64 rnd(seed);
  const int32_t first = ldbc->range_first[kPersonType], last = ldbc->range_last[kPersonType];
  std::vector<uint32_t> all;
  for (;;) {  // a Person with at least one friend
    const uint64_t n = static_cast<uint64_t>(last - first + 1);
    if (FLAGS_r2_own_persons && client >= 0) {  // --r2_own_persons: Persons client, client + k, ...
      const uint64_t c = static_cast<uint64_t>(client);
      person_ = static_cast<uint32_t>(first + c + clients * (rnd() % ((n - 1 - c) / clients + 1)));
    } else {
      person_ = static_cast<uint32_t>(first + rnd() % n);
    }
    all.clear();
    if (const IntList* out = storage->get_out_relationships(person_))
      for (int32_t r : *out) {
        Relationship* rel = storage->get_relationship(static_cast<uint32_t>(r));
        if (rel != nullptr && rel->type() == kPersonKnowsPerson) all.push_back(rel->end_node());
      }
    if (const IntList* in = storage->get_in_relationships(person_))
      for (int32_t r : *in) {
        Relationship* rel = storage->get_relationship(static_cast<uint32_t>(r));
        if (rel != nullptr && rel->type() == kPersonKnowsPerson) all.push_back(rel->start_node());
      }
    std::sort(all.begin(), all.end());
    all.erase(std::unique(all.begin(), all.end()), all.end());
    if (!all.empty()) break;
  }
  for (size_t i = 0; i < all.size() && i < friends; ++i)  // partial Fisher-Yates
    std::swap(all[i], all[i + rnd() % (all.size() - i)]);
  if (all.size() > friends) all.resize(friends);
  friends_ = std::move(all);
}

void FriendScoresLongRunningTx::run_locally() {
  auto t0 = std::chrono::steady_clock::now();
  protocol_->start_client(tid_, epoch_.load(std::memory_order_relaxed));
  auto write_score = [&](uint32_t node_id) {
    protocol_->reserve_mammoth_write(write_set_, new_key(), ResourceType::NODE,
                                     static_cast<int64_t>(node_id),
                                     LongRunningState::VISITED);
    Node* node = storage_->get_node(node_id);
    uint32_t ver_read = 0;
    Node* new_val = node->copy(storage_->version_ptr(node_id), &ver_read);
    if (SerLog::on()) ser_reads_.push_back({node_id, ver_read});
    copies_.push_back(new_val);
    new_val->add_property(*property_key_, static_cast<int64_t>(reads_));
    Protocol::update_last_write_set_entry(write_set_, nullptr, new_val);
  };
  for (uint32_t f : friends_) {
    if (const IntList* in = storage_->get_in_relationships(f))
      for (int32_t r : *in) {
        Relationship* rel = storage_->get_relationship(static_cast<uint32_t>(r));
        if (rel == nullptr || (rel->type() != kCommentHasCreatorPerson &&
                               rel->type() != kPostHasCreatorPerson))
          continue;
        const uint32_t m = rel->start_node();
        protocol_->reserve_mammoth_read(read_set_, new_key(), ResourceType::NODE,
                                        static_cast<int64_t>(m),
                                        LongRunningState::PENDING_READ);
        if (storage_->get_node(m) == nullptr) std::printf("[DBG] node %u not found\n", m);
        if (SerLog::on()) ser_reads_.push_back({m, storage_->version(m)});
        ++reads_;
      }
  }
  write_score(person_);
  if (!FLAGS_r2_source_only)
    for (uint32_t f : friends_) write_score(f);
  if (has_more_work_) {
    has_finished_->fetch_sub(1);
    has_more_work_ = false;
  }
  g_lr_phase1_ns.v.fetch_add(elapsed_ns(t0), std::memory_order_relaxed);
}

// --- LDBC Q2 (FindExperts with the BI query's Country) -------------------------------------------------------

namespace {
inline const std::string kBi10Key = "BI-10";

double q2_now_us() {
  return std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Static Person data, built once: Country per Person (the Person CSV's LocationCityId -> its Country), distinct
// friends (both knows directions, Person indices), the hubs, and each long client's Country.
struct Q2Context {
  int32_t first = 0;                           // node id of Person index 0
  std::vector<int16_t> country;                // per Person index
  std::vector<std::vector<uint32_t>> friends;  // per Person index
  std::vector<uint32_t> hubs;                  // Person indices
  std::vector<int16_t> client_country;
  // The Tags of each Person's Messages, flattened (CSR over Person indices; Tag index = node id - tag_first),
  // as the Saros driver's static arrays, so both systems count Tags at the same cost.
  int32_t tag_first = 0;
  uint32_t tag_n = 0;
  std::vector<uint32_t> ptag_off, ptag;
};
Q2Context g_q2;
std::once_flag g_q2_once;

std::vector<std::vector<std::string>> read_csv(const std::string& file) {
  std::vector<std::vector<std::string>> rows;
  std::ifstream in(file);
  if (!in) { std::fprintf(stderr, "q2: cannot open %s\n", file.c_str()); std::exit(1); }
  std::string line;
  std::getline(in, line);
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    std::vector<std::string> cols;
    size_t s = 0;
    for (size_t b; (b = line.find('|', s)) != std::string::npos; s = b + 1) cols.push_back(line.substr(s, b - s));
    cols.push_back(line.substr(s));
    rows.push_back(std::move(cols));
  }
  return rows;
}

// Tag node ids of the Messages created by the Person `node` (hasCreator in, hasTag out), via `f`.
template <typename F>
void for_each_message_tag(GraphStorage* storage, uint32_t node, F&& f) {
  const IntList* in = storage->get_in_relationships(node);
  if (in == nullptr) return;
  for (int32_t r : *in) {
    Relationship* rel = storage->get_relationship(static_cast<uint32_t>(r));
    if (rel == nullptr || (rel->type() != kCommentHasCreatorPerson && rel->type() != kPostHasCreatorPerson)) continue;
    const IntList* out = storage->get_out_relationships(static_cast<uint32_t>(rel->start_node()));
    if (out == nullptr) continue;
    for (int32_t t : *out) {
      Relationship* tr = storage->get_relationship(static_cast<uint32_t>(t));
      if (tr != nullptr && (tr->type() == 5 || tr->type() == 23)) f(static_cast<int32_t>(tr->end_node()));
    }
  }
}
void q2_init(GraphStorage* storage, const LdbcGraph* ldbc) {
  Q2Context& q = g_q2;
  q.first = ldbc->range_first[kPersonType];
  const uint32_t n = static_cast<uint32_t>(ldbc->range_last[kPersonType] - q.first + 1);
  std::unordered_map<std::string, std::pair<std::string, std::string>> place;  // id -> (name, type)
  std::unordered_map<std::string, std::string> part_of;
  for (auto& c : read_csv(FLAGS_ldbc_path + "/static/Place/part-0.csv")) {
    place[c[0]] = {c[1], c[3]};
    part_of[c[0]] = c[4];
  }
  std::unordered_map<std::string, int16_t> country_id;
  q.country.assign(n, -1);
  uint32_t i = 0;
  for (auto& c : read_csv(FLAGS_ldbc_path + "/dynamic/Person/part-0.csv")) {  // node loading order
    std::string cid = c[10];
    if (place[cid].second == "City") cid = part_of[cid];
    auto it = country_id.emplace(place[cid].first, static_cast<int16_t>(country_id.size()));
    if (i < n) q.country[i] = it.first->second;
    ++i;
  }
  q.friends.assign(n, {});
  double sum = 0;
  for (uint32_t p = 0; p < n; ++p) {
    auto& f = q.friends[p];
    const uint32_t node = static_cast<uint32_t>(q.first) + p;
    if (const IntList* out = storage->get_out_relationships(node))
      for (int32_t r : *out) {
        Relationship* rel = storage->get_relationship(static_cast<uint32_t>(r));
        if (rel != nullptr && rel->type() == kPersonKnowsPerson) f.push_back(static_cast<uint32_t>(rel->end_node() - q.first));
      }
    if (const IntList* in = storage->get_in_relationships(node))
      for (int32_t r : *in) {
        Relationship* rel = storage->get_relationship(static_cast<uint32_t>(r));
        if (rel != nullptr && rel->type() == kPersonKnowsPerson) f.push_back(static_cast<uint32_t>(rel->start_node() - q.first));
      }
    std::sort(f.begin(), f.end());
    f.erase(std::unique(f.begin(), f.end()), f.end());
    sum += static_cast<double>(f.size());
  }
  for (uint32_t p = 0; p < n; ++p)
    if (static_cast<double>(q.friends[p].size()) >= static_cast<double>(FLAGS_q2_hub_factor) * sum / n) q.hubs.push_back(p);
  q.tag_first = ldbc->range_first[kTagType];
  q.tag_n = static_cast<uint32_t>(ldbc->range_last[kTagType] - q.tag_first + 1);
  q.ptag_off.assign(n + 1, 0);
  for (uint32_t p = 0; p < n; ++p) {
    for_each_message_tag(storage, static_cast<uint32_t>(q.first) + p,
                         [&](int32_t t) { q.ptag.push_back(static_cast<uint32_t>(t - q.tag_first)); });
    q.ptag_off[p + 1] = static_cast<uint32_t>(q.ptag.size());
  }
  const std::string& list = FLAGS_q2_countries;
  for (size_t b = 0; b <= list.size();) {
    size_t e = list.find(',', b);
    if (e == std::string::npos) e = list.size();
    auto it = country_id.find(list.substr(b, e - b));
    if (it == country_id.end()) { std::fprintf(stderr, "q2: unknown country %s\n", list.substr(b, e - b).c_str()); std::exit(1); }
    q.client_country.push_back(it->second);
    b = e + 1;
  }
  std::printf("#ldbc_q2_graph\thubs=%zu\tavg_friends=%.1f\tcountries=%zu\n", q.hubs.size(), sum / n, country_id.size());
  std::fflush(stdout);
}

}  // namespace

void q2_prepare(GraphStorage* storage, const LdbcGraph* ldbc) { std::call_once(g_q2_once, q2_init, storage, ldbc); }

FindExpertsLongRunningTx::FindExpertsLongRunningTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
                                                   const LdbcGraph* ldbc, uint64_t budget, uint64_t seed,
                                                   int client)
    : AllNodesLongRunningTx(tid, storage, output, ldbc->number_of_nodes, budget) {
  ldbc_ = ldbc;
  property_key_ = &kBi10Key;
  q2_prepare(storage, ldbc);
  std::mt19937_64 rnd(seed);
  source_ = static_cast<uint32_t>(g_q2.first) +
            (FLAGS_q2_hub >= 0 ? g_q2.hubs[FLAGS_q2_hub % g_q2.hubs.size()] : g_q2.hubs[rnd() % g_q2.hubs.size()]);
  country_ = g_q2.client_country[static_cast<size_t>(std::max(client, 0)) % g_q2.client_country.size()];
}

void FindExpertsLongRunningTx::run_locally() {
  auto t0 = std::chrono::steady_clock::now();
  protocol_->start_client(tid_, epoch_.load(std::memory_order_relaxed));
  const uint64_t d = FLAGS_q2_stmt_delay_us;
  if (!searched_) {  // the search: knows BFS from the hub, candidates in the Country, source Tags
    const uint32_t n = static_cast<uint32_t>(g_q2.friends.size());
    std::vector<uint8_t> seen(n, 0);
    std::vector<uint32_t> front{source_ - static_cast<uint32_t>(g_q2.first)}, next;
    seen[front[0]] = 1;
    for (uint64_t dist = 1; dist <= FLAGS_q2_max_path && !front.empty(); ++dist) {
      next.clear();
      for (uint32_t u : front)
        for (uint32_t v : g_q2.friends[u]) {
          if (seen[v]) continue;
          seen[v] = 1;
          next.push_back(v);
          if (dist >= FLAGS_q2_min_path && (FLAGS_q2_all_countries || g_q2.country[v] == country_))
            cand_.push_back(static_cast<uint32_t>(g_q2.first) + v);
        }
      front.swap(next);
    }
    const uint32_t s = source_ - static_cast<uint32_t>(g_q2.first);
    src_tags_.assign(g_q2.tag_n, 0);
    for (uint32_t j = g_q2.ptag_off[s]; j < g_q2.ptag_off[s + 1]; ++j) src_tags_[g_q2.ptag[j]] = 1;
    searched_ = true;
    if (FLAGS_q2_all_countries) std::printf("#q2_candidates\t%zu\n", cand_.size());
    ready_us_ = d ? q2_now_us() + static_cast<double>(d) : 0;
  }
  // Updates installed at the end of the previous slice are VISITED from now on.
  for (uint32_t id : pending_)
    protocol_->mark_mammoth_write(ResourceType::NODE, id, LongRunningState::VISITED);
  pending_.clear();
  while (next_ < cand_.size() && q2_now_us() >= ready_us_) {
    const uint32_t id = cand_[next_];
    int64_t count = 0;
    const uint32_t c = id - static_cast<uint32_t>(g_q2.first);
    for (uint32_t j = g_q2.ptag_off[c]; j < g_q2.ptag_off[c + 1]; ++j) count += src_tags_[g_q2.ptag[j]];
    const bool last_slice = d == 0;
    protocol_->reserve_mammoth_write(write_set_, new_key(), ResourceType::NODE, id,
                                     last_slice ? LongRunningState::VISITED : LongRunningState::PENDING_WRITE);
    if (!last_slice) pending_.push_back(id);
    uint32_t ver = 0;
    Node* new_val = storage_->get_node(id)->copy(storage_->version_ptr(id), &ver);
    if (SerLog::on()) ser_reads_.push_back({id, ver});
    copies_.push_back(new_val);
    new_val->add_property(kBi10Key, count);
    Protocol::update_last_write_set_entry(write_set_, nullptr, new_val);
    ++next_;
    if (d == 0) continue;
    ready_us_ = q2_now_us() + static_cast<double>(d);
    break;
  }
  if (next_ == cand_.size() && q2_now_us() >= ready_us_ && has_more_work_) {
    has_finished_->fetch_sub(1);
    has_more_work_ = false;
  }
  g_lr_phase1_ns.v.fetch_add(elapsed_ns(t0), std::memory_order_relaxed);
}

}  // namespace tuskflow
