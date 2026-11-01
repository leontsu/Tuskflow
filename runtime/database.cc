#include "runtime/database.hh"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <vector>
#include <cstdio>
#include <thread>

#include "config.hh"
#include "protocol/two_pl_protocol.hh"
#include "tx/bomb_tx.hh"
#include "tx/mammoth_tx.hh"
#include "util/thread_random.hh"
#include "util/concurrency.hh"

namespace tuskflow {

// --bomb_l1_own_factories: the index of the long-running client on this thread.
thread_local int tl_l1_client = -1;

void Database::submit_query(bool is_read, bool long_running, ResultQueue* output) {
  int task_id = last_tx_id_.fetch_add(1) + 1;
  Task* task = TaskPool::new_instance(task_id, long_running);
  if (cfg_.bomb != nullptr) {
    if (long_running) {  // a uniformly chosen factory, as BoMB's L1
      uint32_t f = static_cast<uint32_t>(tl_rng().random_int(0, cfg_.bomb->factories - 1));
      if (FLAGS_bomb_l1_own_factories && tl_l1_client >= 0) {
        const uint32_t k = static_cast<uint32_t>(FLAGS_bomb_l1_clients);
        const uint32_t i = static_cast<uint32_t>(tl_l1_client);
        f = i + k * static_cast<uint32_t>(tl_rng().random_int(0, (cfg_.bomb->factories - 1 - i) / k));
      }
      task->tx = new BombL1Tx(task_id, storage_, output, cfg_.bomb, f);
    }
    else if (is_read)
      task->tx = new BombS2Tx(task_id, storage_, output, cfg_.bomb);
    else
      task->tx = new BombS1Tx(task_id, storage_, output, cfg_.bomb);
  } else if (long_running) {
    if (cfg_.ldbc != nullptr && FLAGS_lr_kind == "q2")
      task->tx = new FindExpertsLongRunningTx(task_id, storage_, output, cfg_.ldbc, FLAGS_lr_budget,
                                              0x9e2 + static_cast<uint64_t>(task_id), tl_l1_client);
    else if (cfg_.ldbc != nullptr && FLAGS_lr_kind == "r2")
      task->tx = new FriendScoresLongRunningTx(task_id, storage_, output, cfg_.ldbc,
                                               FLAGS_lr_budget, FLAGS_r2_friends,
                                               0x5a305 + static_cast<uint64_t>(task_id),
                                               tl_l1_client, FLAGS_lr_clients);
    else if (cfg_.ldbc != nullptr)  // LDBC_QUERY_TYPE = 0
      task->tx = new AllMessagesLongRunningTx(task_id, storage_, output,
                                              cfg_.ldbc, FLAGS_lr_budget);
    else
      task->tx = new AllNodesLongRunningTx(task_id, storage_, output,
                                           cfg_.number_of_nodes,
                                           FLAGS_lr_budget);
  } else if (cfg_.ldbc != nullptr) {  // USE_TRAVERSALS = true
    if (is_read)
      task->tx = new FriendMessagesReadTx(task_id, storage_, output, cfg_);
    else
      task->tx = new FriendMessagesUpdateTx(task_id, storage_, output, cfg_);
  } else if (is_read) {
    task->tx = new ReadTx(task_id, storage_, output, cfg_);
  } else {
    task->tx = new MixedTx(task_id, storage_, output, cfg_);
  }
  // CypherParser creates every transaction with `new TwoPLImpl()`.
  task->tx->set_transaction_type(TxType::EXECUTE_AND_COMMIT,
                                 new TwoPLProtocol(locks_), EpochContext{});
  task->start_time_ms = now_millis();
  manager_->submit(task);
}

// PrototypeClient with USE_ASYNC_CLIENTS = true.
void client_main(Database* db, std::atomic<bool>* bench_running,
                 int read_ratio, uint64_t txs_per_second) {
  JavaRandom rnd(42);  // every client uses new Random(42L)
  // The connection outlives the client thread: transactions still in flight
  // add their results to it (kept alive by the GC in the original).
  ResultQueue& results = *new ResultQueue();
  while (!bench_running->load())
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  if (FLAGS_client_paced && txs_per_second > 0) {
    // Evenly spaced submissions, as BoMB's request dispatcher: client i starts
    // i/clients of a period late so that the clients interleave.
    static std::atomic<uint64_t> next_client{0};
    const auto period = std::chrono::nanoseconds(1000000000ULL / txs_per_second);
    auto next = std::chrono::steady_clock::now() +
                period * static_cast<int64_t>(next_client.fetch_add(1)) /
                    static_cast<int64_t>(std::max<uint64_t>(FLAGS_clients, 1));
    while (bench_running->load()) {
      std::this_thread::sleep_until(next);
      next += period;
      bool is_read = rnd.next_int(100) < read_ratio;
      db->submit_query(is_read, false, &results);
      while (Result* r = results.poll()) delete r;
    }
    return;
  }
  uint64_t throughput_counter = 0;
  auto start = std::chrono::steady_clock::now();
  while (bench_running->load()) {
    bool is_read = rnd.next_int(100) < read_ratio;  // randInt(rand, 0, 99)
    db->submit_query(is_read, false, &results);
    ++throughput_counter;
    if (txs_per_second > 0 && throughput_counter == txs_per_second) {
      auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start)
                    .count();
      if (dt < 1000)
        std::this_thread::sleep_for(std::chrono::milliseconds(1000 - dt));
      start = std::chrono::steady_clock::now();
      throughput_counter = 0;
    }
    while (Result* r = results.poll()) delete r;  // consumeAsync
  }
}

// PrototypeLongRunningClient
void long_running_client_main(Database* db, std::atomic<bool>* bench_running,
                              uint64_t delay_ms) {
  static std::atomic<int> next_l1_client{0};
  tl_l1_client = next_l1_client.fetch_add(1);
  ResultQueue& results = *new ResultQueue();
  while (!bench_running->load())
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto start = std::chrono::steady_clock::now();
  while (bench_running->load()) {
    auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - start)
                  .count();
    if (static_cast<uint64_t>(dt) >= delay_ms) {
      start = std::chrono::steady_clock::now();
      std::printf("[DBG] Starting long-running tx.\n");
      std::fflush(stdout);
      db->submit_query(false, true, &results);
      while (results.empty()) park_nanos_1();
      delete results.poll();
      double ms = std::chrono::duration<double, std::milli>(
                      std::chrono::steady_clock::now() - start)
                      .count();
      if (!FLAGS_lr_repeat) {
        std::printf("[DBG] Long-running tx has ended in %f millis.\n", ms);
        std::fflush(stdout);
        return;
      }
      // --lr_repeat: the next long transaction as soon as this one finishes.
      std::vector<double> lat_ms{ms};
      auto t_first = start;
      double last_end_ms = 0;
      while (bench_running->load()) {
        auto s = std::chrono::steady_clock::now();
        db->submit_query(false, true, &results);
        while (results.empty()) park_nanos_1();
        delete results.poll();
        auto e = std::chrono::steady_clock::now();
        if (!bench_running->load()) break;  // finished after the end: not counted
        lat_ms.push_back(std::chrono::duration<double, std::milli>(e - s).count());
        last_end_ms = std::chrono::duration<double, std::milli>(e - t_first).count();
      }
      std::sort(lat_ms.begin(), lat_ms.end());
      auto q = [&](double p) { return lat_ms[std::min(lat_ms.size() - 1, static_cast<size_t>(p * lat_ms.size()))]; };
      std::printf("#lr_repeat\tcommits=%zu\tspan_s=%.3f\tp50_ms=%.1f\tp99_ms=%.1f\tmax_ms=%.1f\n",
                  lat_ms.size(), last_end_ms / 1000.0, q(0.5), q(0.99), lat_ms.back());
      std::fflush(stdout);
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}

}  // namespace tuskflow
