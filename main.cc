// TuskFlow, C++ port of the original Java prototype for the random-graph
// and LDBC workloads.
// This file is PrototypeBenchmark: it loads the graph, starts the database
// (manager, workers, monitor) and the clients, runs the experiment and prints
// the results.  The rest follows the original class by class; see README_ja.md
// for the map from paper Algorithm 1 and from the Java classes to the files.

#include <gflags/gflags.h>
#include <x86intrin.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "config.hh"
#include "monitor/counters.hh"
#include "monitor/metrics.hh"
#include "monitor/report.hh"
#include "monitor/ser_log.hh"
#include "protocol/forseti.hh"
#include "protocol/reservation_table.hh"
#include "runtime/database.hh"
#include "runtime/query_manager.hh"
#include "runtime/worker.hh"
#include "storage/graph_storage.hh"
#include "storage/bomb_loader.hh"
#include "storage/ldbc_loader.hh"
#include "storage/random_loader.hh"
#include "tx/bomb_tx.hh"
#include "tx/short_tx.hh"
#include "tx/task.hh"
#include "tx/transaction.hh"
#include "util/concurrency.hh"
#include "util/qsbr.hh"
#include "util/thread_random.hh"

namespace tuskflow {

int run_benchmark() {
  // Before loading: q2_prepare (main thread) already retires through Qsbr.
  Qsbr::set_fix_retire_pin(FLAGS_fix_qsbr_retire_pin);
  {
    auto w0 = std::chrono::steady_clock::now();
    uint64_t c0 = __rdtsc();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    uint64_t c1 = __rdtsc();
    double ns = std::chrono::duration<double, std::nano>(
                    std::chrono::steady_clock::now() - w0).count();
    g_tsc_ghz = static_cast<double>(c1 - c0) / ns;
  }
  set_tracking_threshold(static_cast<int>(FLAGS_tracking_threshold));
  const bool ldbc = FLAGS_dataset == "ldbc";
  const bool bomb = FLAGS_dataset == "bomb";
  if ((!ldbc && !bomb && FLAGS_dataset != "random") ||
      (ldbc && FLAGS_ldbc_path.empty()) ||
      (!ldbc && !bomb && (FLAGS_nodes < 2 || FLAGS_nodes > FLAGS_node_capacity ||
                 FLAGS_rels > FLAGS_rel_capacity)) ||
      FLAGS_hops < 1 || FLAGS_hops >= FLAGS_ops || FLAGS_read_ratio > 100) {
    std::fprintf(stderr, "invalid arguments\n");
    return 1;
  }
  const size_t workers = FLAGS_workers != 0
                             ? FLAGS_workers
                             : std::max(1u, std::thread::hardware_concurrency());
  PropertyStore::get_mapping(kPropertyKey);  // register names up front
  PropertyStore::get_mapping(kNewPropertyKey);
  PropertyStore::get_mapping(kLikesKey);
  TaskPool::init();

  std::printf("#dataset:\t%s\n", FLAGS_dataset.c_str());
  if (ldbc)
    std::printf("#ldbc_path:\t%s\n", FLAGS_ldbc_path.c_str());
  else if (bomb)
    std::printf("#bomb:\tfactories %" PRIu64 " materials %" PRIu64 " products %" PRIu64
                " root_wips %" PRIu64 " trees/product %" PRIu64 " tree_size %" PRIu64
                " materials/wip %" PRIu64 " seed %" PRIu64 "\n#bomb_interactive_us:\t%" PRIu64
                "\n#bomb_l1_clients:\t%" PRIu64 "\n#bomb_l1_read_state:\t%s\n",
                FLAGS_bomb_factories, FLAGS_bomb_materials, FLAGS_bomb_products,
                FLAGS_bomb_root_wips, FLAGS_bomb_trees_per_product, FLAGS_bomb_tree_size,
                FLAGS_bomb_materials_per_wip, FLAGS_bomb_seed, FLAGS_bomb_interactive_us,
                FLAGS_bomb_l1_clients, FLAGS_bomb_l1_read_state.c_str());
  else
    std::printf("#nodes:\t%" PRIu64 "\n#rels:\t%" PRIu64 "\n", FLAGS_nodes,
                FLAGS_rels);
  std::printf("#workers:\t%zu\n"
              "#clients:\t%" PRIu64 "\n#client_tx_per_sec:\t%" PRIu64 "\n"
              "#read_ratio:\t%" PRIu64 "\n#ops:\t%" PRIu64 "\n#hops:\t%" PRIu64
              "\n#use_zipf:\t%d\n#epoch_size:\t%" PRIu64 "\n#start_lr:\t%d\n"
              "#lr_budget:\t%" PRIu64 "\n#lr_delay_ms:\t%" PRIu64
              "\n#duration_ms:\t%" PRIu64 "\n#reordering:\t%d\n"
              "#tag_optimization:\t%d\n#track_metadata:\t%d\n"
              "#tracking_threshold:\t%" PRIu64 "\n#fix_getnext_race:\t%d\n"
              "#fix_mode_switch_race:\t%d\n#client_paced:\t%d\n",
              workers, FLAGS_clients,
              FLAGS_client_tx_per_sec, FLAGS_read_ratio, FLAGS_ops, FLAGS_hops,
              FLAGS_use_zipf, FLAGS_epoch_size, FLAGS_start_lr, FLAGS_lr_budget,
              FLAGS_lr_delay_ms, FLAGS_duration_ms, FLAGS_reordering,
              FLAGS_tag_optimization, FLAGS_track_metadata,
              FLAGS_tracking_threshold, FLAGS_fix_getnext_race,
              FLAGS_fix_mode_switch_race, FLAGS_client_paced);
  std::printf("#lr_kind:\t%s\n#r2_friends:\t%" PRIu64 "\n#lr_repeat:\t%d\n#lr_clients:\t%" PRIu64
              "\n#r2_source_only:\t%d\n#r2_own_persons:\t%d\n#q2_countries:\t%s\n#q2_path:\t%" PRIu64 "-%" PRIu64
              "\n#q2_hub_factor:\t%" PRIu64 "\n#q2_stmt_delay_us:\t%" PRIu64 "\n#q2_all_countries:\t%d\n#q2_hub:\t%ld\n",
              FLAGS_lr_kind.c_str(), FLAGS_r2_friends, FLAGS_lr_repeat, FLAGS_lr_clients,
              FLAGS_r2_source_only, FLAGS_r2_own_persons, FLAGS_q2_countries.c_str(), FLAGS_q2_min_path,
              FLAGS_q2_max_path, FLAGS_q2_hub_factor, FLAGS_q2_stmt_delay_us, FLAGS_q2_all_countries,
              static_cast<long>(FLAGS_q2_hub));
  std::printf("#fix_qsbr_retire_pin:\t%d\n", FLAGS_fix_qsbr_retire_pin);
  std::fflush(stdout);

  // Database(): storage, reservation table, workers.
  auto* storage = new GraphStorage(static_cast<uint32_t>(FLAGS_node_capacity),
                                   static_cast<uint32_t>(FLAGS_rel_capacity),
                                   FLAGS_track_metadata,
                                   static_cast<int>(FLAGS_tracking_threshold));
  auto* table = new ReservationTable();
  if (FLAGS_reordering) table->enable_reordering();
  std::printf("[DBG] Starting %s reservation table (%s tagging).\n",
              FLAGS_reordering ? "an optimized" : "a regular",
              FLAGS_tag_optimization ? "with" : "without");

  // BenchmarkUtils.populateDatabase with RandomLoader or LdbcLoader.
  auto load_t0 = std::chrono::steady_clock::now();
  uint32_t n = static_cast<uint32_t>(FLAGS_nodes);
  std::vector<int32_t> mappings;
  LdbcGraph ldbc_graph;
  BombGraph bomb_graph;
  if (bomb) {
    bomb_graph = load_bomb_graph(storage);
    n = bomb_graph.number_of_nodes;
    std::printf("[DBG] BoMB: L1 reads %" PRIu64 " leaf material costs (%" PRIu64
                " distinct materials) and writes %u product costs per factory\n",
                bomb_graph.leaf_reads, bomb_graph.distinct_materials, bomb_graph.products);
  } else if (ldbc) {
    ldbc_graph = load_ldbc_graph(storage, FLAGS_ldbc_path);
    n = ldbc_graph.number_of_nodes;
    if (FLAGS_lr_kind == "q2") q2_prepare(storage, &ldbc_graph);  // before the experiment, not in mammoth 1
  } else {
    mappings = load_random_graph(storage, n, FLAGS_rels, FLAGS_seed);
  }
  double load_ms = std::chrono::duration<double, std::milli>(
                       std::chrono::steady_clock::now() - load_t0)
                       .count();
  std::printf("[DBG] Dataset %s with %.3fM nodes and %.3fM relationships "
              "(avg degree %.1f) loaded in %.0f ms; RSS %.2f GB\n",
              bomb ? "BoMB" : ldbc ? "LDBC" : "Random", storage->node_count() / 1e6,
              storage->rel_count() / 1e6,
              storage->metadata().average_degree(), load_ms,
              rss_bytes() / 1e9);
  if (ldbc)
    std::printf("[DBG] LDBC: %u persons, %" PRIu64 " posts, %" PRIu64
                " comments\n", ldbc_graph.number_of_persons,
                ldbc_graph.number_of_posts, ldbc_graph.number_of_comments);
  std::fflush(stdout);

  if (!FLAGS_ser_log.empty()) {
    SerLog::enable();
    storage->enable_versions(n);
  }
  ShortTxConfig cfg{n, &mappings, static_cast<int>(FLAGS_ops),
                    static_cast<int>(FLAGS_hops), FLAGS_use_zipf};
  if (bomb) cfg.bomb = &bomb_graph;
  if (ldbc) {  // start from the Persons (personMappingsForZipf)
    cfg.number_of_nodes = ldbc_graph.number_of_persons;
    cfg.mappings = &ldbc_graph.person_mappings;
    cfg.ldbc = &ldbc_graph;
    cfg.first_node = static_cast<uint32_t>(ldbc_graph.range_first[kPersonType]);
  }
  std::atomic<bool> db_running{true};
  std::atomic<bool> bench_running{false};
  auto* locks = new ForsetiLockManager();
  QueryManager manager(table, locks, &db_running, FLAGS_epoch_size);
  Database db(storage, &manager, locks, cfg);
  g_execution_mode = manager.execution_mode();
  Metrics metrics(workers);

  // Database.start(): manager, workers, monitor.
  std::vector<std::thread> threads;  // clients (detached at exit)
  std::vector<std::thread> worker_threads;
  std::thread manager_thread([&] { manager.run(); });
  for (size_t w = 0; w < workers; ++w) {
    worker_threads.emplace_back(worker_main, w, std::ref(manager),
                                std::ref(metrics), std::ref(db_running));
  }
  std::thread monitor_thread([&] {  // MetricsMonitor (1 s period) + memory
    uint64_t prev = 0;
    auto prev_t = std::chrono::steady_clock::now();
    while (db_running.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      auto t = std::chrono::steady_clock::now();
      uint64_t c = metrics.total_committed();
      double dt = std::chrono::duration<double>(t - prev_t).count();
      std::printf("[MON] %10.2f TX/s [c %" PRIu64 " a %" PRIu64 "] [%s] "
                  "[mode %d epoch %d lr %d] [locks %" PRId64 "/%" PRId64
                  "/%" PRId64 " wait %" PRIu64 " dl %" PRIu64 "] [gc %" PRIu64
                  "] [rt %" PRId64 "/%" PRId64 "/%" PRId64
                  " acc %" PRId64 "] [tp %d] [rss %.2f GB]\n",
                  (c - prev) / dt, c, g_aborts.v.load(),
                  manager.queues_string().c_str(), manager.mode(),
                  manager.epoch(), manager.lr_active() ? 1 : 0, locks->locks(0),
                  locks->locks(1), locks->locks(2), g_lock_stats.waits.load(),
                  g_lock_stats.deadlocks.load(), Qsbr::instance().pending(),
                  table->entries(ResourceType::NODE),
                  table->entries(ResourceType::RELATIONSHIP),
                  table->entries(ResourceType::NODE_RELATIONSHIPS),
                  storage->metadata().accesses().size(),
                  TaskPool::count().load(), rss_bytes() / 1e9);
      std::fflush(stdout);
      prev = c;
      prev_t = t;
    }
  });

  // PrototypeBenchmark: clients, long-running client, timed experiment.
  for (uint64_t i = 0; i < FLAGS_clients; ++i)
    threads.emplace_back(client_main, &db, &bench_running,
                         static_cast<int>(FLAGS_read_ratio),
                         FLAGS_client_tx_per_sec);
  if (FLAGS_start_lr)
    for (uint64_t i = 0; i < (bomb ? FLAGS_bomb_l1_clients : FLAGS_lr_clients); ++i)
      threads.emplace_back(long_running_client_main, &db, &bench_running,
                           FLAGS_lr_delay_ms);
  std::printf("[DBG] Starting the experiment.\n");
  std::fflush(stdout);
  auto exp_t0 = std::chrono::steady_clock::now();
  double exp_t0_ms = now_millis();
  bench_running.store(true);
  for (;;) {  // waitForExperiment busy-waits in the original
    auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - exp_t0)
                  .count();
    if (static_cast<uint64_t>(dt) >= FLAGS_duration_ms) break;
  }
  std::printf("[DBG] Stopping the experiment.\n");
  bench_running.store(false);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));  // waitForClients
  double exp_end_ms = now_millis();
  // The original prints while its workers keep running.  Stop them first so
  // the per-worker logs can be read safely.
  db_running.store(false);
  for (auto& t : worker_threads) t.join();
  manager_thread.join();
  manager.join_background();
  if (SerLog::on()) {
    size_t words = SerLog::dump(FLAGS_ser_log);
    std::printf("ser_log_words:\t%zu\n", words);
  }
  monitor_thread.join();

  // printLatencyMetrics + our summary.
  std::vector<LatencySample> samples = metrics.all_latencies();
  std::vector<double> all;
  all.reserve(samples.size());
  for (const auto& s : samples) all.push_back(s.latency_ms);
  print_percentiles(all, "Query Latency Percentiles");
  print_percentiles(manager.phase_latency(0), "Phase 1 Latency Percentiles");
  print_percentiles(manager.phase_latency(1), "Phase 2 Latency Percentiles");

  const double lr_s = manager.lr_start_ms(), lr_e = manager.lr_end_ms();
  std::printf("committed:\t%" PRIu64 "\n", metrics.total_committed());
  std::printf("aborts:\t%" PRIu64 "\n", g_aborts.v.load());
  std::printf("throughput[tps]:\t%.1f\n",
              metrics.total_committed() / ((exp_end_ms - exp_t0_ms) / 1000.0));
  if (bomb) {  // per type, as ccbench's BoMB Summary (commits,aborts of L1, S1, S2)
    const double min = FLAGS_duration_ms / 60000.0;
    const char* names[kBombTypes] = {"S1", "S2", "L1"};
    for (int t = 0; t < kBombTypes; ++t)
      std::printf("#bomb_tx\t%s\tcommits=%" PRIu64 "\tper_min=%.1f\tepoch_aborts=%" PRIu64
                  "\tp50_ms=%.3f\tp99_ms=%.3f\tp999_ms=%.3f\n", names[t],
                  g_bomb.commits[t].load(), g_bomb.commits[t].load() / min, g_bomb.aborts[t].load(),
                  g_bomb.percentile(static_cast<BombType>(t), 0.5),
                  g_bomb.percentile(static_cast<BombType>(t), 0.99),
                  g_bomb.percentile(static_cast<BombType>(t), 0.999));
    std::printf("#bomb_summary\tL1/min=%.1f\tshort/min=%.1f\tS1/min=%.1f\tS2/min=%.1f\tjv_rows=%" PRIu64 "\n",
                g_bomb.commits[kBombL1].load() / min,
                (g_bomb.commits[kBombS1].load() + g_bomb.commits[kBombS2].load()) / min,
                g_bomb.commits[kBombS1].load() / min, g_bomb.commits[kBombS2].load() / min,
                g_bomb.jv_rows.load());
  }
  if (lr_e > 0) {
    std::printf("mammoth_start_s:\t%.3f\nmammoth_end_s:\t%.3f\n"
                "mammoth_duration_s:\t%.3f\nmammoth_epochs:\t%d\n",
                (lr_s - exp_t0_ms) / 1000.0, (lr_e - exp_t0_ms) / 1000.0,
                (lr_e - lr_s) / 1000.0,
                manager.lr_end_epoch() - manager.lr_start_epoch() + 1);
    std::printf("mammoth_phase1_ms:\t%.1f\nmammoth_phase2_ms:\t%.1f\n"
                "reservation_clear_ms:\t%.1f\n",
                g_lr_phase1_ns.v.load() / 1e6, g_lr_phase2_ns.v.load() / 1e6,
                manager.clear_ms());
    // Short transactions that committed while the mammoth ran vs. the rest.
    std::vector<double> in_w, out_w;
    for (const auto& s : samples)
      ((s.end_ms >= lr_s && s.end_ms <= lr_e) ? in_w : out_w)
          .push_back(s.latency_ms);
    std::printf("window_mammoth_commits:\t%zu\nwindow_mammoth_tps:\t%.1f\n",
                in_w.size(), in_w.size() / ((lr_e - lr_s) / 1000.0));
    print_percentiles(in_w, "Mammoth-window Latency Percentiles");
    print_percentiles(out_w, "Outside-window Latency Percentiles");
  } else if (lr_s > 0) {
    std::printf("mammoth_start_s:\t%.3f\nmammoth_end_s:\tnot finished\n",
                (lr_s - exp_t0_ms) / 1000.0);
  }
  std::printf("reservation_entries_node:\t%" PRId64 "\n"
              "reservation_entries_rel:\t%" PRId64 "\n"
              "reservation_entries_nbr:\t%" PRId64 "\n"
              "reservation_bins_node:\t%zu\nreservation_resizes:\t%" PRIu64 "\n"
              "reservation_resize_ms:\t%.1f\nreservation_bytes_approx:\t%zu\n"
              "metadata_access_entries:\t%" PRId64 "\n"
              "metadata_access_resizes:\t%" PRIu64 "\n",
              table->entries(ResourceType::NODE),
              table->entries(ResourceType::RELATIONSHIP),
              table->entries(ResourceType::NODE_RELATIONSHIPS),
              table->bins(ResourceType::NODE), table->resizes(),
              table->resize_ns() / 1e6, table->approx_bytes(),
              storage->metadata().accesses().size(),
              storage->metadata().accesses().resizes());
  std::printf("diag_overflow_resubmit:\t%" PRIu64 "\n"
              "diag_stale_poll:\t%" PRIu64 "\n"
              "diag_commit_outside_phase2:\t%" PRIu64 "\n"
              "diag_local_outside_phase1:\t%" PRIu64 "\n"
              "diag_put_back:\t%" PRIu64 "\n",
              g_diag_overflow_resubmit.v.load(), g_diag_stale_poll.v.load(),
              g_diag_commit_outside_p2.v.load(),
              g_diag_local_outside_p1.v.load(), g_diag_put_back.v.load());
  std::printf("commits_2pl:\t%" PRIu64 "\n2pl_lock_waits:\t%" PRIu64 "\n"
              "2pl_deadlocks:\t%" PRIu64 "\n2pl_deadlock_retries:\t%" PRIu64
              "\n2pl_failed:\t%" PRIu64 "\n2pl_upgrades:\t%" PRIu64 "\n"
              "diag_2pl_converted:\t%" PRIu64 "\n"
              "diag_2pl_in_epoch_slot:\t%" PRIu64 "\n"
              "diag_2pl_overlap_epoch:\t%" PRIu64 "\n"
              "diag_2pl_inflight_at_switch:\t%" PRIu64 "\n",
              g_commits_2pl.v.load(), g_lock_stats.waits.load(),
              g_lock_stats.deadlocks.load(), g_2pl_deadlock_retries.v.load(),
              g_2pl_failed.v.load(), g_lock_stats.upgrades.load(),
              g_diag_2pl_converted.v.load(), g_diag_2pl_in_epoch.v.load(),
              g_diag_2pl_overlap.v.load(), g_diag_inflight_at_switch.v.load());
  if (FLAGS_lr_breakdown) {
    double ghz = g_tsc_ghz;
    std::printf("lr_ms_reserve:\t%.1f\nlr_ms_get_node:\t%.1f\n"
                "lr_ms_copy:\t%.1f\nlr_ms_add_property_ws:\t%.1f\n",
                g_lr_cyc_reserve.v.load() / ghz / 1e6,
                g_lr_cyc_get.v.load() / ghz / 1e6,
                g_lr_cyc_copy.v.load() / ghz / 1e6,
                g_lr_cyc_rest.v.load() / ghz / 1e6);
  }
  std::printf("lr_ms_merge:\t%.1f\nlr_ms_free:\t%.1f\n",
              g_lr_cyc_merge.v.load() / g_tsc_ghz / 1e6,
              g_lr_cyc_free.v.load() / g_tsc_ghz / 1e6);
  std::printf("rss_end_gb:\t%.2f\npeak_rss_gb:\t%.2f\n", rss_bytes() / 1e9,
              peak_rss_bytes() / 1e9);

  if (!FLAGS_epoch_log.empty()) {
    std::ofstream out(FLAGS_epoch_log);
    out << "epoch\tt_s\tcount\tphase1_ms\tphase2_ms\tlr\tretries\n";
    for (const auto& e : manager.epoch_log())
      out << e.epoch << '\t' << (e.t_ms - exp_t0_ms) / 1000.0 << '\t'
          << e.count << '\t' << e.phase1_ms << '\t' << e.phase2_ms << '\t'
          << (e.lr_active ? 1 : 0) << '\t' << e.retries << '\n';
  }
  std::fflush(stdout);
  std::_Exit(0);  // System.exit(0): skip tearing down the graph
}

}  // namespace tuskflow

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("TuskFlow, C++ port of the Java prototype");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return tuskflow::run_benchmark();
}
