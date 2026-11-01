#include "runtime/query_manager.hh"

#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>

#include "config.hh"
#include "monitor/counters.hh"
#include "protocol/reservation_protocol.hh"
#include "protocol/two_pl_protocol.hh"
#include "tx/mammoth_tx.hh"
#include "tx/transaction.hh"

namespace tuskflow {

QueryManager::QueryManager(ReservationTable* table, ForsetiLockManager* locks,
                           std::atomic<bool>* running, uint64_t epoch_size)
    : table_(table), locks_(locks), running_(running),
      batch_size_(static_cast<int>(epoch_size)) {}

void QueryManager::join_background() {
  if (clear_thread_.joinable()) clear_thread_.join();
}

// Database.submitQuery -> queryManager.submit.
void QueryManager::submit(Task* task) {
  // Clients wait while the manager switches back to regular execution.
  while (!set_queries_.load(std::memory_order_acquire)) park_nanos_1();
  if (task->long_running) {
    long_running_queries_.add(task);
    int regular = 0;
    execution_mode_.compare_exchange_strong(regular, 1);
  } else if (execution_mode_.load(std::memory_order_acquire) != 0) {
    change_execution_mode(task, epoch_.load());
    pending_queries_.add(task);
  } else {
    running_queries_.add(task);
  }
}

// getNext(): a worker takes the next task of the current phase.
Task* QueryManager::get_next() {
  Task* task = nullptr;
  int epoch_before = epoch_.load(std::memory_order_seq_cst);
  int mode = execution_mode_.load(std::memory_order_seq_cst);
  if (mode != -1) {
    ConcurrentQueue<Task>& queue =
        (mode != 2) ? running_queries_ : running_queries2_;
    task = queue.poll();
    // Within one epoch the modes 1 and 2 each hold for one contiguous
    // interval, so the same (epoch, mode) before and after the poll means
    // the poll happened in that phase.
    if (task != nullptr &&
        (epoch_.load(std::memory_order_seq_cst) != epoch_before ||
         execution_mode_.load(std::memory_order_seq_cst) != mode)) {
      g_diag_stale_poll.v.fetch_add(1, std::memory_order_relaxed);
      if (FLAGS_fix_getnext_race) {
        queue.add(task);  // leave it for a worker in the right phase
        g_diag_put_back.v.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
      }
    }
    if (task != nullptr && mode == 1 && FLAGS_fix_mode_switch_race &&
        task->tx->is_two_pl()) {
      // Port fix #8 (2026-10-30): a 2PL task queued before the switch to
      // epochs goes to the pending queue without touching the epoch's work
      // counter.  Taking a slot and giving it back (below) let a concurrent
      // poll of the mammoth see the counter below zero and abort the process
      // ("All long-running tasks must have been scheduled"); one mammoth back
      // to back with one-shot slices switches modes hundreds of times a second.
      g_diag_2pl_converted.v.fetch_add(1, std::memory_order_relaxed);
      g_diag_2pl_in_epoch.v.fetch_add(1, std::memory_order_relaxed);
      submit(task);
      return nullptr;
    }
    if (task != nullptr && mode == 1) {
      task->tx->set_epoch(epoch_.load());
      int new_val = work_counter_.fetch_sub(1) - 1;
      if (new_val < 0) {
        if (task->tx->is_two_pl()) {
          // Expected while switching to epochs: queued 2PL tasks move to
          // the pending queue in epoch mode.
          g_diag_2pl_converted.v.fetch_add(1, std::memory_order_relaxed);
        } else {
          g_diag_overflow_resubmit.v.fetch_add(1, std::memory_order_relaxed);
          std::printf("[DIAG] epoch %d: workCounter %d after poll in mode 1; "
                      "task %d re-submitted to pending [%s]\n",
                      epoch_.load(), new_val, task->task_id,
                      queues_string().c_str());
        }
        if (task->long_running) {
          std::fprintf(stderr,
                       "All long-running tasks must have been scheduled\n");
          std::abort();
        }
        submit(task);
        task = nullptr;
      } else if (task->tx->is_two_pl()) {
        // A 2PL task submitted before the switch took an epoch slot.
        g_diag_2pl_converted.v.fetch_add(1, std::memory_order_relaxed);
        g_diag_2pl_in_epoch.v.fetch_add(1, std::memory_order_relaxed);
        if (FLAGS_fix_mode_switch_race) work_counter_.fetch_add(1);
        submit(task);
        task = nullptr;
      }
    }
    if (task != nullptr && mode != 0 && task->tx->is_two_pl()) {
      std::fprintf(stderr, "[DBG] Wrong protocol with mode %d\n", mode);
      std::abort();
    }
  }
  return task;
}

// LongRunningQueryManager.run(): spins until a mammoth arrives.
void QueryManager::run() {
  while (running_->load()) {
    if (!long_running_queries_.empty()) {
      schedule_in_epochs();
      reset_execution_mode();
    }
  }
}

std::string QueryManager::queues_string() const {
  char buf[160];
  std::snprintf(buf, sizeof(buf), "p %3zu r %3zu _r %3zu lr %3zu",
                pending_queries_.size(), running_queries_.size(),
                running_queries2_.size(), long_running_queries_.size());
  return buf;
}

EpochContext QueryManager::context() {
  return EpochContext{&running_queries_, &running_queries2_,
                      &aborted_counter_, &phase_one_counter_,
                      &phase_two_counter_};
}

// changeExecutionMode: the task switches to the epoch protocol, starting
// with the execution phase (Alg.1 EXECUTE_LOCALLY).
void QueryManager::change_execution_mode(Task* task, int epoch) {
  task->tx->set_transaction_type(TxType::EXECUTE_LOCALLY,
                                 new ReservationProtocol(table_), context());
  task->tx->set_epoch(epoch);
  task->tx->set_start_time(task->start_time_ms);
}

// scheduleLongRunningTask / scheduleSingleLongRunningTask.
void QueryManager::schedule_long_running_task() {
  // --lr_repeat: the previous mammoth's table clear (not awaited) must finish
  // before this mammoth or any epoch transaction reserves.  No-op for one mammoth.
  join_background();
  Task* lr = long_running_queries_.poll();
  auto* lr_tx = static_cast<AllNodesLongRunningTx*>(lr->tx);
  if (FLAGS_tag_optimization) {  // setupReservationTable
    table_->add_resource_updated(ResourceType::NODE);  // List.of(NODE)
    if (const LdbcGraph* g = lr_tx->ldbc())  // List.of(messageIds)
      table_->set_label_range(g->range_first[kPostType],
                              g->range_last[kCommentType]);
  }
  has_long_running_finished_.store(1);
  change_execution_mode(lr, epoch_.load());
  lr_tx->set_variable_for_completion(&has_long_running_finished_);
  aborted_counter_.fetch_add(1);
  running_queries_.add(lr);
  lr_active_.store(true);
  lr_start_ms_.store(now_millis());
  lr_start_epoch_.store(epoch_.load());
  std::printf("[DBG] Starting long-running tx %d on epoch %d [%s]\n",
              lr->task_id, epoch_.load(), queues_string().c_str());
  std::fflush(stdout);
}

void QueryManager::check_long_running_tasks() {
  bool found = false;
  // A worker that polled with a stale (epoch, mode) holds the mammoth briefly
  // before putting it back (get_next, --fix_getnext_race); with the very short
  // epochs of an interactive mammoth that waits for its client, the check can
  // fall into that window.  Wait for the put-back before giving up.
  auto t0 = std::chrono::steady_clock::now();
  for (;;) {
    running_queries_.for_each([&](Task* t) {
      if (t->long_running) found = true;
      return !found;
    });
    if (found || std::chrono::steady_clock::now() - t0 > std::chrono::milliseconds(100)) break;
    park_nanos_1();
  }
  if (!found) {
    std::fprintf(stderr, "Not all expected long-running txs are present\n");
    std::abort();
  }
}

// counter.await(10, SECONDS); also returns at shutdown, when the workers
// that would count down are gone.
bool QueryManager::await_phase(CountDownLatch& counter) {
  for (int i = 0; i < 100; ++i) {
    if (counter.await_for(std::chrono::milliseconds(100))) return true;
    if (!running_->load()) return true;
  }
  return false;
}

// scheduleNextEpoch: the aborted tasks (and the mammoth) of the last epoch
// are already in the running queue; fill up to EPOCH_SIZE from pending.
int QueryManager::schedule_next_epoch() {
  int count = aborted_counter_.load();
  if (lr_active_) check_long_running_tasks();
  while (!pending_queries_.empty() && count < batch_size_) {
    Task* t = pending_queries_.poll();
    if (t == nullptr) break;
    running_queries_.add(t);
    ++count;
  }
  return count;
}

// scheduleInEpochs()
void QueryManager::schedule_in_epochs() {
  while (!long_running_queries_.empty()) {
    while (!running_queries_.empty()) {  // queued 2PL tasks move to pending
      if (!running_->load()) return;
      park_nanos_1();
    }
    execution_mode_.store(-1, std::memory_order_seq_cst);
    // The original does not wait for 2PL transactions still running.
    uint64_t inflight = g_inflight_2pl.v.load();
    g_diag_inflight_at_switch.v.fetch_add(inflight);
    if (FLAGS_fix_mode_switch_race)
      while (g_inflight_2pl.v.load() != 0) park_nanos_1();
    schedule_long_running_task();
    while (has_long_running_finished_.load() != 0 && running_->load())
      run_epoch();
    lr_end_ms_.store(now_millis());
    lr_end_epoch_.store(epoch_.load() - 1);
    std::printf("[DBG] Long-running tx finished at epoch %d (2PL txs running "
                "at the switch: %" PRIu64 ")\n", lr_end_epoch_.load(),
                inflight);
    std::fflush(stdout);
    // --lr_repeat: every mammoth goes back through reset_execution_mode, as a
    // single mammoth does.  Scheduling the next one directly would leave the
    // last epoch's aborted tasks in running_queries_ (the drain above waits
    // for them forever) and the previous mammoth's states in the table.
    if (FLAGS_lr_repeat) break;
  }
}

void QueryManager::convert_to_two_pl(Task* t) {
  t->tx->set_transaction_type(TxType::EXECUTE_AND_COMMIT,
                              new TwoPLProtocol(locks_), EpochContext{});
}

// resetExecutionMode()
void QueryManager::reset_execution_mode() {
  std::printf("[DBG] Resetting remaining queries for normal execution [%s]\n",
              queues_string().c_str());
  set_queries_.store(false, std::memory_order_seq_cst);
  running_queries_.for_each([&](Task* t) {
    convert_to_two_pl(t);
    return true;
  });
  // --lr_repeat: the tasks aborted in the last epoch's phase 2 now run under
  // 2PL, so they must not inflate the next mammoth's first epoch count (that
  // epoch would wait for tasks that never come).  No-op for one mammoth.
  aborted_counter_.store(0);
  // scheduleParallelResetTasks: CORES - 1 = 3 threads move pending tasks.
  std::vector<std::thread> movers;
  for (int i = 0; i < 3; ++i)
    movers.emplace_back([this] {
      while (!pending_queries_.empty()) {
        Task* t = pending_queries_.poll();
        if (t == nullptr) continue;
        convert_to_two_pl(t);
        running_queries_.add(t);
      }
    });
  for (auto& m : movers) m.join();
  // executor.execute(() -> reservationTable.clear()): not awaited.
  join_background();  // --lr_repeat: a joinable thread must not be reassigned
  clear_thread_ = std::thread([this] {
    double c0 = now_millis();
    table_->clear();
    clear_ms_.store(now_millis() - c0);
  });
  lr_active_.store(false);
  execution_mode_.store(0, std::memory_order_seq_cst);
  set_queries_.store(true, std::memory_order_seq_cst);
}

// One epoch (the body of scheduleInEpochs' loop): Phase 1 runs
// RunLocally for every scheduled task (Alg.1 l.2-5), Phase 2 runs
// TryToRunOnDatabase (l.6-9); a count-down barrier closes each phase.
void QueryManager::run_epoch() {
  double t0 = now_millis();
  std::atomic_thread_fence(std::memory_order_seq_cst);
  int retries = aborted_counter_.load();
  int count = schedule_next_epoch();
  aborted_counter_.store(0);
  work_counter_.store(count);
  phase_one_counter_.reset(count);
  phase_two_counter_.reset(count);
  execution_mode_.store(1, std::memory_order_release);
  if (!await_phase(phase_one_counter_))
    std::printf("[Warning] Phase 1 of epoch %d  %d/%d tasks did not complete "
                "[%s]\n", epoch_.load(), phase_one_counter_.count(), count,
                queues_string().c_str());
  double t1 = now_millis();
  execution_mode_.store(2, std::memory_order_release);
  if (!await_phase(phase_two_counter_))
    std::printf("[Warning] Phase 2 of epoch %d  %d/%d tasks did not complete "
                "[%s]\n", epoch_.load(), phase_two_counter_.count(), count,
                queues_string().c_str());
  execution_mode_.store(-1, std::memory_order_release);
  double t2 = now_millis();
  bool lr = lr_active_.load();
  if (count > 0) {
    phase_lat_[0].push_back(t1 - t0);
    phase_lat_[1].push_back(t2 - t1);
    epoch_log_.push_back(
        {epoch_.load(), t0, count, t1 - t0, t2 - t1, lr, retries});
  }
  epoch_.fetch_add(1);
}

}  // namespace tuskflow
