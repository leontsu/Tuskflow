#pragma once

// Quiescent-state-based reclamation: stands in for the garbage collector for
// objects that other threads may still be reading after they were unlinked
// (hash-map nodes and tables, lock objects, lock clients).
//
// A thread registers lazily on its first retire() or quiescent().  Workers call
// quiescent() between tasks, a point where they hold no pointer into shared
// structures.  An object retired at era e is freed once every registered,
// online thread has announced an era > e.  Threads that never touch shared
// structures need not register; threads that register and then block for a
// long time call offline().
//
// --fix_qsbr_retire_pin (port fix, 2026-11-01; default off = the port as
// before): retire() also registers the calling thread, online at the current
// era.  A thread that is not a worker never calls quiescent() or offline(),
// so once it has retired one object its era stays where it was and no object
// retired after that is ever freed by anyone.  On LDBC Q2 this is the main
// thread: q2_prepare's tracked reads insert ~9.4M access-metadata entries and
// the resizes of that map retire the old nodes, so from the first second of
// the run nothing is freed; every worker's retired list grows by ~46 objects
// per 2PL transaction, and every quiescent() then rescans the whole list
// (reclaim), which is what slows the workers down.  With the fix a thread
// that does not report quiescent states is registered offline and is online
// only inside a Section (around its reads of a protected structure, the
// access-metadata update), so it can no longer pin the grace period, and its
// reads there are covered as a worker's are.

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <vector>

namespace tuskflow {

class Qsbr {
public:
  static Qsbr& instance() {
    static Qsbr q;
    return q;
  }

  // --fix_qsbr_retire_pin; set once in main(), before any other thread starts.
  static void set_fix_retire_pin(bool on) { fix_retire_pin_ = on; }
  static bool fix_retire_pin() { return fix_retire_pin_; }

  // Called by a thread at a point where it holds no shared pointer.
  void quiescent() {
    tl_reports_ = true;
    ThreadState& ts = state();
    slots_[ts.slot].era.store(global_.load(std::memory_order_acquire),
                              std::memory_order_release);
    if (ts.retired.size() >= kReclaimBatch) reclaim(ts);
  }

  // --fix_qsbr_retire_pin: a thread that does not report quiescent states
  // (anything but a worker) is online for the lifetime of a Section and
  // offline otherwise.  No-op for workers and when the fix is off.
  class Section {
  public:
    Section() : counted_(fix_retire_pin_ && !tl_reports_) {
      if (counted_ && tl_depth_++ == 0) instance().enter();
    }
    ~Section() {
      if (counted_ && --tl_depth_ == 0) instance().leave();
    }
    Section(const Section&) = delete;
    Section& operator=(const Section&) = delete;

  private:
    bool counted_;
  };
  // Marks the calling thread as not holding shared pointers until its next
  // quiescent() call.
  void offline() {
    slots_[state().slot].era.store(0, std::memory_order_release);
  }

  template <typename T>
  void retire(T* p) {
    if (p == nullptr) return;
    ThreadState& ts = state();
    uint64_t era = global_.fetch_add(1, std::memory_order_acq_rel);
    ts.retired.push_back(
        {era, p, [](void* q) { delete static_cast<T*>(q); }, false});
  }
  // For objects that stay reachable through a stale field after they are
  // retired (a lock client's `waiting_for_lock_` may still point to a lock or
  // to the client owning it until that client clears its wait state, which
  // it does before its next quiescent point).  After one grace period nobody
  // can newly reach the object; a second one covers threads that reached it
  // in between.
  template <typename T>
  void retire_twice(T* p) {
    if (p == nullptr) return;
    ThreadState& ts = state();
    uint64_t era = global_.fetch_add(1, std::memory_order_acq_rel);
    ts.retired.push_back(
        {era, p, [](void* q) { delete static_cast<T*>(q); }, true});
  }

  uint64_t pending() {  // retired, not yet freed (approximate)
    uint64_t n = 0;
    for (int i = 0; i < kMaxThreads; ++i)
      n += slots_[i].pending.load(std::memory_order_relaxed);
    return n;
  }

private:
  static constexpr int kMaxThreads = 1024;
  static constexpr size_t kReclaimBatch = 256;

  struct Retired {
    uint64_t era;
    void* p;
    void (*del)(void*);
    bool second_grace;  // still needs a second grace period
  };
  struct alignas(64) Slot {
    std::atomic<uint64_t> era{0};  // 0 = offline / unused
    std::atomic<uint64_t> pending{0};
  };
  struct ThreadState {
    int slot = -1;
    std::vector<Retired> retired;
    ~ThreadState() {
      if (slot >= 0) Qsbr::instance().slots_[slot].era.store(0);
      // Leftovers are leaked: they may still be referenced and the process
      // is about to end (threads only exit at shutdown).
    }
  };

  static inline bool fix_retire_pin_ = false;
  static inline thread_local bool tl_reports_ = false;  // has called quiescent()
  static inline thread_local int tl_depth_ = 0;         // Section nesting

  std::atomic<uint64_t> global_{1};
  std::atomic<int> next_slot_{0};
  Slot slots_[kMaxThreads];

  ThreadState& state() {
    thread_local ThreadState ts;
    if (ts.slot < 0) {
      ts.slot = next_slot_.fetch_add(1);
      if (ts.slot >= kMaxThreads) std::abort();
      // Original: online at the current era, also for a thread registered by
      // retire() that will never report a quiescent state.  Fix: offline; a
      // worker goes online in its first quiescent(), right after this, and
      // any other thread only inside a Section.
      slots_[ts.slot].era.store(fix_retire_pin_ ? 0 : global_.load(),
                                std::memory_order_release);
    }
    return ts;
  }

  // Section entry/exit (--fix_qsbr_retire_pin).  Coming online from offline
  // is a store followed by loads of shared pointers, so it needs a full
  // fence, paired with the one at the start of reclaim(): either a
  // reclaimer's slot scan sees this era, or this thread's reads see every
  // unlink that the reclaimer did before its scan (the object is then
  // unreachable).  An era E loaded here that is above an object's retire era
  // means the retire's fetch_add is visible, and with it the unlink before it.
  void enter() {
    ThreadState& ts = state();
    slots_[ts.slot].era.store(global_.load(std::memory_order_acquire),
                              std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);
  }
  void leave() {  // release: the Section's reads happen before any free
    slots_[state().slot].era.store(0, std::memory_order_release);
  }

  void reclaim(ThreadState& ts) {
    if (fix_retire_pin_) std::atomic_thread_fence(std::memory_order_seq_cst);
    uint64_t min_era = UINT64_MAX;
    int n = next_slot_.load(std::memory_order_acquire);
    for (int i = 0; i < n && i < kMaxThreads; ++i) {
      uint64_t e = slots_[i].era.load(std::memory_order_acquire);
      if (e != 0 && e < min_era) min_era = e;
    }
    size_t kept = 0;
    for (Retired& r : ts.retired) {
      if (r.era >= min_era) {
        ts.retired[kept++] = r;
      } else if (r.second_grace) {
        r.era = global_.fetch_add(1, std::memory_order_acq_rel);
        r.second_grace = false;
        ts.retired[kept++] = r;
      } else {
        r.del(r.p);
      }
    }
    ts.retired.resize(kept);
    slots_[ts.slot].pending.store(kept, std::memory_order_relaxed);
  }
};

}  // namespace tuskflow
