#pragma once

// NoLocksWithReservationImpl of the original: the epoch protocol of paper
// Algorithm 1.  Reservations go to the shared ReservationTable; the commit
// decision is HasConflicts over the transaction's read and write sets.

#include <cstdint>
#include <vector>

#include "protocol/protocol.hh"
#include "protocol/reservation_table.hh"

namespace tuskflow {

class ReservationProtocol final : public Protocol {
public:
  explicit ReservationProtocol(ReservationTable* table) : table_(table) {}

  void start_client(int64_t tid, int epoch) override {
    tid_ = tid;
    epoch_ = epoch;
  }
  // Alg.1 l.13-14.
  void reserve_read(std::vector<RWKey*>& rs, RWKey* key, ResourceType type,
                    int64_t id) override {
    *key = RWKey{epoch_, id, type, nullptr, nullptr};
    rs.push_back(key);
    table_->merge_read(type, id, static_cast<int>(tid_), epoch_);
  }
  // Alg.1 l.15-16.
  void reserve_write(std::vector<RWKey*>& ws, RWKey* key, ResourceType type,
                     int64_t id) override {
    *key = RWKey{epoch_, id, type, nullptr, nullptr};
    ws.push_back(key);
    // `hasReserved |= ...` starts at true, so it never becomes false (kept).
    has_reserved_ |= table_->merge_write(type, id, static_cast<int>(tid_), epoch_);
  }
  // Alg.1 l.17-18.
  void reserve_mammoth_read(std::vector<RWKey*>& rs, RWKey* key,
                            ResourceType type, int64_t id,
                            LongRunningState state) override {
    *key = RWKey{epoch_, id, type, nullptr, nullptr};
    rs.push_back(key);
    table_->merge_mammoth_read(type, id, state, epoch_);
  }
  void mark_mammoth_write(ResourceType type, int64_t id, LongRunningState state) override {
    table_->merge_mammoth_write(type, id, state, epoch_);
  }
  // Alg.1 l.19-20.
  void reserve_mammoth_write(std::vector<RWKey*>& ws, RWKey* key,
                             ResourceType type, int64_t id,
                             LongRunningState state) override {
    *key = RWKey{epoch_, id, type, nullptr, nullptr};
    ws.push_back(key);
    table_->merge_mammoth_write(type, id, state, epoch_);
  }
  // Alg.1 l.26-37: reads first, then writes, one entry at a time
  // (ReservationTable::check_entry); `state_` carries seenVisited /
  // seenUnvisited (l.27-28) across the entries.
  bool has_conflicts(const std::vector<RWKey*>& rs,
                     const std::vector<RWKey*>& ws) override {
    if (!has_reserved_) return true;
    for (const RWKey* r : rs)
      if (table_->check_entry(r->type, r->id, tid_, epoch_, state_, true))
        return true;
    for (const RWKey* w : ws)
      if (table_->check_entry(w->type, w->id, tid_, epoch_, state_, false))
        return true;
    return false;
  }
  void clear() override {
    state_.reset();
    has_reserved_ = true;
  }

private:
  ReservationTable* table_;
  ReservationState state_;
  int epoch_ = 0;
  int64_t tid_ = -1;
  bool has_reserved_ = true;
};

}  // namespace tuskflow
