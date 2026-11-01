#pragma once

// ConcurrencyProtocol of the original (tasks/protocol/), the interface a
// transaction uses to reserve (or lock) what it reads and writes:
//   protocol/reservation_protocol.hh  NoLocksWithReservationImpl (epochs)
//   protocol/two_pl_protocol.hh       TwoPLImpl (2PL, outside the mammoth)
// Method names follow paper Algorithm 1; the Java names are in comments.

#include <cstdint>
#include <vector>

#include "protocol/reservation_table.hh"
#include "storage/entities.hh"
#include "storage/graph_storage.hh"

namespace tuskflow {

// RWKey: one read or write of a transaction.  For a write, newVal is the
// value to install (epochs) and oldVal the value to restore on abort (2PL).
struct RWKey {
  int epoch;
  int64_t id;
  ResourceType type;
  Node* oldVal;
  Node* newVal;
};

class Protocol {
public:
  virtual ~Protocol() = default;
  virtual void start_client(int64_t tid, int epoch) = 0;
  virtual void stop_client() {}
  virtual void abort(std::vector<RWKey*>&, GraphStorage*) {}
  // Alg.1 l.13 ReserveRead (Java prepareRead).  Takes ownership of `key`.
  virtual void reserve_read(std::vector<RWKey*>& rs, RWKey* key,
                            ResourceType type, int64_t id) = 0;
  // Alg.1 l.15 ReserveWrite (Java prepareUpdate).  Takes ownership of `key`.
  virtual void reserve_write(std::vector<RWKey*>& ws, RWKey* key,
                             ResourceType type, int64_t id) = 0;
  // Alg.1 l.17-18 ReserveMammothRead (only the r2 long transaction uses it).
  virtual void reserve_mammoth_read(std::vector<RWKey*>&, RWKey*, ResourceType,
                                    int64_t, LongRunningState) {}
  // Move the mammoth state of a record it wrote in an earlier epoch forward
  // (PENDING_WRITE -> VISITED once the write is installed), without writing it
  // again (only the BoMB L1 uses it).
  virtual void mark_mammoth_write(ResourceType, int64_t, LongRunningState) {}
  // Alg.1 l.19 ReserveMammothWrite (Java prepareLongRunningUpdate).
  virtual void reserve_mammoth_write(std::vector<RWKey*>& ws, RWKey* key,
                                     ResourceType type, int64_t id,
                                     LongRunningState state) = 0;
  // Alg.1 l.26-37 HasConflicts (Java detectConflicts).
  virtual bool has_conflicts(const std::vector<RWKey*>& rs,
                             const std::vector<RWKey*>& ws) = 0;
  virtual void clear() = 0;
  virtual bool is_two_pl() const { return false; }
  // Java updateLastWriteSetEntry.
  static void update_last_write_set_entry(std::vector<RWKey*>& ws, Node* old_val,
                                          Node* new_val) {
    ws.back()->oldVal = old_val;
    ws.back()->newVal = new_val;
  }
};

}  // namespace tuskflow
