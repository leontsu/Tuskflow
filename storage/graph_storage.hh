#pragma once

// GraphStorage and MetadataStore of the original (database/storage/): arrays
// of pointers to the entities of storage/entities.hh, in/out relationship
// lists, and the access tracking used by the partitioners.

#include <atomic>
#include <cstdint>

#include "storage/entities.hh"
#include "util/chm.hh"
#include "util/thread_random.hh"  // track_access_draw

namespace tuskflow {

// ConcurrentBitSet (only set() is used on this path).
class ConcurrentBitSet {
public:
  explicit ConcurrentBitSet(uint32_t bits)
      : words_((bits + 63) / 64), data_(new std::atomic<uint64_t>[words_]) {
    for (size_t i = 0; i < words_; ++i)
      data_[i].store(0, std::memory_order_relaxed);
  }
  ~ConcurrentBitSet() { delete[] data_; }
  void set(uint32_t i) {
    data_[i / 64].fetch_or(uint64_t{1} << (i % 64), std::memory_order_relaxed);
  }

private:
  size_t words_;
  std::atomic<uint64_t>* data_;
};

// MetadataStore: degrees, and per-node access counts used by the
// partitioners.  Access counting stays on by default in the original
// (GraphStorage.createGraphStorage(true)), so it is part of its cost.
class MetadataStore {
public:
  explicit MetadataStore(uint32_t number_of_nodes)
      : degrees_len_(next_power_of_two(number_of_nodes)),
        degrees_(new std::atomic<int32_t>[degrees_len_]),
        accesses_() {
    for (uint32_t i = 0; i < degrees_len_; ++i)
      degrees_[i].store(0, std::memory_order_relaxed);
  }
  ~MetadataStore() { delete[] degrees_; }

  void add_relationship(int32_t from, int32_t to) {
    int32_t pf = degrees_[from].load(std::memory_order_relaxed);
    int32_t pt = degrees_[to].load(std::memory_order_relaxed);
    if (pf == 0) degree_count_.fetch_add(1, std::memory_order_relaxed);
    if (pt == 0) degree_count_.fetch_add(1, std::memory_order_relaxed);
    degree_sum_.fetch_add(2, std::memory_order_relaxed);
    degrees_[from].store(pf + 1, std::memory_order_relaxed);
    degrees_[to].store(pt + 1, std::memory_order_relaxed);
  }
  void access_node(int32_t node_id) {  // accesses.merge(id, 1, Integer::sum)
    // --fix_qsbr_retire_pin: the main thread (q2_prepare, loading) and the
    // long-running clients (mammoth constructors) reach this map too; they
    // are online only here.  No-op for workers and when the fix is off.
    Qsbr::Section qsbr_section;
    accesses_.merge(
        node_id, [] { return int32_t{1}; }, [](int32_t v) { return v + 1; });
  }
  double average_degree() const {
    return static_cast<double>(degree_sum_.load()) /
           static_cast<double>(degree_count_.load());
  }
  ConcurrentHashMap<int32_t, int32_t>& accesses() { return accesses_; }

private:
  uint32_t degrees_len_;
  std::atomic<int32_t>* degrees_;
  ConcurrentHashMap<int32_t, int32_t> accesses_;
  std::atomic<int64_t> degree_sum_{0};
  std::atomic<int64_t> degree_count_{0};
};


class GraphStorage {
public:
  GraphStorage(uint32_t node_capacity, uint32_t rel_capacity,
               bool track_metadata, int tracking_threshold)
      : nodes_(node_capacity), rels_(rel_capacity),
        in_relationships_(node_capacity), out_relationships_(node_capacity),
        metadata_(node_capacity), active_nodes_(node_capacity),
        track_metadata_(track_metadata),
        tracking_threshold_(tracking_threshold) {}

  // GraphStorage.update(entity) with the default per-call tracking draw.
  uint32_t update_node(Node& node) {
    return update_node(node, track_metadata_ && track_access_draw());
  }
  // Only nodes are written by this workload.  The stored node always exists
  // (all nodes are loaded up front), so updateNode reduces to merge.
  // Returns the node's new version when versions are enabled, else 0.
  uint32_t update_node(Node& node, bool track_access) {
    uint32_t node_id = static_cast<uint32_t>(node.id());
    Node* previous = nodes_.get(node_id);
    uint32_t v = previous->merge(node, version_ptr(node_id));
    if (track_metadata_ && track_access) metadata_.access_node(node_id);
    return v;
  }

  // Serializability log (not in the original): one version per
  // node, incremented by every physical write under the node's lock.
  void enable_versions(uint32_t n) {
    versions_ = new std::atomic<uint32_t>[n];
    for (uint32_t i = 0; i < n; ++i)
      versions_[i].store(0, std::memory_order_relaxed);
  }
  std::atomic<uint32_t>* version_ptr(uint32_t node_id) {
    return versions_ != nullptr ? &versions_[node_id] : nullptr;
  }
  uint32_t version(uint32_t node_id) const {
    return versions_ != nullptr
               ? versions_[node_id].load(std::memory_order_acquire)
               : 0;
  }

  Node* get_node(uint32_t node_id) {
    return get_node(node_id, track_metadata_ && track_access_draw());
  }
  Node* get_node(uint32_t node_id, bool track_access) {
    Node* node = nodes_.get(node_id);
    if (track_metadata_ && node != nullptr && track_access)
      metadata_.access_node(static_cast<int32_t>(node_id));
    return node;
  }

  Relationship* get_relationship(uint32_t rel_id) {
    return get_relationship(rel_id, track_metadata_ && track_access_draw());
  }
  Relationship* get_relationship(uint32_t rel_id, bool track_access) {
    Relationship* rel = rels_.get(rel_id);
    if (track_metadata_ && rel != nullptr && track_access) {
      metadata_.access_node(static_cast<int32_t>(rel->start_node()));
      metadata_.access_node(static_cast<int32_t>(rel->end_node()));
    }
    return rel;
  }

  const IntList* get_out_relationships(uint32_t node_id) const {
    return out_relationships_.get(node_id);
  }
  const IntList* get_in_relationships(uint32_t node_id) const {
    return in_relationships_.get(node_id);
  }

  // Loading (unsafeInsert*WithoutCopy), single-threaded.
  void unsafe_insert_node(Node* node) {
    uint32_t node_id = static_cast<uint32_t>(node->id());
    nodes_.put(node_id, node);
    if (track_metadata_) active_nodes_.set(node_id);
  }
  void unsafe_insert_relationship(Relationship* rel) {
    uint32_t rel_id = static_cast<uint32_t>(rel->id());
    uint32_t start = static_cast<uint32_t>(rel->start_node());
    uint32_t end = static_cast<uint32_t>(rel->end_node());
    rels_.put(rel_id, rel);
    if (!out_relationships_.contains(start))
      out_relationships_.put(start, new IntList());
    out_relationships_.get(start)->push_back(static_cast<int32_t>(rel_id));
    if (!in_relationships_.contains(end))
      in_relationships_.put(end, new IntList());
    in_relationships_.get(end)->push_back(static_cast<int32_t>(rel_id));
    if (track_metadata_)
      metadata_.add_relationship(static_cast<int32_t>(start),
                                 static_cast<int32_t>(end));
  }

  MetadataStore& metadata() { return metadata_; }
  uint32_t node_count() const { return nodes_.size(); }
  // Array sizes: the capacities given at construction, rounded up to a power
  // of two (as the original's AtomicObjectArray).
  uint32_t node_capacity() const { return nodes_.capacity(); }
  uint32_t rel_capacity() const { return rels_.capacity(); }
  uint32_t rel_count() const { return rels_.size(); }
  int tracking_threshold() const { return tracking_threshold_; }
  bool track_metadata() const { return track_metadata_; }

private:
  AtomicObjectArray<Node> nodes_;
  AtomicObjectArray<Relationship> rels_;
  AtomicObjectArray<IntList> in_relationships_;
  AtomicObjectArray<IntList> out_relationships_;
  MetadataStore metadata_;
  ConcurrentBitSet active_nodes_;
  bool track_metadata_;
  int tracking_threshold_;
  std::atomic<uint32_t>* versions_ = nullptr;
};

}  // namespace tuskflow

