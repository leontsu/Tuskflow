#pragma once

// BoMB (Saros comparison, 2026-10-29): the static-BoM database of BoMB
// (ccbench include/bomb.hh makeDB, --bomb_use_cache) as graph nodes.
//   MaterialCostMaster (f, m): one node per factory and material, properties
//     price and quantity (L1 reads price / quantity, S1 updates them).
//   ProductCostMaster (f, p): one node per factory and product, property cost
//     (L1 writes it, S2 reads it).
//   The BoM itself is static and cached outside the database, as with
//   --bomb_use_cache: per product, the leaf materials of its WIP trees.
//   JournalVoucher rows are inserts with fresh keys (see tx/bomb_tx.hh).
// The generator follows makeDB: each product has `trees_per_product` distinct
// root WIPs drawn from `root_wips`; a WIP tree has `tree_size` nodes, each new
// node attached to a uniformly chosen earlier one; every leaf WIP gets
// `materials_per_wip` distinct materials drawn from `materials`.  Every
// factory makes the same `products` products.

#include <cstdint>
#include <string>
#include <vector>

#include "storage/graph_storage.hh"

namespace tuskflow {

inline const std::string kBombPriceKey = "price";
inline const std::string kBombQuantityKey = "quantity";
inline const std::string kBombCostKey = "cost";

struct BombGraph {
  uint32_t factories = 0, materials = 0, products = 0;
  uint32_t pc_base = 0;          // first ProductCostMaster node
  uint32_t number_of_nodes = 0;
  // Per product: the material index of every leaf of its BoM tree (with
  // repetitions, as L1 reads every leaf).
  std::vector<std::vector<uint32_t>> leaves;
  uint64_t leaf_reads = 0;       // L1 reads per factory (sum of leaves)
  uint64_t distinct_materials = 0;

  uint32_t mc_node(uint32_t f, uint32_t m) const { return f * materials + m; }
  uint32_t pc_node(uint32_t f, uint32_t p) const { return pc_base + f * products + p; }
};

BombGraph load_bomb_graph(GraphStorage* storage);

}  // namespace tuskflow
