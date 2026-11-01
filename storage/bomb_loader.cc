#include "storage/bomb_loader.hh"

#include <set>
#include <unordered_map>

#include "config.hh"
#include "util/random.hh"

namespace tuskflow {

namespace {

// One WIP tree (load_work_in_progress): the leaf materials of its leaf WIPs.
std::vector<uint32_t> make_wip_tree(Xoroshiro128Plus& rnd, uint32_t tree_size,
                                    uint32_t materials_per_wip, uint32_t materials) {
  std::vector<uint32_t> children(tree_size, 0);
  for (uint32_t i = 1; i < tree_size; ++i)  // parent = nodes.at(random(0, size-1))
    ++children[rnd.random_int(0, i - 1)];
  std::vector<uint32_t> out;
  for (uint32_t i = 0; i < tree_size; ++i) {
    if (children[i] != 0) continue;
    std::set<uint32_t> s;
    while (s.size() < materials_per_wip)
      s.insert(static_cast<uint32_t>(rnd.random_int(0, materials - 1)));
    out.insert(out.end(), s.begin(), s.end());
  }
  return out;
}

}  // namespace

BombGraph load_bomb_graph(GraphStorage* storage) {
  BombGraph g;
  g.factories = static_cast<uint32_t>(FLAGS_bomb_factories);
  g.materials = static_cast<uint32_t>(FLAGS_bomb_materials);
  g.products = static_cast<uint32_t>(FLAGS_bomb_products);
  g.pc_base = g.factories * g.materials;
  g.number_of_nodes = g.pc_base + g.factories * g.products;

  Xoroshiro128Plus rnd;
  rnd.s[0] = rnd.splitMix64(FLAGS_bomb_seed);
  rnd.s[1] = rnd.splitMix64(rnd.s[0]);
  std::unordered_map<uint32_t, std::vector<uint32_t>> wip_trees;  // root WIP -> leaf materials
  std::set<uint32_t> distinct;
  g.leaves.resize(g.products);
  for (uint32_t p = 0; p < g.products; ++p) {
    std::set<uint32_t> roots;
    while (roots.size() < FLAGS_bomb_trees_per_product)
      roots.insert(static_cast<uint32_t>(rnd.random_int(0, FLAGS_bomb_root_wips - 1)));
    for (uint32_t r : roots) {
      auto it = wip_trees.find(r);
      if (it == wip_trees.end())
        it = wip_trees.emplace(r, make_wip_tree(rnd, static_cast<uint32_t>(FLAGS_bomb_tree_size),
                                                static_cast<uint32_t>(FLAGS_bomb_materials_per_wip),
                                                g.materials)).first;
      g.leaves[p].insert(g.leaves[p].end(), it->second.begin(), it->second.end());
    }
    g.leaf_reads += g.leaves[p].size();
    distinct.insert(g.leaves[p].begin(), g.leaves[p].end());
  }
  g.distinct_materials = distinct.size();

  for (uint32_t f = 0; f < g.factories; ++f)
    for (uint32_t m = 0; m < g.materials; ++m) {
      Node* node = new Node(g.mc_node(f, m));
      node->add_property(kBombPriceKey, 1);     // insert_material_cost_master(..., 1.0, 1.0)
      node->add_property(kBombQuantityKey, 1);
      storage->unsafe_insert_node(node);
    }
  for (uint32_t f = 0; f < g.factories; ++f)
    for (uint32_t p = 0; p < g.products; ++p) {
      Node* node = new Node(g.pc_node(f, p));
      node->add_property(kBombCostKey, 0);      // insert_product_cost_master(..., 0.0)
      storage->unsafe_insert_node(node);
    }
  return g;
}

}  // namespace tuskflow
