#pragma once

// Run-time configuration (DBConfig of the original).  Defined in config.cc.

#include <gflags/gflags.h>

DECLARE_string(dataset);
DECLARE_uint64(bomb_factories);
DECLARE_uint64(bomb_materials);
DECLARE_uint64(bomb_products);
DECLARE_uint64(bomb_root_wips);
DECLARE_uint64(bomb_trees_per_product);
DECLARE_uint64(bomb_tree_size);
DECLARE_uint64(bomb_materials_per_wip);
DECLARE_uint64(bomb_seed);
DECLARE_uint64(bomb_interactive_us);
DECLARE_uint64(bomb_l1_clients);
DECLARE_string(bomb_l1_read_state);
DECLARE_string(ldbc_path);
DECLARE_uint64(nodes);
DECLARE_uint64(rels);
DECLARE_uint64(seed);
DECLARE_uint64(node_capacity);
DECLARE_uint64(rel_capacity);
DECLARE_uint64(workers);
DECLARE_uint64(clients);
DECLARE_uint64(client_tx_per_sec);
DECLARE_bool(client_paced);
DECLARE_bool(bomb_l1_own_factories);
DECLARE_uint64(bomb_s2_round_trips);
DECLARE_uint64(read_ratio);
DECLARE_uint64(ops);
DECLARE_uint64(hops);
DECLARE_bool(use_zipf);
DECLARE_uint64(epoch_size);
DECLARE_bool(start_lr);
DECLARE_uint64(lr_budget);
DECLARE_string(lr_kind);
DECLARE_uint64(r2_friends);
DECLARE_uint64(lr_clients);
DECLARE_string(q2_countries);
DECLARE_uint64(q2_min_path);
DECLARE_uint64(q2_max_path);
DECLARE_uint64(q2_hub_factor);
DECLARE_uint64(q2_stmt_delay_us);
DECLARE_bool(q2_all_countries);
DECLARE_int64(q2_hub);
DECLARE_bool(r2_source_only);
DECLARE_bool(r2_own_persons);
DECLARE_bool(lr_repeat);
DECLARE_uint64(lr_delay_ms);
DECLARE_uint64(duration_ms);
DECLARE_bool(reordering);
DECLARE_bool(tag_optimization);
DECLARE_bool(track_metadata);
DECLARE_uint64(tracking_threshold);
DECLARE_string(epoch_log);
DECLARE_bool(fix_getnext_race);
DECLARE_bool(lr_breakdown);
DECLARE_string(ser_log);
DECLARE_bool(fix_mode_switch_race);
DECLARE_bool(fix_qsbr_retire_pin);
