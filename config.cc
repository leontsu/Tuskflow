// Run-time configuration: the fields of the original's DBConfig, as gflags
// with the original defaults (except --tag_optimization, see the help text).

#include "config.hh"

DEFINE_string(dataset, "random", "DATASET: random, ldbc or bomb.");
// BoMB (Saros comparison, 2026-10-29): the static-BoM configuration of BoMB (ccbench include/bomb.hh,
// --bomb_use_cache): every factory makes the same products; L1 costs one factory's products from the leaf
// materials of their BoM trees, S1 updates one material cost, S2 reads a factory's product costs and
// inserts one journal voucher per product.
DEFINE_uint64(bomb_factories, 8, "bomb: factories (--bomb_factory_size).");
DEFINE_uint64(bomb_materials, 75000, "bomb: raw materials (--bomb_material_size).");
DEFINE_uint64(bomb_products, 100, "bomb: products per factory (--bomb_base_product_size_per_factory).");
DEFINE_uint64(bomb_root_wips, 198000, "bomb: root work-in-progress items (--bomb_work_size).");
DEFINE_uint64(bomb_trees_per_product, 5, "bomb: WIP trees per product (--bomb_tree_num_per_product).");
DEFINE_uint64(bomb_tree_size, 10, "bomb: WIP nodes per tree (--bomb_base_tree_size).");
DEFINE_uint64(bomb_materials_per_wip, 3, "bomb: materials per leaf WIP (--bomb_material_per_wip).");
DEFINE_uint64(bomb_seed, 20261029, "bomb: seed of the BoM generator.");
DEFINE_uint64(bomb_interactive_us, 0, "bomb: pause per statement, microseconds (--bomb_interactive_ms): "
              "after each of L1's product-cost writes, after S1, after S2's scan and after its inserts.");
DEFINE_uint64(bomb_l1_clients, 1, "bomb: concurrent L1 clients, each submitting L1 back to back.");
DEFINE_string(bomb_l1_read_state, "visited", "bomb: mammoth state of L1's material reads: visited (done "
              "with them once read; TuskFlow's best case) or pending (until L1 commits; a database that "
              "cannot know whether an interactive client will read them again).");
DEFINE_string(ldbc_path, "",
              "LDBC SNB datagen raw CSV directory (.../graphs/csv/raw/"
              "composite-merged-fk), for --dataset=ldbc.  The LDBC workload is "
              "USE_TRAVERSALS = true with the mammoth LDBC_QUERY_TYPE = 0.");
DEFINE_uint64(nodes, 30000000, "RandomLoader NODES_SIZE.");
DEFINE_uint64(rels, 100000000, "RandomLoader RELS_SIZE.");
DEFINE_uint64(seed, 42, "Seed of the random graph (the original is unseeded; "
              "same generator as GraphStorage::build_tuskflow_random).");
DEFINE_uint64(node_capacity, 36000000,
              "DBConfig.INITIAL_NODES_SIZE when the storage is created (array "
              "capacity, rounded up to a power of two).");
DEFINE_uint64(rel_capacity, 160000000,
              "DBConfig.INITIAL_RELS_SIZE when the storage is created.");
DEFINE_uint64(workers, 0, "Worker threads; 0 = hardware threads "
              "(Runtime.availableProcessors()).");
DEFINE_uint64(clients, 100, "NUMBER_OF_CLIENTS.");
DEFINE_uint64(client_tx_per_sec, 100, "CLIENT_TX_PER_SEC (0 = unthrottled).");
DEFINE_uint64(bomb_s2_round_trips, 2, "bomb: round trips of S2 with --bomb_interactive_us: one after the read, then "
              "(n - 1) for the inserts, as Saros's --bomb_s2_round_trips. 2 = the original.");
DEFINE_bool(bomb_l1_own_factories, false, "bomb: L1 client i (0-based) costs only factories i, i+k, i+2k, ... "
            "(k = --bomb_l1_clients), as Saros's --bomb_l1_own_factories.");
DEFINE_bool(client_paced, false, "Submit one transaction every 1/CLIENT_TX_PER_SEC s, clients staggered, "
            "instead of the original burst of CLIENT_TX_PER_SEC at the start of each second.");
DEFINE_uint64(read_ratio, 100, "READ_RATIO (percent read-only transactions).");
DEFINE_uint64(ops, 10, "NUMBER_OF_OPS.");
DEFINE_uint64(hops, 1, "NUMBER_OF_HOPS.");
DEFINE_bool(use_zipf, true, "USE_ZIPF_DISTRIBUTION (shuffled mapping).");
DEFINE_uint64(epoch_size, 10000, "EPOCH_SIZE.");
DEFINE_bool(start_lr, true, "START_LONG_RUNNING_TX.");
DEFINE_uint64(lr_budget, 500000, "LONG_RUNNING_EXECUTION_BUDGET.");
// R2 of the Saros comparison (2026-10-27): a read-heavy long transaction run back to back.
DEFINE_string(lr_kind, "q0", "LDBC long transaction: q0 (all Messages, the original), q2 (FindExperts with the "
              "BI query's Country, see FindExpertsLongRunningTx) or r2 "
              "(read the Messages of up to --r2_friends friends of a random Person, update a score "
              "in the Person and those friends).");
DEFINE_uint64(r2_friends, 6, "r2: friends per long transaction.");
// Multi-mammoth LDBC (2026-10-31), as the Saros driver's --r2_clients/--r2_source_only/--r2_own_persons.
// LDBC Q2 with the BI query's Country parameter (2026-10-31), as the Saros driver's --ldbc_mode=q2.
DEFINE_string(q2_countries, "Ethiopia,South_Korea,Niger,Colombia,Greece,Senegal,Romania,Iran,Australia,Tunisia,Cuba,"
              "Argentina,Sri_Lanka,South_Africa,Egypt,Kenya",
              "q2: Country of each long client (client i takes the i-th).");
DEFINE_uint64(q2_min_path, 2, "q2: minimum knows distance of a candidate (MIN_PATH_LENGTH).");
DEFINE_uint64(q2_max_path, 4, "q2: maximum knows distance of a candidate (MAX_PATH_LENGTH).");
DEFINE_uint64(q2_hub_factor, 10, "q2: a source Person has at least this many times the average number of friends.");
DEFINE_uint64(q2_stmt_delay_us, 0, "q2: client round trip after each statement (the search, then each candidate's "
              "update); with d > 0 one update per epoch once the previous round trip has elapsed (as BoMB's L1).");
// TuskFlow's Fig. 11 setup (2026-11-01), as the Saros driver's flags of the same names.
DEFINE_bool(q2_all_countries, false, "q2: every Person 2..4 knows hops from the hub is a candidate (the original Q2, "
            "no Country).");
DEFINE_int64(q2_hub, -1, "q2: the source Person is hub number q2_hub (-1 = a random hub per Q2).");
DEFINE_uint64(lr_clients, 1, "ldbc: concurrent long-transaction clients (each one back to back with "
              "--lr_repeat); TuskFlow schedules their mammoths one at a time.");
DEFINE_bool(r2_source_only, false, "r2: update only the source Person's score (not the friends').");
DEFINE_bool(r2_own_persons, false, "r2: long client i of --lr_clients=k scores only Persons i, i + k, "
            "i + 2k, ... (0-based within the Person range), as --bomb_l1_own_factories.");
DEFINE_bool(lr_repeat, false, "Submit the next long transaction as soon as the previous one "
            "finishes, until the experiment ends (the original submits one).");
DEFINE_uint64(lr_delay_ms, 100000, "LONG_RUNNING_TX_DELAY_IN_MILLIS.");
DEFINE_uint64(duration_ms, 150000, "EXPERIMENT_DURATION_IN_MILLIS.");
DEFINE_bool(reordering, true, "USE_REORDERING_OPTIMIZATION.");
DEFINE_bool(tag_optimization, false,
            "USE_TAG_OPTIMIZATION (original default true; false is the "
            "minimal configuration).");
DEFINE_bool(track_metadata, true,
            "GraphStorage access tracking for the partitioners (on in the "
            "original: createGraphStorage(true)).");
DEFINE_uint64(tracking_threshold, 40, "TRACKING_THRESHOLD (percent).");
DEFINE_string(epoch_log, "", "If set, write one line per non-empty epoch.");
DEFINE_bool(fix_getnext_race, false,
            "Fix the original's getNext() race: "
            "after polling, check that the epoch and the execution mode are "
            "unchanged and otherwise put the task back.  false = original "
            "behaviour (a worker that read a stale mode can take a task of the "
            "next epoch, re-submit it to pending or commit it during Phase 1).");
DEFINE_bool(lr_breakdown, false,
            "Measure the mammoth's per-step cost with rdtsc (Phase 1: reserve, "
            "getNode, copy, addProperty+write set; Phase 2: merge, free).  Not "
            "in the original; adds a few ns per step.");
DEFINE_string(ser_log, "",
              "If set, write a serializability log to this file: "
              "for every committed transaction the (node, version) pairs it "
              "read and wrote.  Not in the original; for correctness runs.");
DEFINE_bool(fix_mode_switch_race, false,
            "Fix the original's races at the switch from 2PL to epochs: "
            "wait for running 2PL transactions "
            "to finish before the first epoch, and give back the epoch slot "
            "of a 2PL task found during Phase 1.  false = original behaviour.");
DEFINE_bool(fix_qsbr_retire_pin, false,
            "Fix the port's QSBR pin (not in the original, which uses the JVM "
            "GC): a thread that is not a worker registers on its first "
            "retire() (the main thread, when q2_prepare's access-metadata "
            "inserts resize the map) and never reports a quiescent state, so "
            "nothing retired later is ever freed and every worker rescans a "
            "growing retired list between tasks.  With the fix such threads "
            "are offline except inside Qsbr::Section (the access-metadata "
            "update).  Changes only when memory is freed.  false = port "
            "behaviour before 2026-11-01.");
