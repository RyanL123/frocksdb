//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "rocksdb/cache.h"
#include "trace_replay/block_cache_tracer.h"
#include "utilities/simulator_cache/cache_simulator.h"

namespace ROCKSDB_NAMESPACE {

static const size_t kBlockSize = 4096;

struct BenchmarkConfig {
  uint64_t num_accesses = 100000;
  uint64_t num_unique_blocks = 5000;
  uint64_t seed = 42;
  uint32_t skew = 0;

  double quick_mrc_sampling_rate = 1.0;
  uint32_t quick_mrc_histogram_bin_size = 1;
  uint32_t quick_mrc_max_bucket_size = 60;
  uint32_t quick_mrc_ghost_cache_multiplier = 1;

  std::string cache_capacities;
  uint32_t sim_num_shard_bits = 0;
  uint64_t warmup_seconds = 0;

  uint64_t quickmrc_cache_capacity = 0;

  std::string output_dir = ".";
  std::string output_prefix = "quickmrc_bench";
};

static void PrintUsage(const char* prog) {
  fprintf(stderr,
    "Usage: %s [options]\n"
    "\n"
    "Workload generation:\n"
    "  --num_accesses=N          Total block accesses (default: 100000)\n"
    "  --num_unique_blocks=N     Number of unique block keys (default: 5000)\n"
    "  --seed=N                  Random seed (default: 42)\n"
    "  --skew=N                  Skew degree, 0=uniform (default: 0)\n"
    "\n"
    "QuickMRC settings:\n"
    "  --quick_mrc_sampling_rate=F       Sampling probability [0,1] (default: 1.0)\n"
    "  --quick_mrc_histogram_bin_size=N  Histogram bin size in 4KB units (default: 1)\n"
    "  --quick_mrc_max_bucket_size=N     Max entries per recency bucket (default: 60)\n"
    "  --quick_mrc_ghost_cache_multiplier=N  Ghost cache multiplier (default: 1)\n"
    "\n"
    "Ground truth simulator:\n"
    "  --cache_capacities=CSV    Comma-separated capacities in bytes (default: auto)\n"
    "  --sim_num_shard_bits=N    Simulator shard bits (default: 0)\n"
    "  --warmup_seconds=N        Simulator warmup seconds (default: 0)\n"
    "\n"
    "QuickMRC cache:\n"
    "  --quickmrc_cache_capacity=N  QuickMRC cache capacity bytes, 0=auto (default: 0)\n"
    "\n"
    "Output:\n"
    "  --output_dir=PATH         Output directory (default: .)\n"
    "  --output_prefix=STR       File name prefix (default: quickmrc_bench)\n"
    "\n"
    "  --help                    Show this message\n",
    prog);
}

static bool TryParseFlag(const char* arg, const char* name, std::string* out) {
  std::string prefix = std::string("--") + name + "=";
  if (strncmp(arg, prefix.c_str(), prefix.size()) == 0) {
    *out = std::string(arg + prefix.size());
    return true;
  }
  return false;
}

static bool TryParseFlag(const char* arg, const char* name, uint64_t* out) {
  std::string val;
  if (TryParseFlag(arg, name, &val)) {
    *out = std::stoull(val);
    return true;
  }
  return false;
}

static bool TryParseFlag(const char* arg, const char* name, uint32_t* out) {
  uint64_t v;
  if (TryParseFlag(arg, name, &v)) {
    *out = static_cast<uint32_t>(v);
    return true;
  }
  return false;
}

static bool TryParseFlag(const char* arg, const char* name, double* out) {
  std::string val;
  if (TryParseFlag(arg, name, &val)) {
    *out = std::stod(val);
    return true;
  }
  return false;
}

static BenchmarkConfig ParseArgs(int argc, char** argv) {
  BenchmarkConfig cfg;
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
      PrintUsage(argv[0]);
      exit(0);
    }
    bool matched = false;
    matched = matched || TryParseFlag(argv[i], "num_accesses", &cfg.num_accesses);
    matched = matched || TryParseFlag(argv[i], "num_unique_blocks", &cfg.num_unique_blocks);
    matched = matched || TryParseFlag(argv[i], "seed", &cfg.seed);
    matched = matched || TryParseFlag(argv[i], "skew", &cfg.skew);
    matched = matched || TryParseFlag(argv[i], "quick_mrc_sampling_rate", &cfg.quick_mrc_sampling_rate);
    matched = matched || TryParseFlag(argv[i], "quick_mrc_histogram_bin_size", &cfg.quick_mrc_histogram_bin_size);
    matched = matched || TryParseFlag(argv[i], "quick_mrc_max_bucket_size", &cfg.quick_mrc_max_bucket_size);
    matched = matched || TryParseFlag(argv[i], "quick_mrc_ghost_cache_multiplier", &cfg.quick_mrc_ghost_cache_multiplier);
    matched = matched || TryParseFlag(argv[i], "cache_capacities", &cfg.cache_capacities);
    matched = matched || TryParseFlag(argv[i], "sim_num_shard_bits", &cfg.sim_num_shard_bits);
    matched = matched || TryParseFlag(argv[i], "warmup_seconds", &cfg.warmup_seconds);
    matched = matched || TryParseFlag(argv[i], "quickmrc_cache_capacity", &cfg.quickmrc_cache_capacity);
    matched = matched || TryParseFlag(argv[i], "output_dir", &cfg.output_dir);
    matched = matched || TryParseFlag(argv[i], "output_prefix", &cfg.output_prefix);
    if (!matched) {
      fprintf(stderr, "Unknown flag: %s\n", argv[i]);
      PrintUsage(argv[0]);
      exit(1);
    }
  }
  return cfg;
}

// ---------------------------------------------------------------------------

static std::vector<uint64_t> ParseCapacities(const std::string& csv) {
  std::vector<uint64_t> caps;
  if (csv.empty()) return caps;
  std::istringstream ss(csv);
  std::string tok;
  while (std::getline(ss, tok, ',')) {
    caps.push_back(std::stoull(tok));
  }
  std::sort(caps.begin(), caps.end());
  return caps;
}

static std::vector<uint64_t> AutoCapacities(uint64_t num_unique_blocks) {
  std::vector<uint64_t> caps;
  uint64_t max_bytes = num_unique_blocks * kBlockSize;
  uint64_t step = std::max<uint64_t>(kBlockSize, max_bytes / 50);
  for (uint64_t c = step; c <= max_bytes; c += step) {
    caps.push_back(c);
  }
  if (caps.empty() || caps.back() != max_bytes) {
    caps.push_back(max_bytes);
  }
  return caps;
}

static std::vector<BlockCacheTraceRecord> GenerateTrace(
    const BenchmarkConfig& cfg) {
  std::vector<BlockCacheTraceRecord> records;
  records.reserve(cfg.num_accesses);

  std::mt19937_64 rng(cfg.seed);
  std::uniform_int_distribution<uint64_t> dist(0, cfg.num_unique_blocks - 1);

  for (uint64_t i = 0; i < cfg.num_accesses; i++) {
    uint64_t block_id = dist(rng);
    for (uint32_t s = 0; s < cfg.skew; s++) {
      block_id = std::min(block_id, dist(rng));
    }

    BlockCacheTraceRecord rec;
    rec.block_key = "blk-" + std::to_string(block_id);
    rec.block_type = TraceType::kBlockTraceDataBlock;
    rec.block_size = kBlockSize;
    rec.cf_id = 0;
    rec.cf_name = "default";
    rec.level = 1;
    rec.sst_fd_number = 100;
    rec.caller = TableReaderCaller::kUserGet;
    rec.is_cache_hit = Boolean::kFalse;
    rec.no_insert = Boolean::kFalse;
    rec.access_timestamp = (i + 1) * kMicrosInSecond;
    records.push_back(std::move(rec));
  }
  return records;
}

struct MRCPoint {
  uint64_t capacity;
  double quickmrc_miss_ratio;
  double ground_truth_miss_ratio;
  double abs_error;
  double rel_error;
};

static void RunQuickMRCPath(
    const std::vector<BlockCacheTraceRecord>& trace,
    const BenchmarkConfig& cfg,
    uint64_t cache_capacity,
    std::vector<uint64_t>* out_histogram,
    uint64_t* out_total_accesses) {
  LRUCacheOptions opts;
  opts.capacity = cache_capacity;
  opts.num_shard_bits = 0;
  opts.strict_capacity_limit = false;
  opts.high_pri_pool_ratio = 0.0;
  opts.quick_mrc_enabled = true;
  opts.quick_mrc_sampling_rate = cfg.quick_mrc_sampling_rate;
  opts.quick_mrc_histogram_bin_size = cfg.quick_mrc_histogram_bin_size;
  opts.quick_mrc_max_bucket_size = cfg.quick_mrc_max_bucket_size;
  opts.quick_mrc_ghost_cache_multiplier = cfg.quick_mrc_ghost_cache_multiplier;
  auto cache = NewLRUCache(opts);

  uint64_t total = 0;
  for (const auto& rec : trace) {
    total++;
    auto handle = cache->Lookup(rec.block_key);
    if (handle) {
      cache->Release(handle);
    } else {
      cache->Insert(rec.block_key, nullptr, rec.block_size, nullptr);
    }
  }

  *out_histogram = cache->GetQuickMRCStackDistanceHistogram();
  *out_total_accesses = total;
}

static double QuickMRCMissRatio(
    const std::vector<uint64_t>& histogram,
    uint32_t bin_size,
    uint64_t capacity_bytes) {
  if (histogram.empty()) return 100.0;

  uint64_t complete_misses = histogram.back();

  uint64_t total_sampled = complete_misses;
  for (size_t i = 0; i + 1 < histogram.size(); i++) {
    total_sampled += histogram[i];
  }
  if (total_sampled == 0) return 0.0;

  uint64_t capacity_units = capacity_bytes / kBlockSize;
  size_t capacity_bin = (bin_size > 0)
                            ? (capacity_units + bin_size - 1) / bin_size
                            : histogram.size();

  uint64_t misses = complete_misses;
  for (size_t i = capacity_bin; i + 1 < histogram.size(); i++) {
    misses += histogram[i];
  }
  return 100.0 * static_cast<double>(misses) /
         static_cast<double>(total_sampled);
}

static void RunGroundTruthPath(
    const std::vector<BlockCacheTraceRecord>& trace,
    const BenchmarkConfig& cfg,
    const std::vector<uint64_t>& capacities,
    std::map<uint64_t, double>* out_miss_ratios) {
  CacheConfiguration config;
  config.cache_name = "lru";
  config.num_shard_bits = cfg.sim_num_shard_bits;
  config.ghost_cache_capacity = 0;
  config.cache_capacities = capacities;

  BlockCacheTraceSimulator simulator(cfg.warmup_seconds, 1, {config});
  Status s = simulator.InitializeCaches();
  if (!s.ok()) {
    fprintf(stderr, "Failed to initialize simulator: %s\n",
            s.ToString().c_str());
    return;
  }

  for (const auto& rec : trace) {
    simulator.Access(rec);
  }

  for (auto const& config_caches : simulator.sim_caches()) {
    const CacheConfiguration& c = config_caches.first;
    for (uint32_t i = 0; i < c.cache_capacities.size(); i++) {
      (*out_miss_ratios)[c.cache_capacities[i]] =
          config_caches.second[i]->miss_ratio_stats().miss_ratio();
    }
  }
}

// ---------------------------------------------------------------------------
// Output writers
// ---------------------------------------------------------------------------

static void WriteMRCComparison(const std::string& path,
                               const std::vector<MRCPoint>& points) {
  std::ofstream out(path);
  if (!out.is_open()) {
    fprintf(stderr, "Cannot write to %s\n", path.c_str());
    return;
  }
  out << "capacity,quickmrc_miss_ratio,ground_truth_miss_ratio,"
         "abs_error,rel_error\n";
  for (const auto& p : points) {
    char buf[256];
    snprintf(buf, sizeof(buf),
             "%" PRIu64 ",%.4f,%.4f,%.4f,%.4f\n",
             p.capacity, p.quickmrc_miss_ratio, p.ground_truth_miss_ratio,
             p.abs_error, p.rel_error);
    out << buf;
  }
  out.close();
  printf("Wrote MRC comparison: %s\n", path.c_str());
}

static void WriteHistogram(const std::string& path,
                           const std::vector<uint64_t>& histogram,
                           uint32_t bin_size) {
  std::ofstream out(path);
  if (!out.is_open()) {
    fprintf(stderr, "Cannot write to %s\n", path.c_str());
    return;
  }
  out << "distance_bin_start_units,count\n";
  for (size_t i = 0; i + 1 < histogram.size(); i++) {
    out << (i * bin_size) << "," << histogram[i] << "\n";
  }
  if (!histogram.empty()) {
    out << "complete_miss," << histogram.back() << "\n";
  }
  out.close();
  printf("Wrote histogram: %s\n", path.c_str());
}

static void WriteRunConfig(const std::string& path,
                           const BenchmarkConfig& cfg) {
  std::ofstream out(path);
  if (!out.is_open()) return;
  out << "{\n";
  out << "  \"num_accesses\": " << cfg.num_accesses << ",\n";
  out << "  \"num_unique_blocks\": " << cfg.num_unique_blocks << ",\n";
  out << "  \"seed\": " << cfg.seed << ",\n";
  out << "  \"skew\": " << cfg.skew << ",\n";
  out << "  \"quick_mrc_sampling_rate\": " << cfg.quick_mrc_sampling_rate << ",\n";
  out << "  \"quick_mrc_histogram_bin_size\": " << cfg.quick_mrc_histogram_bin_size << ",\n";
  out << "  \"quick_mrc_max_bucket_size\": " << cfg.quick_mrc_max_bucket_size << ",\n";
  out << "  \"quick_mrc_ghost_cache_multiplier\": " << cfg.quick_mrc_ghost_cache_multiplier << ",\n";
  out << "  \"quickmrc_cache_capacity\": " << cfg.quickmrc_cache_capacity << ",\n";
  out << "  \"sim_num_shard_bits\": " << cfg.sim_num_shard_bits << ",\n";
  out << "  \"warmup_seconds\": " << cfg.warmup_seconds << ",\n";
  out << "  \"cache_capacities\": \"" << cfg.cache_capacities << "\"\n";
  out << "}\n";
  out.close();
}

// ---------------------------------------------------------------------------

static int Run(int argc, char** argv) {
  BenchmarkConfig cfg = ParseArgs(argc, argv);

  if (cfg.num_unique_blocks == 0 || cfg.num_accesses == 0) {
    fprintf(stderr, "num_accesses and num_unique_blocks must be > 0\n");
    return 1;
  }

  std::vector<uint64_t> capacities = ParseCapacities(cfg.cache_capacities);
  if (capacities.empty()) {
    capacities = AutoCapacities(cfg.num_unique_blocks);
    printf("Auto-generated %zu capacity points\n", capacities.size());
  } else {
    printf("Using %zu user-specified capacity points\n", capacities.size());
  }

  printf("Generating trace: %" PRIu64 " accesses, %" PRIu64 " unique blocks, "
         "seed=%" PRIu64 ", skew=%u\n",
         cfg.num_accesses, cfg.num_unique_blocks, cfg.seed, cfg.skew);
  auto trace = GenerateTrace(cfg);

  uint64_t qmrc_cache_cap = cfg.quickmrc_cache_capacity;
  if (qmrc_cache_cap == 0) {
    qmrc_cache_cap = cfg.num_unique_blocks * kBlockSize;
  }
  printf("Running QuickMRC path (cache_capacity=%" PRIu64
         ", sampling_rate=%.4f, bin_size=%u) ...\n",
         qmrc_cache_cap, cfg.quick_mrc_sampling_rate,
         cfg.quick_mrc_histogram_bin_size);

  std::vector<uint64_t> histogram;
  uint64_t total_accesses = 0;
  RunQuickMRCPath(trace, cfg, qmrc_cache_cap, &histogram, &total_accesses);
  printf("  Histogram bins: %zu, total accesses: %" PRIu64 "\n",
         histogram.size(), total_accesses);

  printf("Running ground truth simulator (%zu capacities) ...\n",
         capacities.size());
  std::map<uint64_t, double> gt_miss_ratios;
  RunGroundTruthPath(trace, cfg, capacities, &gt_miss_ratios);

  std::vector<MRCPoint> points;
  double max_abs_err = 0;
  double sum_abs_err = 0;
  uint64_t worst_cap = 0;

  for (uint64_t cap : capacities) {
    MRCPoint pt;
    pt.capacity = cap;
    pt.quickmrc_miss_ratio =
        QuickMRCMissRatio(histogram, cfg.quick_mrc_histogram_bin_size, cap);
    auto it = gt_miss_ratios.find(cap);
    pt.ground_truth_miss_ratio =
        (it != gt_miss_ratios.end()) ? it->second : -1;
    pt.abs_error =
        std::abs(pt.quickmrc_miss_ratio - pt.ground_truth_miss_ratio);
    pt.rel_error = (pt.ground_truth_miss_ratio > 0)
                       ? pt.abs_error / pt.ground_truth_miss_ratio
                       : 0.0;
    if (pt.abs_error > max_abs_err) {
      max_abs_err = pt.abs_error;
      worst_cap = cap;
    }
    sum_abs_err += pt.abs_error;
    points.push_back(pt);
  }

  double avg_abs_err = points.empty() ? 0 : sum_abs_err / points.size();

  printf("\n=== Summary ===\n");
  printf("Capacity points:      %zu\n", points.size());
  printf("Max absolute error:   %.4f%% (at capacity %" PRIu64 ")\n",
         max_abs_err, worst_cap);
  printf("Avg absolute error:   %.4f%%\n", avg_abs_err);

  std::string prefix = cfg.output_dir + "/" + cfg.output_prefix;
  WriteMRCComparison(prefix + "_mrc_comparison.csv", points);
  WriteHistogram(prefix + "_quickmrc_histogram.csv", histogram,
                 cfg.quick_mrc_histogram_bin_size);
  WriteRunConfig(prefix + "_run_config.json", cfg);

  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  return ROCKSDB_NAMESPACE::Run(argc, argv);
}
