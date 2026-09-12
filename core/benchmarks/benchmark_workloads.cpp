// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file benchmark_workloads.cpp
 * @brief Decode throughput and per-event memory for production-shaped streams.
 *
 * Two binaries are built from this file. The throughput binary runs with the
 * stock allocator so the timings are undistorted; the memory binary links
 * alloc_tracker.cpp, which replaces global operator new, and reports the bytes
 * an event costs while it waits in the engine queue. Selecting a mode:
 *
 *   mes_benchmark_workloads [iterations]        throughput per workload
 *   mes_benchmark_workloads --scaling [iters]   per-thread decode scaling
 *   mes_benchmark_workloads --dump              decoded sample for each schema
 *   mes_benchmark_workloads_mem [events]        queued bytes per event
 *
 * `--only <name>` restricts either binary to a single workload, which is what
 * a sampling profiler needs to attribute time to one schema. `--emit <dir>`
 * writes each workload's byte stream to `<dir>/<name>.bin` so the binding
 * benchmarks can feed exactly the same events.
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "cdc_engine.h"
#include "event_header.h"
#include "workloads.h"

#ifdef MES_BENCH_ALLOC_TRACKING
#include "alloc_tracker.h"
#endif

namespace {

using mes::bench::ColumnSpec;

size_t MaxRssBytes() {
#ifdef _WIN32
  return 0;
#else
  struct rusage usage {};
  if (getrusage(RUSAGE_SELF, &usage) != 0) return 0;
#ifdef __APPLE__
  return static_cast<size_t>(usage.ru_maxrss);
#else
  return static_cast<size_t>(usage.ru_maxrss) * 1024;
#endif
#endif
}

/// One replayable unit of stream: the bytes fed per iteration and how many
/// ChangeEvents the engine is expected to produce from them.
struct Workload {
  std::string name;
  std::vector<uint8_t> bytes;
  size_t rows_per_iteration = 0;
  size_t columns = 0;
  size_t annotate_sql_bytes = 0;
};

void Append(std::vector<uint8_t>& dst, const std::vector<uint8_t>& src) {
  dst.insert(dst.end(), src.begin(), src.end());
}

/// TABLE_MAP + ROWS, optionally preceded by an ANNOTATE_ROWS statement.
Workload MakeWorkload(const std::string& name, const std::vector<ColumnSpec>& schema,
                      const std::vector<uint8_t>& rows_body, uint8_t rows_type_code, size_t rows,
                      size_t annotate_sql_bytes) {
  Workload w;
  w.name = name;
  w.rows_per_iteration = rows;
  w.columns = schema.size();
  w.annotate_sql_bytes = annotate_sql_bytes;

  if (annotate_sql_bytes > 0) {
    std::string sql = "INSERT INTO bench.orders (id, user_id, amount) VALUES ";
    while (sql.size() < annotate_sql_bytes) sql += "(1, 2, 3.45),";
    sql.resize(annotate_sql_bytes);
    const std::vector<uint8_t> body(sql.begin(), sql.end());
    Append(w.bytes,
           mes::test::BuildEvent(
               static_cast<uint8_t>(mes::BinlogEventType::kMariaDBAnnotateRowsEvent), 0, 0, body));
  }

  Append(w.bytes,
         mes::test::BuildEvent(static_cast<uint8_t>(mes::BinlogEventType::kTableMapEvent), 0, 0,
                               mes::bench::BuildTableMap(1, "bench", "orders", schema)));
  Append(w.bytes, mes::test::BuildEvent(rows_type_code, 0, 0, rows_body));
  return w;
}

Workload WriteWorkload(const std::string& name, const std::vector<ColumnSpec>& schema, size_t rows,
                       size_t annotate_sql_bytes = 0) {
  return MakeWorkload(name, schema, mes::bench::BuildRowsBody(1, schema, rows),
                      static_cast<uint8_t>(mes::BinlogEventType::kWriteRowsEvent), rows,
                      annotate_sql_bytes);
}

Workload UpdateWorkload(const std::string& name, const std::vector<ColumnSpec>& before,
                        const std::vector<ColumnSpec>& after, size_t rows) {
  return MakeWorkload(name, before, mes::bench::BuildUpdateRowsBody(1, before, after, rows),
                      static_cast<uint8_t>(mes::BinlogEventType::kUpdateRowsEvent), rows, 0);
}

std::vector<Workload> BuildWorkloads() {
  const auto int1 = mes::bench::SchemaInt1();
  const auto temporal7 = mes::bench::SchemaTemporal7();
  const auto strings7 = mes::bench::SchemaStrings7();
  const auto wide28 = mes::bench::SchemaWide28();
  const auto wide28_after = mes::bench::SchemaWide28After();

  std::vector<Workload> w;
  w.push_back(WriteWorkload("int1_write_x1", int1, 1));
  w.push_back(WriteWorkload("temporal7_write_x1", temporal7, 1));
  w.push_back(WriteWorkload("strings7_write_x1", strings7, 1));
  w.push_back(WriteWorkload("wide28_write_x1", wide28, 1));
  w.push_back(WriteWorkload("wide28_write_x50", wide28, 50));
  w.push_back(UpdateWorkload("wide28_update_x1", wide28, wide28_after, 1));
  w.push_back(UpdateWorkload("wide28_update_x50", wide28, wide28_after, 50));
  w.push_back(WriteWorkload("annotate256_wide28_x1", wide28, 1, 256));
  w.push_back(WriteWorkload("annotate256_wide28_x50", wide28, 50, 256));
  w.push_back(WriteWorkload("annotate8k_wide28_x1", wide28, 1, 8192));
  w.push_back(WriteWorkload("annotate8k_wide28_x50", wide28, 50, 8192));
  w.push_back(WriteWorkload("annotate8k_wide28_x200", wide28, 200, 8192));
  return w;
}

/// Feed one iteration and drain every event it produces.
/// Returns false if the engine did not yield the expected event count.
bool RunIteration(mes::CdcEngine& engine, const Workload& w) {
  size_t offset = 0;
  while (offset < w.bytes.size()) {
    const size_t consumed = engine.Feed(w.bytes.data() + offset, w.bytes.size() - offset);
    if (consumed == 0) return false;
    offset += consumed;
  }
  size_t drained = 0;
  mes::ChangeEvent event;
  while (engine.NextEvent(&event)) ++drained;
  return drained == w.rows_per_iteration;
}

double MedianOf(std::vector<double> samples) {
  std::sort(samples.begin(), samples.end());
  const size_t mid = samples.size() / 2;
  if (samples.size() % 2 == 1) return samples[mid];
  return (samples[mid - 1] + samples[mid]) / 2.0;
}

/// Empty selects every workload.
std::string g_only;

bool Selected(const Workload& w) { return g_only.empty() || w.name == g_only; }

void ReportThroughput(size_t iterations, size_t repeats) {
  std::cout << "mode=throughput iterations=" << iterations << " repeats=" << repeats << '\n';
  for (const auto& w : BuildWorkloads()) {
    if (!Selected(w)) continue;
    mes::CdcEngine warmup;
    for (size_t i = 0; i < 64; ++i) {
      if (!RunIteration(warmup, w)) {
        std::cerr << "workload " << w.name << " did not decode as expected\n";
        std::exit(2);
      }
    }

    std::vector<double> rows_per_second;
    std::vector<double> us_per_row;
    for (size_t r = 0; r < repeats; ++r) {
      mes::CdcEngine engine;
      const auto started = std::chrono::steady_clock::now();
      for (size_t i = 0; i < iterations; ++i) {
        if (!RunIteration(engine, w)) {
          std::cerr << "workload " << w.name << " failed mid-run\n";
          std::exit(2);
        }
      }
      const double seconds =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
      const double rows = static_cast<double>(iterations * w.rows_per_iteration);
      rows_per_second.push_back(rows / seconds);
      us_per_row.push_back(seconds * 1'000'000.0 / rows);
    }

    std::cout << std::fixed << std::setprecision(2) << "workload=" << w.name
              << " columns=" << w.columns << " rows_per_event=" << w.rows_per_iteration
              << " annotate_sql_bytes=" << w.annotate_sql_bytes
              << " row_events_per_second=" << MedianOf(rows_per_second)
              << " us_per_row_event=" << MedianOf(us_per_row) << " max_rss_bytes=" << MaxRssBytes()
              << '\n';
  }
}

/// Decode the same workload on N independent engines to expose any shared
/// serialization point in the column decoders.
void ReportScaling(size_t iterations, size_t max_threads) {
  const auto workloads = BuildWorkloads();
  std::cout << "mode=scaling iterations_per_thread=" << iterations << '\n';
  for (const auto& w : workloads) {
    if (!g_only.empty()) {
      if (w.name != g_only) continue;
    } else if (w.name != "temporal7_write_x1" && w.name != "strings7_write_x1" &&
               w.name != "wide28_write_x50") {
      continue;
    }
    double single_thread_rate = 0.0;
    for (size_t threads = 1; threads <= max_threads; threads *= 2) {
      std::vector<std::thread> workers;
      const auto started = std::chrono::steady_clock::now();
      for (size_t t = 0; t < threads; ++t) {
        workers.emplace_back([&w, iterations]() {
          mes::CdcEngine engine;
          for (size_t i = 0; i < iterations; ++i) {
            if (!RunIteration(engine, w)) std::exit(2);
          }
        });
      }
      for (auto& t : workers) t.join();
      const double seconds =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
      const double rows = static_cast<double>(iterations * w.rows_per_iteration * threads);
      const double rate = rows / seconds;
      if (threads == 1) single_thread_rate = rate;
      std::cout << std::fixed << std::setprecision(2) << "workload=" << w.name
                << " threads=" << threads << " row_events_per_second=" << rate
                << " scaling_vs_1t=" << (rate / single_thread_rate) << '\n';
    }
  }
}

/// Print the decoded first row of every schema so the synthetic bytes can be
/// checked against what a server would have produced.
void DumpSamples() {
  for (const auto& w : BuildWorkloads()) {
    if (!Selected(w) || w.rows_per_iteration != 1) continue;
    mes::CdcEngine engine;
    size_t offset = 0;
    while (offset < w.bytes.size()) {
      const size_t consumed = engine.Feed(w.bytes.data() + offset, w.bytes.size() - offset);
      if (consumed == 0) break;
      offset += consumed;
    }
    mes::ChangeEvent event;
    if (!engine.NextEvent(&event)) {
      std::cout << "workload=" << w.name << " DECODE FAILED error=" << engine.ErrorCode() << '\n';
      continue;
    }
    std::cout << "workload=" << w.name << " source_sql_bytes=" << event.SourceSql().size() << '\n';
    const mes::RowData& row = event.after.columns.empty() ? event.before : event.after;
    for (const auto& col : row.columns) {
      std::cout << "  " << std::string(col.name) << " = ";
      if (col.is_null) {
        std::cout << "NULL";
      } else if (!col.string_val.empty() || col.type == mes::ColumnType::kVarchar ||
                 col.type == mes::ColumnType::kBlob || col.type == mes::ColumnType::kString) {
        std::cout << (col.is_binary ? "<binary:" : "'")
                  << (col.is_binary ? std::to_string(col.string_val.size())
                                    : col.string_val.substr(0, 48))
                  << (col.is_binary ? ">" : "'");
      } else if (col.type == mes::ColumnType::kFloat || col.type == mes::ColumnType::kDouble) {
        std::cout << col.real_val;
      } else {
        std::cout << col.int_val;
      }
      std::cout << '\n';
    }
  }
}

/// Write each selected workload's stream to <dir>/<name>.bin.
int EmitStreams(const std::string& dir) {
  for (const auto& w : BuildWorkloads()) {
    if (!Selected(w)) continue;
    const std::string path = dir + "/" + w.name + ".bin";
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
      std::cerr << "cannot write " << path << '\n';
      return 1;
    }
    out.write(reinterpret_cast<const char*>(w.bytes.data()),
              static_cast<std::streamsize>(w.bytes.size()));
    std::cout << "wrote " << path << " bytes=" << w.bytes.size()
              << " rows_per_event=" << w.rows_per_iteration << '\n';
  }
  return 0;
}

#ifdef MES_BENCH_ALLOC_TRACKING
/// Queue events without draining, so the reported peak is the memory the
/// engine actually holds per pending event.
void MeasureWorkload(const Workload& w, size_t queued_events) {
  const size_t iterations = std::max<size_t>(1, queued_events / w.rows_per_iteration);
  const size_t rows = iterations * w.rows_per_iteration;
  {
    // Decode on a throwaway engine first, so whatever the decode path builds
    // lazily on its first use is already resident and is not billed to the
    // measured run.
    mes::CdcEngine warmup;
    for (size_t i = 0; i < 8; ++i) {
      if (!RunIteration(warmup, w)) {
        std::cerr << "workload " << w.name << " did not decode as expected\n";
        std::exit(2);
      }
    }
  }

  mes::CdcEngine engine;
  engine.SetMaxQueueSize(rows + w.rows_per_iteration);
  // Holding every event at once is the measurement, so neither bound may
  // retire an event before the snapshot is taken. The byte budget charges
  // each row event for the names and statement that produced it, which for a
  // multi-row workload is many times the bytes fed, so it is lifted rather
  // than sized from the input.
  engine.SetMaxQueueBytes(std::numeric_limits<size_t>::max());
  mes::bench::ResetStats();
  const auto before = mes::bench::Snapshot();
  for (size_t i = 0; i < iterations; ++i) {
    size_t offset = 0;
    while (offset < w.bytes.size()) {
      const size_t consumed = engine.Feed(w.bytes.data() + offset, w.bytes.size() - offset);
      if (consumed == 0) {
        std::cerr << "workload " << w.name << " stopped consuming after " << offset << " of "
                  << w.bytes.size() << " bytes on iteration " << i << '\n';
        std::exit(2);
      }
      offset += consumed;
    }
  }
  const auto after = mes::bench::Snapshot();
  // Every figure below is an average over the events the engine is holding,
  // so the divisor has to be what the queue actually contains rather than
  // what the workload was expected to produce.
  const size_t pending = engine.PendingEventCount();
  if (pending != rows) {
    std::cerr << "workload " << w.name << " holds " << pending << " events, expected " << rows
              << '\n';
    std::exit(2);
  }
  const double per_event =
      static_cast<double>(after.live_bytes - before.live_bytes) / static_cast<double>(rows);
  const double peak_per_event =
      static_cast<double>(after.peak_live_bytes - before.live_bytes) / static_cast<double>(rows);
  std::cout << std::fixed << std::setprecision(1) << "workload=" << w.name
            << " columns=" << w.columns << " rows_per_event=" << w.rows_per_iteration
            << " annotate_sql_bytes=" << w.annotate_sql_bytes
            << " queued_bytes_per_event=" << per_event << " peak_bytes_per_event=" << peak_per_event
            << " allocs_per_event=" << static_cast<double>(after.alloc_count) / rows
            << " total_bytes_per_event=" << static_cast<double>(after.total_bytes) / rows
            << " resident_total_bytes=" << (after.live_bytes - before.live_bytes)
            << " max_rss_bytes=" << MaxRssBytes() << '\n';
}

/**
 * Measure every selected workload, each in its own process.
 *
 * max_rss_bytes is the peak resident size of the whole process and only ever
 * grows, so in a shared process every workload after the first would report
 * its predecessor's peak rather than its own. The fork happens before this
 * process has decoded anything, which also keeps whatever the decode path
 * builds lazily on first use out of the first workload's figures.
 */
void ReportMemory(size_t queued_events) {
  std::cout << "mode=memory queued_events=" << queued_events << '\n';
  std::cout.flush();
  for (const auto& w : BuildWorkloads()) {
    if (!Selected(w)) continue;
#ifdef _WIN32
    // No isolation primitive here, so one workload per invocation is the only
    // way to get a figure that does not depend on what ran before it.
    MeasureWorkload(w, queued_events);
    return;
#else
    const pid_t child = fork();
    if (child < 0) {
      std::cerr << "workload " << w.name << " could not be isolated for measurement\n";
      std::exit(2);
    }
    if (child == 0) {
      MeasureWorkload(w, queued_events);
      std::cout.flush();
      _exit(0);
    }
    int status = 0;
    if (waitpid(child, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      std::cerr << "workload " << w.name << " did not complete its measurement\n";
      std::exit(2);
    }
#endif
  }
}
#endif

}  // namespace

int main(int argc, char** argv) {
  std::vector<char*> args(argv, argv + argc);
  for (size_t i = 1; i + 1 < args.size(); ++i) {
    if (std::strcmp(args[i], "--only") == 0) {
      g_only = args[i + 1];
      args.erase(args.begin() + static_cast<long>(i), args.begin() + static_cast<long>(i) + 2);
      break;
    }
  }
  argc = static_cast<int>(args.size());
  argv = args.data();

  if (argc > 1 && std::strcmp(argv[1], "--dump") == 0) {
    DumpSamples();
    return 0;
  }
  if (argc > 2 && std::strcmp(argv[1], "--emit") == 0) {
    return EmitStreams(argv[2]);
  }
  if (argc > 1 && std::strcmp(argv[1], "--scaling") == 0) {
    const size_t iterations = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 200000;
    ReportScaling(iterations, 8);
    return 0;
  }
#ifdef MES_BENCH_ALLOC_TRACKING
  const size_t queued = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 10000;
  ReportMemory(queued == 0 ? 10000 : queued);
#else
  const size_t iterations = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 20000;
  ReportThroughput(iterations == 0 ? 20000 : iterations, 5);
#endif
  return 0;
}
