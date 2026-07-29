#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>

#ifndef _WIN32
#include <sys/resource.h>
#endif

#include "cdc_engine.h"
#include "event_header.h"
#include "test_helpers.h"

namespace {

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

}  // namespace

int main(int argc, char** argv) {
  const size_t iterations = argc > 1 ? static_cast<size_t>(std::strtoull(argv[1], nullptr, 10))
                                     : 100000;
  if (iterations == 0) return 1;

  const auto table_map = mes::test::BuildEvent(
      static_cast<uint8_t>(mes::BinlogEventType::kTableMapEvent), 0, 0,
      mes::test::BuildTableMapBody(1, "bench", "events"));
  const auto row = mes::test::BuildEvent(static_cast<uint8_t>(mes::BinlogEventType::kWriteRowsEvent),
                                         0, 0, mes::test::BuildWriteRowsBody(1, 42));

  mes::CdcEngine engine;
  const auto started = std::chrono::steady_clock::now();
  for (size_t i = 0; i < iterations; ++i) {
    engine.Feed(table_map.data(), table_map.size());
    engine.Feed(row.data(), row.size());
    mes::ChangeEvent event;
    if (!engine.NextEvent(&event)) return 2;
  }
  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
  const double events_per_second = static_cast<double>(iterations) / seconds;
  const double feed_latency_us = seconds * 1'000'000.0 / (iterations * 2);

  std::cout << std::fixed << std::setprecision(2)
            << "events=" << iterations << " decode_events_per_second=" << events_per_second
            << " feed_latency_us=" << feed_latency_us << " max_rss_bytes=" << MaxRssBytes()
            << '\n';
  return 0;
}
