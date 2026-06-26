// libFuzzer harness for the binlog parser entry point.
//
// Feeds arbitrary attacker-controlled bytes through the public C ABI
// (mes_feed / mes_next_event), exercising the event-header parser, state
// machine, TABLE_MAP metadata parser, and row decoder against malformed
// input. Build with: cmake -B build-fuzz -DMES_ENABLE_FUZZ=ON and run the
// resulting `fuzz_feed` binary. Intended to be combined with ASan/UBSan.

#include <cstddef>
#include <cstdint>

#include "mes.h"

/**
 * @brief libFuzzer entry point.
 *
 * Each input is fed to a fresh engine in chunks so that the incremental
 * feeding path (partial events spanning multiple mes_feed calls) is also
 * exercised. All drained events are read out to drive the decoder.
 */
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  mes_engine_t* engine = mes_create();
  if (engine == nullptr) {
    return 0;
  }

  // Feed in small chunks to exercise the partial-event reassembly path.
  constexpr size_t kChunk = 7;
  size_t offset = 0;
  while (offset < size) {
    const size_t len = (size - offset < kChunk) ? (size - offset) : kChunk;
    size_t consumed = 0;
    if (mes_feed(engine, data + offset, len, &consumed) != MES_OK) {
      // On a parse/decode error the contract requires a reset before
      // feeding can continue; exercise that recovery path too.
      mes_reset(engine);
    }
    offset += len;

    // Drain any events produced so the row decoder runs on the input.
    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK && event != nullptr) {
    }
  }

  mes_destroy(engine);
  return 0;
}
