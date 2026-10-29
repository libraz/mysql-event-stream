/**
 * @file checksum.h
 * @brief Per-file binlog checksum algorithm and trailer framing.
 *
 * Each binlog file declares its algorithm in its FORMAT_DESCRIPTION_EVENT
 * (FDE): one byte at `len - 5`, behind a server-version gate (MySQL < 5.6.1 and
 * MariaDB < 5.3.0 write no byte and no trailer). The FDE of a checksum-aware
 * server always carries its own 4-byte trailer, even when it declares OFF. The
 * algorithm is never taken from a server variable; it changes only at an FDE.
 * The artificial ROTATE a dump sends uses the algorithm the replica announced,
 * so under OFF framing it counts as checksummed only when its CRC verifies.
 */

#ifndef MES_CODEC_CHECKSUM_H_
#define MES_CODEC_CHECKSUM_H_

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "mes/codec/event_header.h"
#include "mes/codec/status.h"

namespace mes::codec {

/** @brief Checksum algorithm; values are the FDE's wire byte. */
enum class ChecksumAlgorithm : uint8_t {
  kOff = 0,          ///< No trailer (except on the FDE itself).
  kCrc32 = 1,        ///< Every event ends with a CRC32 of the bytes before it.
  kUndefined = 255,  ///< Pre-checksum server: no event, FDE included, has a trailer.
};

/**
 * @brief Whether an FDE server version predates binlog checksums.
 *
 * MySQL below 5.6.1, MariaDB (version containing "MariaDB" or "-maria-") below
 * 5.3.0. Unparseable components count as 0.
 */
bool ServerVersionPredatesChecksums(std::string_view server_version) noexcept;

/**
 * @brief Read the algorithm a complete FDE frame declares.
 * @param frame Whole event, header included.
 * @return kOk; kTruncated if the frame cannot hold the fixed fields (or, for a
 *   checksum-aware version, the algorithm byte and trailer after them);
 *   kUnsupported for an unknown algorithm byte.
 */
Status ReadFdeChecksumAlgorithm(const uint8_t* frame, size_t len, ChecksumAlgorithm* out);

/** @brief Where the payload ends within one event frame. */
struct FrameLayout {
  size_t payload_length = 0;  ///< Header + body; the trailer, if any, follows.
  bool has_trailer = false;   ///< A 4-byte trailer follows the payload.

  /** @brief Body length (payload minus the common header). */
  size_t body_length() const { return payload_length - kEventHeaderSize; }
};

/**
 * @brief Tracks the algorithm in effect and frames each event under it.
 *
 * Feed every event of one stream, in order, to OnEvent(). Before the first
 * FDE the default algorithm applies (SetDefaultAlgorithm()); each FDE then
 * replaces it for the rest of its file. SetVerify(false) keeps the framing but
 * skips CRC comparison, for producers that verified the trailer already; it
 * does not affect artificial-ROTATE detection, which is framing. Not
 * thread-safe; one instance per stream.
 */
class ChecksumTracker {
 public:
  /** @brief Algorithm assumed before the first FDE of a stream (and after Reset()). */
  void SetDefaultAlgorithm(ChecksumAlgorithm algorithm) noexcept {
    default_algorithm_ = algorithm;
    algorithm_ = algorithm;
  }
  ChecksumAlgorithm default_algorithm() const noexcept { return default_algorithm_; }

  /** @brief Whether CRC32 trailers are compared (default true). */
  void SetVerify(bool verify) noexcept { verify_ = verify; }
  bool verify() const noexcept { return verify_; }

  /** @brief Algorithm in effect for the next non-FDE event. */
  ChecksumAlgorithm algorithm() const noexcept { return algorithm_; }

  /** @brief Start a new stream: return to the default algorithm. */
  void Reset() noexcept { algorithm_ = default_algorithm_; }

  /**
   * @brief Frame one complete event and, under CRC32, verify its trailer.
   *
   * An FDE re-derives the algorithm before it is framed.
   *
   * @param header Parsed header of @p frame.
   * @param frame Whole event; @p frame_length must equal `header.event_length`.
   * @param[out] layout Filled on kOk.
   * @return kOk; kTruncated when the frame is shorter than its header says or an
   *   FDE is too short; kMalformedValue when it is longer; kChecksumMismatch for
   *   a CRC32 mismatch or a checksummed frame under 23 bytes; kUnsupported for
   *   an FDE with an unknown algorithm byte.
   */
  Status OnEvent(const EventHeader& header, const uint8_t* frame, size_t frame_length,
                 FrameLayout* layout);

 private:
  ChecksumAlgorithm default_algorithm_ = ChecksumAlgorithm::kCrc32;
  ChecksumAlgorithm algorithm_ = ChecksumAlgorithm::kCrc32;
  bool verify_ = true;
};

}  // namespace mes::codec

#endif  // MES_CODEC_CHECKSUM_H_
