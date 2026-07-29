#ifndef MES_CLIENT_GTID_SET_H_
#define MES_CLIENT_GTID_SET_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "mes.h"

namespace mes {

/** Canonical MySQL GTID interval set used for resume and checkpoints. */
class GtidSet {
 public:
  using Sid = std::array<uint8_t, 16>;

  struct Tsid {
    Sid sid{};
    std::string tag;

    bool operator<(const Tsid& other) const {
      return sid < other.sid || (sid == other.sid && tag < other.tag);
    }
  };

  struct Interval {
    uint64_t start = 0;
    uint64_t end = 0;  // Exclusive.
  };

  static constexpr size_t kMaxIntervalsPerSid = 65536;

  static mes_error_t Parse(const std::string& text, GtidSet* out);
  static bool DecodeBinary(const uint8_t* data, size_t size, GtidSet* out);

  mes_error_t EncodeBinary(std::vector<uint8_t>* out) const;
  bool Merge(const GtidSet& other);
  /** Return true when every interval in this set is covered by @p other. */
  bool IsSubsetOf(const GtidSet& other) const;
  bool Add(const Sid& sid, Interval interval);
  bool Add(const Sid& sid, const std::string& tag, Interval interval);
  std::string ToString() const;
  void Clear() { sets_.clear(); }

 private:
  using SetMap = std::map<Tsid, std::vector<Interval>>;

  static bool NormalizeIntervals(std::vector<Interval>* intervals);
  static mes_error_t ParseUuid(const std::string& text, Sid* out);
  static mes_error_t ParseInterval(const std::string& text, Interval* out);
  static bool NormalizeTag(const std::string& text, std::string* out);
  static void StoreU64Le(std::vector<uint8_t>* out, uint64_t value);
  static std::string FormatSid(const Sid& sid);

  SetMap sets_;
};

}  // namespace mes

#endif  // MES_CLIENT_GTID_SET_H_
