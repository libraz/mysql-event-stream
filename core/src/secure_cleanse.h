// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_SECURE_CLEANSE_H_
#define MES_SECURE_CLEANSE_H_

#include <openssl/crypto.h>

#include <cstddef>
#include <string>

namespace mes {

/**
 * @brief Overwrites @p len bytes at @p data with a wipe the compiler may not
 *        drop.
 *
 * Storage that has held a plaintext credential is normally released or reused
 * immediately after being wiped, which makes the zeroing a dead store that a
 * plain `std::fill`, `memset` or hand-rolled loop is free to remove.
 * `OPENSSL_cleanse` is opaque to that analysis, so it is the one primitive
 * this header exposes and the one place in the tree that names it; reaching it
 * through this inline wrapper preserves the guarantee, since the call still
 * lands in a function the optimizer cannot see through.
 *
 * A null @p data or a zero @p len wipes nothing.
 */
inline void SecureWipe(void* data, size_t len) {
  if (data == nullptr || len == 0) {
    return;
  }
  OPENSSL_cleanse(data, len);
}

/**
 * @brief Overwrites the bytes @p value currently holds.
 *
 * Wipes in place and leaves size and capacity alone. A caller that also wants
 * the secret gone from the allocator calls clear() and shrink_to_fit() itself
 * afterwards; doing it in that order is what keeps the wipe ahead of the
 * release.
 */
inline void SecureWipe(std::string& value) { SecureWipe(value.data(), value.size()); }

/**
 * @brief Wipes secret-bearing storage when it leaves scope.
 *
 * The destructor is what makes the wipe reach every exit path, including error
 * returns taken between the point the secret is written and the end of the
 * scope holding it.
 *
 * The `std::string` form stores the string rather than its data pointer and
 * length, and re-reads both at destruction: a guard is frequently declared
 * before the credential is assigned, and an assignment may reallocate, so a
 * pointer captured at construction could name storage the string no longer
 * owns.
 */
struct SecureCleanse {
  /** @brief Wipes @p len bytes at @p data at scope exit. */
  SecureCleanse(void* data, size_t len) : buf_(data), len_(len) {}

  /** @brief Wipes whatever bytes @p value holds at scope exit. */
  explicit SecureCleanse(std::string& value) : str_(&value) {}

  ~SecureCleanse() {
    if (str_ != nullptr) {
      SecureWipe(*str_);
    } else {
      SecureWipe(buf_, len_);
    }
  }

  SecureCleanse(const SecureCleanse&) = delete;
  SecureCleanse& operator=(const SecureCleanse&) = delete;
  SecureCleanse(SecureCleanse&&) = delete;
  SecureCleanse& operator=(SecureCleanse&&) = delete;

 private:
  void* buf_ = nullptr;
  size_t len_ = 0;
  std::string* str_ = nullptr;
};

}  // namespace mes

#endif  // MES_SECURE_CLEANSE_H_
