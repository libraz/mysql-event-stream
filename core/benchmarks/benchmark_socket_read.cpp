// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file benchmark_socket_read.cpp
 * @brief Cost of the plaintext ReadExact read-ahead staging buffer.
 *
 * A feeder thread blasts a fixed volume over loopback TCP while the reader
 * pulls it in fixed-size chunks. Each chunk size is measured twice: once
 * through SocketHandle::ReadExact, which stages every byte through its 64 KiB
 * read-ahead buffer, and once through a bare recv() loop on an identical
 * connection. The gap between the two is what removing the staging copy could
 * recover; anything smaller than the run-to-run spread is not worth chasing.
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "protocol/mysql_socket.h"

namespace {

constexpr size_t kStreamBytes = 256u * 1024u * 1024u;
constexpr size_t kRepeats = 5;

/// Loopback listener that writes `kStreamBytes` to whatever connects.
class Feeder {
 public:
  bool Start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return false;
    int one = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
    if (listen(listen_fd_, 4) != 0) return false;
    socklen_t len = sizeof(addr);
    if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) return false;
    port_ = ntohs(addr.sin_port);
    return true;
  }

  uint16_t port() const { return port_; }

  /// Accept one connection and push `total` bytes at it, then close.
  std::thread ServeOnce(size_t total) {
    return std::thread([this, total]() {
      const int fd = accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) return;
      int one = 1;
      setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
      std::vector<uint8_t> chunk(256u * 1024u, 0xAB);
      size_t sent = 0;
      while (sent < total) {
        const size_t want = std::min(chunk.size(), total - sent);
        const ssize_t n = send(fd, chunk.data(), want, 0);
        if (n <= 0) break;
        sent += static_cast<size_t>(n);
      }
      close(fd);
    });
  }

  ~Feeder() {
    if (listen_fd_ >= 0) close(listen_fd_);
  }

 private:
  int listen_fd_ = -1;
  uint16_t port_ = 0;
};

double MeasureReadExact(Feeder& feeder, size_t chunk_bytes, size_t total) {
  std::thread server = feeder.ServeOnce(total);
  mes::protocol::SocketHandle socket;
  if (socket.Connect("127.0.0.1", feeder.port(), 5) != MES_OK) {
    std::cerr << "connect failed\n";
    std::exit(2);
  }
  std::vector<uint8_t> buf(chunk_bytes);
  const auto started = std::chrono::steady_clock::now();
  size_t read = 0;
  while (read + chunk_bytes <= total) {
    if (socket.ReadExact(buf.data(), chunk_bytes) != MES_OK) break;
    read += chunk_bytes;
  }
  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
  socket.Shutdown();
  server.join();
  return static_cast<double>(read) / seconds;
}

double MeasureRawRecv(Feeder& feeder, size_t chunk_bytes, size_t total) {
  std::thread server = feeder.ServeOnce(total);
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(feeder.port());
  if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    std::cerr << "raw connect failed\n";
    std::exit(2);
  }
  std::vector<uint8_t> buf(chunk_bytes);
  const auto started = std::chrono::steady_clock::now();
  size_t read = 0;
  while (read + chunk_bytes <= total) {
    size_t got = 0;
    while (got < chunk_bytes) {
      const ssize_t n = recv(fd, buf.data() + got, chunk_bytes - got, 0);
      if (n <= 0) break;
      got += static_cast<size_t>(n);
    }
    if (got < chunk_bytes) break;
    read += chunk_bytes;
  }
  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
  close(fd);
  server.join();
  return static_cast<double>(read) / seconds;
}

}  // namespace

double Median(std::vector<double> samples) {
  std::sort(samples.begin(), samples.end());
  const size_t mid = samples.size() / 2;
  if (samples.size() % 2 == 1) return samples[mid];
  return (samples[mid - 1] + samples[mid]) / 2.0;
}

int main() {
  Feeder feeder;
  if (!feeder.Start()) {
    std::cerr << "failed to open loopback listener\n";
    return 1;
  }

  // 64 B is a small binlog packet payload, 16 KiB a large row event, and the
  // two sizes at and above the 64 KiB read-ahead capacity are where a direct
  // recv bypass would take effect.
  const size_t chunk_sizes[] = {64, 16u * 1024u, 64u * 1024u, 1024u * 1024u};
  std::cout << "mode=socket_read stream_bytes=" << kStreamBytes << " repeats=" << kRepeats << '\n';
  for (size_t chunk : chunk_sizes) {
    // One untimed pass per path so loopback buffers and clock ramp settle.
    MeasureReadExact(feeder, chunk, kStreamBytes);
    MeasureRawRecv(feeder, chunk, kStreamBytes);

    std::vector<double> staged;
    std::vector<double> raw;
    for (size_t r = 0; r < kRepeats; ++r) {
      staged.push_back(MeasureReadExact(feeder, chunk, kStreamBytes));
      raw.push_back(MeasureRawRecv(feeder, chunk, kStreamBytes));
    }
    const double staged_median = Median(staged);
    const double raw_median = Median(raw);
    std::cout << std::fixed << std::setprecision(2) << "chunk_bytes=" << chunk
              << " read_exact_MB_per_s=" << staged_median / 1e6 << " read_exact_spread_MB_per_s="
              << (*std::max_element(staged.begin(), staged.end()) -
                  *std::min_element(staged.begin(), staged.end())) /
                     1e6
              << " raw_recv_MB_per_s=" << raw_median / 1e6
              << " read_exact_vs_raw=" << staged_median / raw_median << '\n';
  }
  return 0;
}
