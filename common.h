#pragma once

#include <cstdint>
#include <utility>

#include <seastar/core/file.hh>

using offsets = std::pair<uint64_t, uint64_t>;

constexpr uint64_t BLOCK_LEN = 4096ULL;

using block = seastar::temporary_buffer<char>;

constexpr size_t READ_QUEUE_SIZE = 200;
