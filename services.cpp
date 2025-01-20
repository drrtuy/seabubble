#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/queue.hh>

#include "common.h"
#include "services.h"

extern seastar::logger logger;

seastar::sstring inline sorted_run_file_name(
    const seastar::sstring &tmp_dir_path, const unsigned int tmp_file_id) {
  return tmp_dir_path + "/sorted_run_" +
         std::to_string(seastar::this_shard_id()) + "_" +
         std::to_string(tmp_file_id);
}

seastar::future<> write_sorted_run(blocks_pq &pq,
                                   const seastar::sstring &tmp_dir_path,
                                   unsigned &tmp_file_id) {
  auto temp_file_name = sorted_run_file_name(tmp_dir_path, tmp_file_id);
  auto f = co_await seastar::open_file_dma(
      temp_file_name, seastar::open_flags::wo | seastar::open_flags::create);
  co_await f.allocate(0, pq.size() * BLOCK_LEN);

  uint64_t write_offset = 0;
  while (!pq.empty()) {
    co_await f.dma_write<char>(write_offset, pq.top().get(), BLOCK_LEN);
    write_offset += BLOCK_LEN;
    pq.pop();
  }

  co_await f.close();
}

seastar::future<offsets> calculate_offsets(seastar::file &input_file) {
  auto file_size = co_await input_file.size();
  const auto total_number_of_blocks = file_size / BLOCK_LEN;
  auto blocks_for_this_shard = total_number_of_blocks / seastar::smp::count;
  const auto this_shard_id = seastar::this_shard_id();
  auto start_offset_block = this_shard_id * blocks_for_this_shard;
  auto blocks_remained = total_number_of_blocks % seastar::smp::count;

  if (this_shard_id == 0) {
    blocks_for_this_shard += blocks_remained;
  } else {
    start_offset_block += blocks_remained;
  }

  logger.info("blocks for this shard {}", blocks_for_this_shard);

  auto start_offset = start_offset_block * BLOCK_LEN;
  auto end_offset = start_offset + (blocks_for_this_shard * BLOCK_LEN) - 1;

  co_return offsets({start_offset, end_offset});
}

seastar::future<> sorted_runs_service::run() {
  input_file =
      co_await seastar::open_file_dma(input_file_path, seastar::open_flags::ro);

  auto [start_offset, end_offset] = co_await calculate_offsets(input_file);
  logger.info("sorted_runs_service::run(): start offset {} and end offset {}",
              start_offset, end_offset);

  blocks_pq pq;
  size_t num_of_blocks = 0;

  while (start_offset < end_offset) {
    while (start_offset < end_offset) {
      try {
        auto block =
            co_await input_file.dma_read<char>(start_offset, BLOCK_LEN);
        start_offset += BLOCK_LEN;
        pq.push(std::move(block));
      } catch (const std::bad_alloc &e) {
        break;
      } catch (...) {
        logger.error(
            "sorted_runs_service::run(): main loop failed with exception {}",
            std::current_exception());
        break;
      }
    }

    num_of_blocks += pq.size();

    co_await write_sorted_run(pq, tmp_dir_path, current_tmp_file_id);
    ++current_tmp_file_id;
  }
  logger.info("sorted_runs_service::run(): {} blocks sorted into {} runs.",
              num_of_blocks, current_tmp_file_id);

  co_await input_file.close();
}

seastar::future<> sorted_runs_service::stop() {
  logger.info("sorted_runs_service::stop()");

  if (input_file) {
    co_await input_file.close();
  }
}
