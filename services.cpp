#include <ranges>
#include <seastar/core/future.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>

#include "common.h"
#include "services.h"

extern seastar::logger logger;

seastar::sstring inline sorted_run_file_name(
    const seastar::sstring &tmp_dir_path, const unsigned int tmp_file_id) {
  return tmp_dir_path + "/sorted_run_" +
         std::to_string(seastar::this_shard_id()) + "_" +
         std::to_string(tmp_file_id);
}

seastar::sstring inline merging_file_name(const seastar::sstring &tmp_dir_path,
                                          const unsigned int tmp_file_id) {
  return tmp_dir_path + "/merging_" + std::to_string(tmp_file_id);
}

seastar::future<> write_sorted_run(blocks_pq &pq,
                                   const seastar::sstring &tmp_dir_path,
                                   unsigned &tmp_file_id) {
  auto tmp_file_name = sorted_run_file_name(tmp_dir_path, tmp_file_id);
  auto tmp_file = co_await seastar::open_file_dma(
      tmp_file_name, seastar::open_flags::wo | seastar::open_flags::create);
  co_await tmp_file.allocate(0, pq.size() * BLOCK_LEN);

  uint64_t write_offset = 0;
  while (!pq.empty()) {
    co_await tmp_file.dma_write<char>(write_offset, pq.top().get(), BLOCK_LEN);
    write_offset += BLOCK_LEN;
    pq.pop();
  }

  co_await tmp_file.close();
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

  logger.debug("blocks for this shard {}", blocks_for_this_shard);

  auto start_offset = start_offset_block * BLOCK_LEN;
  auto end_offset = start_offset + (blocks_for_this_shard * BLOCK_LEN) - 1;

  co_return offsets({start_offset, end_offset});
}

seastar::future<> sorted_runs_service::run() {
  input_file =
      co_await seastar::open_file_dma(input_file_path, seastar::open_flags::ro);

  auto [start_offset, end_offset] = co_await calculate_offsets(input_file);
  logger.debug("sorted_runs_service::run(): start offset {} and end offset {}",
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
  logger.debug("sorted_runs_service::run(): {} blocks sorted into {} runs.",
               num_of_blocks, current_tmp_file_id);

  co_await input_file.close();
}

seastar::future<> sorted_runs_service::stop() {
  logger.debug("sorted_runs_service::stop()");

  if (input_file) {
    co_await input_file.close();
  }
}

seastar::future<>
start_queue_producers(seastar::queue<block> &blocks_queue,
                      seastar::semaphore &blocks_queues_consumed,
                      seastar::sstring tmp_file_name) {
  auto queue_input_file =
      co_await seastar::open_file_dma(tmp_file_name, seastar::open_flags::ro);
  auto end_offset = co_await queue_input_file.size();

  uint64_t start_offset = 0;
  while (start_offset < end_offset) {
    auto b = co_await queue_input_file.dma_read<char>(start_offset, BLOCK_LEN);
    co_await blocks_queue.push_eventually(std::move(b));
    start_offset += BLOCK_LEN;
  }

  co_await blocks_queue.push_eventually(std::move(block()));

  co_await blocks_queues_consumed.wait();

  co_await queue_input_file.close();
  co_await seastar::remove_file(tmp_file_name);
}

seastar::future<> merging_service::run(bool is_final_merge) {
  logger.debug("merging_service::run(): number of files {}", number_of_files);

  std::vector<seastar::queue<block>> blocks_queues;
  for (unsigned i = 0; i < number_of_files; ++i) {
    blocks_queues.emplace_back(READ_QUEUE_SIZE);
  }
  seastar::semaphore queue_read_finished{0};

  auto &input_file_path = paths_.input_file_path;
  auto &tmp_dir_path = paths_.tmp_dir_path;
  auto tmp_file_name_fn =
      (is_final_merge) ? merging_file_name : sorted_run_file_name;

  auto queue_producer_co = seastar::coroutine::lambda(
      [&blocks_queues, &queue_read_finished, tmp_dir_path,
       tmp_file_name_fn](unsigned int file_id) -> seastar::future<> {
        auto tmp_file_name = tmp_file_name_fn(tmp_dir_path, file_id);
        auto &block_queue = blocks_queues[file_id];
        return start_queue_producers(block_queue, queue_read_finished,
                                     tmp_file_name);
      });

  auto range = std::views::iota(0u, number_of_files);
  auto producers_future =
      seastar::parallel_for_each(range, std::move(queue_producer_co));

  block_generators_pq pq;
  co_await seastar::parallel_for_each(
      range,
      [&pq, &blocks_queues, this](unsigned batch_file_id) -> seastar::future<> {
        seastar::queue<block> &block_queue = blocks_queues[batch_file_id];
        auto b = co_await block_queue.pop_eventually();
        pq.emplace(std::move(b), batch_file_id);
      });

  auto output_filename =
      (is_final_merge)
          ? seastar::sstring(paths_.output_file_path)
          : merging_file_name(paths_.tmp_dir_path, seastar::this_shard_id());

  logger.debug("merging_service output_filename {}", output_filename);
  auto output_file = co_await seastar::open_file_dma(
      output_filename, seastar::open_flags::wo | seastar::open_flags::create);

  uint64_t write_offset = 0;
  while (!pq.empty()) {
    auto &top_block = pq.top();
    co_await output_file.dma_write<char>(write_offset, top_block.first.get(),
                                         BLOCK_LEN);
    write_offset += BLOCK_LEN;

    auto queue_id = top_block.second;
    pq.pop();

    seastar::queue<block> &block_queue = blocks_queues[queue_id];
    auto b = co_await block_queue.pop_eventually();
    if (!b.empty()) {
      pq.emplace(std::move(b), queue_id);
    }
  }

  queue_read_finished.signal(number_of_files);
  co_await std::move(producers_future);

  blocks_queues.clear();
  co_await output_file.close();

  co_await seastar::sync_directory(paths_.tmp_dir_path);

  logger.debug("merging_service::run(): completed");
}

seastar::future<> merging_service::stop() {
  logger.debug("merging_service::stop()");
  co_return;
}