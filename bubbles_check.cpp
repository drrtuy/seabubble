#include <cstring>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>

#include "common.h"
#include "services.h"

namespace bpo = boost::program_options;

seastar::logger logger("bubbles_check");

seastar::future<offsets> calculate_offsets(seastar::file &input_file) {
  auto file_size = co_await input_file.size();
  const auto total_number_of_blocks = file_size / BLOCK_LEN;

  logger.debug("blocks for this shard {}", total_number_of_blocks);

  auto start_offset = 0;
  auto end_offset = total_number_of_blocks * BLOCK_LEN - 1;

  co_return offsets({start_offset, end_offset});
}

seastar::future<> run_check(seastar::sstring &input_file_name) {
  auto range = std::views::iota(0u, seastar::smp::count);

  logger.debug("open file with {}", input_file_name);

  auto flags = seastar::open_flags::ro;
  auto f = co_await open_file_dma(input_file_name, flags);
  auto [start_offset, end_offset] = co_await calculate_offsets(f);
  logger.debug("start_offset {} end_offset {}", start_offset, end_offset);
  auto cur_buf = co_await f.dma_read<char>(start_offset, BLOCK_LEN);

  do {
    start_offset += BLOCK_LEN;

    auto next_buf = co_await f.dma_read<char>(start_offset, BLOCK_LEN);
    if (block_cmp_greater()(cur_buf, next_buf)) {
      logger.error("file is not sorted with offending blocks {} {}",
                   start_offset - BLOCK_LEN, start_offset);
    }

    cur_buf = std::move(next_buf);
  } while (start_offset < end_offset - BLOCK_LEN);

  co_await f.close();
  co_return;
}

int main(int argc, char *argv[]) {
  seastar::app_template app;

  auto opt_add = app.add_options();
  opt_add("input_file_name,i", bpo::value<seastar::sstring>()->required(),
          "file name to output the gen data");

  return app.run(argc, argv, [&] {
    auto &config = app.configuration();
    auto input_file_name = config["input_file_name"].as<seastar::sstring>();
    auto flags = seastar::open_flags::ro;

    logger.debug("checking if file {} is sorted", input_file_name);
    return run_check(input_file_name);
  });
}
