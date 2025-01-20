#include <cstddef>
#include <seastar/core/app-template.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>

#include "common.h"

namespace bpo = boost::program_options;

seastar::logger logger("bubbles_gen");

static auto random_seed =
    std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch())
        .count();
static thread_local std::default_random_engine random_generator(random_seed);

seastar::future<> truncate_file_to_size(seastar::file f, size_t size) {
  return f.truncate(size).then([f] mutable { return f.flush(); });
}

auto allocate_and_fill_buffer() {
  constexpr size_t alignment{BLOCK_LEN};
  auto buffer = seastar::allocate_aligned_buffer<char>(BLOCK_LEN, alignment);

  std::uniform_int_distribution<int> fill('@', '~');
  auto buffer_ptr = buffer.get();
  for (size_t i = 0; i < BLOCK_LEN; i++) {
    buffer_ptr[i] = fill(random_generator);
  }
  return buffer;
}

seastar::future<> run_fill(seastar::file f, seastar::sstring output_file_name,
                           size_t number_of_blocks, seastar::open_flags flags) {
  auto range = std::views::iota(0u, seastar::smp::count);
  return seastar::parallel_for_each(range, [&, f,
                                            number_of_blocks](auto id) mutable {
    auto range = std::views::iota(0u, number_of_blocks);
    return seastar::do_for_each(range, [&, f, output_file_name, id,
                                        number_of_blocks](auto run) mutable {
      auto wbuf = allocate_and_fill_buffer();

      size_t pos = id * BLOCK_LEN * number_of_blocks + run * BLOCK_LEN;

      return f.dma_write(pos, wbuf.get(), BLOCK_LEN).discard_result();
    });
  });
}

int main(int argc, char *argv[]) {
  seastar::app_template app;

  auto opt_add = app.add_options();
  opt_add("output_file_name,o",
          bpo::value<seastar::sstring>()->default_value("output.bin"),
          "file name to output the gen data")(
      "number_of_blocks,b", bpo::value<size_t>()->default_value(100),
      "how many blocks to gen for each shard");

  return app.run(argc, argv, [&] {
    auto &config = app.configuration();
    auto output_file_name = config["output_file_name"].as<seastar::sstring>();
    auto number_of_blocks = config["number_of_blocks"].as<size_t>();
    auto file_size = BLOCK_LEN * number_of_blocks * seastar::smp::count;
    auto flags = seastar::open_flags::rw | seastar::open_flags::create |
                 seastar::open_flags::truncate;

    logger.debug("generating {} blocks with {} shards",
                 number_of_blocks * seastar::smp::count, seastar::smp::count);

    return with_file(
        open_file_dma(output_file_name, flags),
        [output_file_name, number_of_blocks, flags,
         file_size](seastar::file f) {
          return truncate_file_to_size(f, file_size)
              .then([f, output_file_name, number_of_blocks, flags] mutable {
                return run_fill(f, output_file_name, number_of_blocks, flags);
              });
        });
  });
}
