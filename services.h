#pragma once

#include <queue>
#include <seastar/core/file.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/sharded.hh>

#include "common.h"
#include "paths.h"

class block_cmp_greater {
public:
  bool operator()(block &x, block &y) {
    return strncmp(x.get(), y.get(), BLOCK_LEN) > 0;
  }
};

using blocks_pq =
    std::priority_queue<block, std::vector<block>, block_cmp_greater>;

using blocks_q_vector = std::vector<seastar::queue<block>>;
using block_queue_id = std::pair<block, unsigned>;

class block_queue_id_greater {
public:
  bool operator()(block_queue_id &x, block_queue_id &y) {
    return strncmp(x.first.get(), y.first.get(), BLOCK_LEN) > 0;
  }
};

using block_generators_pq =
    std::priority_queue<block_queue_id,
                        std::vector<block_queue_id>,
                        block_queue_id_greater>;

class sorted_runs_service : public seastar::sharded<sorted_runs_service> {
public:
  sorted_runs_service(const paths &paths)
      : input_file_path(paths.input_file_path),
        tmp_dir_path(paths.tmp_dir_path) {}

  seastar::future<> run();
  seastar::future<> stop();
  unsigned int number_of_tmp_files() const { return current_tmp_file_id; }

private:
  seastar::sstring tmp_dir_path;
  seastar::sstring input_file_path;
  seastar::file input_file{};
  unsigned current_tmp_file_id{0};
};

class merging_service : public seastar::sharded<merging_service> {

public:
  merging_service(const paths &paths, unsigned int number_of_files)
      : paths_(paths), number_of_files(number_of_files) {
  }

  seastar::future<> run(bool is_final_merge = false);
  seastar::future<> stop();

private:
  unsigned int number_of_files;
  const paths paths_;
};
