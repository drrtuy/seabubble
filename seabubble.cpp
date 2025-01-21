#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>

#include "services.h"

seastar::logger logger("seabubble");

seastar::future<> sort(const paths &paths) {
  seastar::sharded<sorted_runs_service> sorted_runs_service_sharded;
  seastar::sharded<merging_service> merging_service_sharded;
  seastar::sharded<merging_service> final_merging_service_sharded;

  try {
    logger.info("Open the input file : {}", paths.input_file_path);

    co_await sorted_runs_service_sharded.start(paths);

    logger.info("Started sorted runs phase");

    co_await sorted_runs_service_sharded.invoke_on_all(
        [](auto &srs) { return srs.run(); });

    logger.info("Completed sorted runs phase");

    auto number_of_tmp_files = [](const sorted_runs_service &srs) {
      return srs.number_of_tmp_files();
    };

    co_await merging_service_sharded.start(
        paths, seastar::sharded_parameter(
                   number_of_tmp_files, std::ref(sorted_runs_service_sharded)));

    logger.info("Started merging phase");
    co_await merging_service_sharded.invoke_on_all(
        [](auto &ms) { return ms.run(); });

    logger.info("Completed merging phase");

    co_await final_merging_service_sharded.start(paths, seastar::smp::count);

    auto final_shard_id = seastar::smp::count > 1 ? 1 : 0;
    
    co_await final_merging_service_sharded.invoke_on(
        final_shard_id, [](merging_service &fms) {
          bool is_final_merge = true;
          return fms.run(is_final_merge);
        });
    logger.info("Completed final merge phase");
    logger.info("Completed sorting the file : {} into {}",
                paths.input_file_path, paths.output_file_path);

  } catch (...) {
    logger.error("Generic exception has happend : {}",
                 std::current_exception());
  }

  co_await sorted_runs_service_sharded.stop();
  co_await merging_service_sharded.stop();
  co_await final_merging_service_sharded.stop();
};

void add_app_config_options(seastar::app_template &app) {
  app.add_options()(
      "tmp-dir-path,t",
      boost::program_options::value<std::filesystem::path>()->required(),
      "Directory to store intermediate files.")(
      "input-file-path,i",
      boost::program_options::value<std::string>()->required(),
      "Input file path.");
}

seastar::future<> check_paths_and_run_app(const paths &paths) {
  assert(paths.tmp_dir_path.size() > 0);

  logger.info("Input file path: {}", paths.input_file_path);
  logger.info("Output file path: {}", paths.output_file_path);
  logger.info("Temp dir path: {}", paths.tmp_dir_path);

  co_await sort(paths);

  co_await cleanup_tmp_dir(paths.tmp_dir_path);
}

int main(int argc, char **argv) {
  seastar::app_template app;
  add_app_config_options(app);

  app.run(argc, argv, [&app] -> seastar::future<> {
    auto paths = co_await make_paths(app);

    co_await check_paths_and_run_app(paths);
  });
}
