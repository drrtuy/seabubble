#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>

#include "services.h"

seastar::logger logger("bubble");

seastar::future<> sort(const paths &paths) {
  seastar::sharded<sorted_runs_service> sorted_runs_service_sharded;
  seastar::file input_file, output_file;

  try {
    logger.debug("Open the input file : {}", paths.input_file_path);

    co_await sorted_runs_service_sharded.start(paths);

    logger.debug("Started sorted runs phase");

    co_await sorted_runs_service_sharded.invoke_on_all(
        [](sorted_runs_service &srs) { return srs.run(); });

    logger.debug("Completed sorted runs phase");

  } catch (...) {
    logger.error("Generic exception has happend : {}",
                 std::current_exception());
  }

  co_await sorted_runs_service_sharded.stop();

  if (input_file) {
    co_await input_file.close();
  }
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
