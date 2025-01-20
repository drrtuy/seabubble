#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>

#include <seastar/util/file.hh>

extern seastar::logger logger;

std::string create_tmp_dir_path(std::filesystem::path tmp_dir) {
  auto new_tmp_suff = tmp_dir / "XXXXXX";
  char tmp_dir_template[new_tmp_suff.native().size() + 1];
  strcpy(tmp_dir_template, new_tmp_suff.c_str());

  const char *tmp_dir_path = mkdtemp(tmp_dir_template);
  if (!tmp_dir_path) {
    logger.error("Failed to create temporary directory with errno : {}",
                 strerror(errno));
    return {};
  }
  return {tmp_dir_path};
}

seastar::future<> cleanup_tmp_dir(const std::string &tmp_dir_path) {
  return seastar::recursive_remove_directory(tmp_dir_path.c_str());
}

class paths {
public:
  paths(seastar::app_template &app) {
    auto &args = app.configuration();
    input_file_path = args["input-file-path"].as<std::string>();
    output_file_path = input_file_path + ".sorted";
    tmp_dir_path =
        create_tmp_dir_path(args["tmp-dir-path"].as<std::filesystem::path>());
  }

  std::string input_file_path{};
  std::string output_file_path{};
  std::string tmp_dir_path{};
};

seastar::future<paths>
make_paths(seastar::app_template &app) {
  co_return paths(app);
}
