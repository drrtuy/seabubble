#pragma once

#include <filesystem>
#include <seastar/core/app-template.hh>

class paths;

std::string create_tmp_dir_path(std::filesystem::path tmp_dir);

seastar::future<> cleanup_tmp_dir(const std::string &tmp_dir_path);

seastar::future<paths> make_paths(seastar::app_template &app);

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
