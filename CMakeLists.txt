
cmake_minimum_required(VERSION 3.28)

project(SeaBubble)

set(CMAKE_CXX_STANDARD 23)

set(SEASTAR_REPO_PATH "" CACHE PATH "Seastar runtime repo path")
if(SEASTAR_REPO_PATH STREQUAL "")
  message(FATAL_ERROR "Set -DSEASTAR_REPO_PATH=path")
else()
  list(APPEND CMAKE_MODULE_PATH "${SEASTAR_REPO_PATH}/cmake")

  list(APPEND CMAKE_PREFIX_PATH "${SEASTAR_REPO_PATH}/build/release")
  list(APPEND CMAKE_PREFIX_PATH "${SEASTAR_REPO_PATH}/build/release/_cooking/installed")
  find_package (Seastar REQUIRED)
endif()

add_executable (seabubble seabubble.cpp
  paths.cpp
  services.cpp
)
target_link_libraries (seabubble Seastar::seastar)

add_executable (bubbles_gen bubbles_gen.cpp)
target_link_libraries (bubbles_gen Seastar::seastar)

add_executable (bubbles_check bubbles_check.cpp)
target_link_libraries (bubbles_check Seastar::seastar)

