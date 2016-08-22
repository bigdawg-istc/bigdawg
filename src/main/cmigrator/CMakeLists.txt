# check: https://github.com/kaizouman/gtest-cmake-example/blob/master/CMakeLists.txt
cmake_minimum_required(VERSION 2.8)

project(cmigrator)

enable_testing()

add_subdirectory(src)
add_subdirectory(test)
