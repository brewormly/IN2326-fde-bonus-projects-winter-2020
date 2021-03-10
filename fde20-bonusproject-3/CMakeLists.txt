cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

set(CMAKE_C_COMPILER gcc)
set(CMAKE_CXX_COMPILER g++)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(fde20_bonusproject_3)

include(lib/gtest.cmake)
include(lib/benchmark.cmake)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -fPIC -Wall -Wextra -fno-omit-frame-pointer -march=native -Wno-unknown-pragmas")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wall -Wextra -fno-omit-frame-pointer -march=native")

### Tests
add_executable(test_all test/GraphTests.cpp)
target_link_libraries(test_all gtest gtest_main pthread)

### Performance Measurements for leaderboard
add_executable(bench benchmark/BenchmarkLeaderboard.cpp include/KNN.hpp include/Util.hpp)

### You can execute this binary for testing
add_executable(forTesting src/ForTesting.cpp include/KNN.hpp include/Util.hpp)