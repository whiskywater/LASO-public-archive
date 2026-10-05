cmake_minimum_required(VERSION 3.16)
project(laso_phase1_fixture LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(fixture
  src/label.cpp
  src/math.cpp
)
target_include_directories(fixture PUBLIC include)
target_compile_options(fixture PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(fixture_tests
  tests/fixture_tests.cpp
)
target_link_libraries(fixture_tests PRIVATE fixture)
target_compile_options(fixture_tests PRIVATE -Wall -Wextra -Wpedantic -Werror)

enable_testing()
add_test(NAME fixture_tests COMMAND fixture_tests)
