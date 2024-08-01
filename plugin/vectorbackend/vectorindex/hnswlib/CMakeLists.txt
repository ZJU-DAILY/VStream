cmake_minimum_required (VERSION 3.5)
project(hnsw_lib LANGUAGES CXX)

add_library(hnswlib INTERFACE)
target_include_directories(hnswlib INTERFACE .)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    set(CMAKE_CXX_STANDARD 17)

    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
      SET( CMAKE_CXX_FLAGS  "-Ofast -DNDEBUG -std=c++17 -DHAVE_CXX0X -openmp -march=native -fpic -ftree-vectorize")
    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      SET( CMAKE_CXX_FLAGS  "-Ofast -lrt -DNDEBUG -std=c++17 -DHAVE_CXX0X -march=native -fpic -w -fopenmp -ftree-vectorize -ftree-vectorizer-verbose=0" )
    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
      SET( CMAKE_CXX_FLAGS  "-Ofast -lrt -DNDEBUG -std=c++17 -DHAVE_CXX0X -openmp -march=native -fpic -w -fopenmp -ftree-vectorize" )
    endif()

    # examples
    add_executable(example_search examples/cpp/example_search.cpp)

    add_executable(example_filter examples/cpp/example_filter.cpp)

    add_executable(example_replace_deleted examples/cpp/example_replace_deleted.cpp)

    add_executable(example_mt_search examples/cpp/example_mt_search.cpp)

    add_executable(example_mt_filter examples/cpp/example_mt_filter.cpp)

    add_executable(example_mt_replace_deleted examples/cpp/example_mt_replace_deleted.cpp)

    # tests
    add_executable(test_updates tests/cpp/updates_test.cpp)

    add_executable(searchKnnCloserFirst_test tests/cpp/searchKnnCloserFirst_test.cpp)

    add_executable(searchKnnWithFilter_test tests/cpp/searchKnnWithFilter_test.cpp)

    add_executable(multiThreadLoad_test tests/cpp/multiThreadLoad_test.cpp)

    add_executable(multiThread_replace_test tests/cpp/multiThread_replace_test.cpp)

    add_executable(main tests/cpp/main.cpp tests/cpp/sift_1b.cpp)

    # for extend
    add_executable(example_search_cos examples/cpp/example_search_cos.cpp)
endif()
