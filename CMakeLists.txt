cmake_minimum_required(VERSION 3.0)
project(rdma_inverted_index)

# set module path and compiler standard
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR})
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# set compiler options
add_compile_options(-Wall -Wextra -Wpedantic -Wfatal-errors)
# add_compile_options(-DVERIFY)       # checks whether the insert queries did not mess up the index
# add_compile_options(-DNOHUGEPAGES)  # disables hugepages
# add_compile_options(-DDEV_DEBUG)    # outputs additional debugging information

# add directories
include_directories(src)
add_subdirectory(rdma-library)
add_subdirectory(src/data_processing)

# executables
add_executable(term_index src/index/term_based/main.cc src/timing/timing.cc)
target_link_libraries(term_index rdma_library)

add_executable(document_index src/index/document_based/main.cc src/timing/timing.cc)
target_link_libraries(document_index rdma_library)

add_executable(block_index src/index/block_based/main.cc src/timing/timing.cc)
target_link_libraries(block_index rdma_library)

add_executable(dynamic_block_index src/index/block_based_dynamic/main.cc src/timing/timing.cc)
target_link_libraries(dynamic_block_index rdma_library)