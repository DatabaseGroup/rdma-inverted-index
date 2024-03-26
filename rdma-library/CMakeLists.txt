cmake_minimum_required(VERSION 3.0)

set(IBVerbs_DIR .)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)

# required packages
find_package(IBVerbs REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)
find_package(Threads REQUIRED)
find_package(TBB REQUIRED)

# our library
file(GLOB_RECURSE LIBRARY_CC library/**.cc)
add_library(rdma_library STATIC ${LIBRARY_CC})
target_include_directories(rdma_library PUBLIC .)
target_link_libraries(rdma_library ${IBVERBS_LIBRARY} ${Boost_LIBRARIES} TBB::tbb Threads::Threads)
