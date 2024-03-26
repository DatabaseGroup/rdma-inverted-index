cmake_minimum_required(VERSION 3.0)

add_executable(serializer serializer/serialize.cc)
target_link_libraries(serializer rdma_library)
set_target_properties(serializer PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/data_processing")

add_executable(partitioner partitioner/partitioner.cc ../timing/timing.cc)
target_link_libraries(partitioner rdma_library)
set_target_properties(partitioner PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/data_processing")

add_executable(create_documents update_queries/create_documents.cc)
target_link_libraries(create_documents rdma_library)
set_target_properties(create_documents PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/data_processing")

add_executable(draw_documents_and_create_index update_queries/draw_documents_and_create_index.cc)
target_link_libraries(draw_documents_and_create_index rdma_library)
set_target_properties(draw_documents_and_create_index PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/data_processing")
