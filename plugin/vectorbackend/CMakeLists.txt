cmake_minimum_required(VERSION 3.10)

set(PLUGIN_NAME vectorbackend)
set(SOURCES)
set(TESTS)

list(APPEND SOURCES
        compression/compression.cc
        memtable/hnsw_memtable_factory.cc
        memtable/hnsw_memtablerep.cc
        options/vcf_options.cc
        options/vector_options.cc
        options/vector_options_helper.cc
        table/binary_search_index_reader.cc
        table/hnsw_block.cc
        table/hnsw_block_builder.cc
        table/hnsw_block_prefetcher.cc
        table/hnsw_table_builder.cc
        table/hnsw_reader.cc
        table/hnsw_table_factory.cc
        table/hnsw_table_format.cc
        table/hnsw_table_iterator.cc
        table/hnsw_table_reader.cc
        table/hnsw_table_uncompression_dict_reader.cc
        table/index_builder.cc
        table/index_reader_common.cc
        vectorindex/hnswlib/hnswalg.cc
)

list(APPEND TESTS
        tests/vector_column_family_options_test.cc
        tests/vector_column_family_test.cc
)

set(${PLUGIN_NAME}_SOURCES ${SOURCES} PARENT_SCOPE)
set(${PLUGIN_NAME}_COMPILE_FLAGS PARENT_SCOPE)
set(${PLUGIN_NAME}_INCLUDE_PATHS PARENT_SCOPE)
set(${PLUGIN_NAME}_LIBS PARENT_SCOPE)
set(${PLUGIN_NAME}_LINK_PATHS PARENT_SCOPE)
set(${PLUGIN_NAME}_CMAKE_SHARED_LINKER_FLAGS PARENT_SCOPE)
set(${PLUGIN_NAME}_CMAKE_EXE_LINKER_FLAGS PARENT_SCOPE)
set(${PLUGIN_NAME}_TESTS ${TESTS} PARENT_SCOPE)
