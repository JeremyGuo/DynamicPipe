cmake_minimum_required(VERSION 3.16)

add_library(modulardp_json_decoder STATIC
    json_decoder_module.cpp
)

target_link_libraries(modulardp_json_decoder
    PUBLIC
        modulardp
        modulardp_file_manager
)

target_include_directories(modulardp_json_decoder
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

if(UNIX)
    find_package(Threads REQUIRED)
    target_link_libraries(modulardp_json_decoder PUBLIC Threads::Threads)
endif()

target_compile_options(modulardp_json_decoder PRIVATE -Wall -Wextra -Wpedantic)

