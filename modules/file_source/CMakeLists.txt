cmake_minimum_required(VERSION 3.16)

add_library(modulardp_file_source STATIC
    file_source_module.cpp
)

target_link_libraries(modulardp_file_source
    PUBLIC
        modulardp
)

target_include_directories(modulardp_file_source
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

if(UNIX)
    find_package(Threads REQUIRED)
    target_link_libraries(modulardp_file_source PUBLIC Threads::Threads)
endif()

target_compile_options(modulardp_file_source PRIVATE -Wall -Wextra -Wpedantic)



