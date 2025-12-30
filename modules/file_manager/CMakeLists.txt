cmake_minimum_required(VERSION 3.16)

add_library(modulardp_file_manager STATIC
    file_manager_module.cpp
)

target_link_libraries(modulardp_file_manager
    PUBLIC
        modulardp
)

target_include_directories(modulardp_file_manager
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

if(UNIX)
    find_package(Threads REQUIRED)
    target_link_libraries(modulardp_file_manager PUBLIC Threads::Threads)
endif()

target_compile_options(modulardp_file_manager PRIVATE -Wall -Wextra -Wpedantic)



