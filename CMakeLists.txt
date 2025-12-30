cmake_minimum_required(VERSION 3.16)

project(ModularDP LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(modulardp INTERFACE)
target_include_directories(modulardp INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

option(MODULARDP_ENABLE_TSAN "Enable ThreadSanitizer (-fsanitize=thread) for tests/examples" OFF)
if (MODULARDP_ENABLE_TSAN)
    message(STATUS "ModularDP: ThreadSanitizer enabled")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        target_compile_options(modulardp INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(modulardp INTERFACE -fsanitize=thread)
    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        message(WARNING "ModularDP: TSAN enabled with GCC. If you hit 'ThreadSanitizer: unexpected memory mapping', prefer clang++ TSAN (CI uses clang).")
        # 某些环境下 GCC TSAN 会因为 PIE/地址空间布局在启动时直接 FATAL，这里尝试禁用 PIE 提高兼容性。
        target_compile_options(modulardp INTERFACE -fsanitize=thread -fno-omit-frame-pointer -fno-pie)
        target_link_options(modulardp INTERFACE -fsanitize=thread -no-pie)
    else()
        message(WARNING "ModularDP: TSAN requested but compiler '${CMAKE_CXX_COMPILER_ID}' is not explicitly supported. Trying generic flags.")
        target_compile_options(modulardp INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(modulardp INTERFACE -fsanitize=thread)
    endif()
endif()

add_executable(example_basic_pipeline examples/basic_pipeline.cpp)
target_link_libraries(example_basic_pipeline PRIVATE modulardp)

#
# User executables (out-of-tree apps should live under executables/)
#
add_subdirectory(executables)

#
# User modules (reusable components should live under modules/)
#
add_subdirectory(modules)

enable_testing()

add_executable(test_module tests/test_module.cpp)
target_link_libraries(test_module PRIVATE modulardp)
add_test(NAME module COMMAND test_module)

add_executable(test_stress tests/test_stress.cpp)
target_link_libraries(test_stress PRIVATE modulardp)
add_test(NAME stress COMMAND test_stress)
set_tests_properties(stress PROPERTIES TIMEOUT 20)

add_executable(test_file_modules tests/test_file_modules.cpp)
target_link_libraries(test_file_modules PRIVATE modulardp modulardp_file_source modulardp_file_manager)
add_test(NAME file_modules COMMAND test_file_modules)


