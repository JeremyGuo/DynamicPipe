#
# Reusable modules should live under this directory.
#
# Recommended layout:
#
# modules/
#   my_module/
#     CMakeLists.txt
#     my_module.h
#     my_module.cpp   # optional
#
# In modules/my_module/CMakeLists.txt, you typically do:
#
#   add_library(modulardp_my_module STATIC my_module.cpp)
#   target_link_libraries(modulardp_my_module PUBLIC modulardp)
#   target_include_directories(modulardp_my_module PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
#
# Or if header-only:
#
#   add_library(modulardp_my_module INTERFACE)
#   target_link_libraries(modulardp_my_module INTERFACE modulardp)
#   target_include_directories(modulardp_my_module INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
#

message(STATUS "ModularDP: configuring user modules under '${CMAKE_CURRENT_SOURCE_DIR}'")

add_subdirectory(file_source)
add_subdirectory(file_manager)
add_subdirectory(json_decoder)


