#
# User-facing executables should live under this directory.
#
# Recommended layout:
#
# executables/
#   my_app/
#     CMakeLists.txt
#     main.cpp
#
# In my_app/CMakeLists.txt, you typically do:
#
#   add_executable(my_app main.cpp)
#   target_link_libraries(my_app PRIVATE modulardp)
#

message(STATUS "ModularDP: configuring user executables under '${CMAKE_CURRENT_SOURCE_DIR}'")


