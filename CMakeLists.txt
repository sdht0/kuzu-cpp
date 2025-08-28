cmake_minimum_required(VERSION 3.31)
project(kuzu-cpp)

set(CMAKE_CXX_STANDARD 20)

include_directories(include)
link_directories(lib)

add_executable(kuzu-cpp main.cpp)
target_link_libraries(kuzu-cpp kuzu)
