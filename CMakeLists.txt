cmake_minimum_required(VERSION 3.16)
project(kuzu-cpp)

set(CMAKE_CXX_STANDARD 20)

# Add headers and libraries
include_directories(${CMAKE_SOURCE_DIR}/include)

add_executable(kuzu-cpp main.cpp)

# Find and link kuzu library
find_library(KUZU_LIB kuzu PATHS ${CMAKE_SOURCE_DIR}/lib REQUIRED)
target_link_libraries(kuzu-cpp PRIVATE ${KUZU_LIB})
