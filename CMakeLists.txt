cmake_minimum_required(VERSION 3.10)
project(src)

set(CMAKE_CXX_STANDARD 17)

include_directories(/mnt/c/Users/Antonio/AppData/Local/Programs/Python/Python310/lib/site-packages/pybind11/include)
include_directories(/mnt/c/Users/Antonio/AppData/Local/Programs/Python/Python310/Include)
#include_directories(C:Users/Antonio/AppData/Local/Programs/Python/Python310/lib/site-packages/pybind11/include)
#include_directories(C:Users/Antonio/AppData/Local/Programs/Python/Python310/Include)

add_executable(src src/CSpell.cpp)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic -Wextra -fopenmp")
#add_definitions(-openmp)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic -Wextra -g -ggdb -O0")
find_package(Threads)
add_executable(CSpellParallel src/CSpellParallel.cpp)
add_executable(CSpellPipelined src/CSpellPipelined.cpp)
target_link_libraries(CSpellParallel ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(CSpellPipelined ${CMAKE_THREAD_LIBS_INIT})
#add_executable(wrapper wrapper.cpp)