cmake_minimum_required(VERSION 3.10)
project(CSpell)

set(CMAKE_CXX_STANDARD 17)

include_directories(/mnt/c/Users/Antonio/AppData/Local/Programs/Python/Python310/lib/site-packages/pybind11/include)
include_directories(/mnt/c/Users/Antonio/AppData/Local/Programs/Python/Python310/Include)
#include_directories(C:Users/Antonio/AppData/Local/Programs/Python/Python310/lib/site-packages/pybind11/include)
#include_directories(C:Users/Antonio/AppData/Local/Programs/Python/Python310/Include)

add_executable(CSpell CSpell.cpp)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic -Wextra -fopenmp")
#add_definitions(-openmp)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic -Wextra -g -ggdb -O0")
find_package(Threads)
add_executable(CSpellParallel CSpellParallel.cpp)
target_link_libraries(CSpellParallel ${CMAKE_THREAD_LIBS_INIT})
#add_executable(wrapper wrapper.cpp)