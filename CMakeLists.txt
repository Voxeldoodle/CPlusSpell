cmake_minimum_required(VERSION 3.10)
project(CSpell)

set(CMAKE_CXX_STANDARD 17)

include_directories(/mnt/c/Users/Antonio/AppData/Local/Programs/Python/Python310/lib/site-packages/pybind11/include)
include_directories(/mnt/c/Users/Antonio/AppData/Local/Programs/Python/Python310/Include)
#include_directories(C:Users/Antonio/AppData/Local/Programs/Python/Python310/lib/site-packages/pybind11/include)
#include_directories(C:Users/Antonio/AppData/Local/Programs/Python/Python310/Include)

add_executable(CSpell CSpell.cpp)
#add_executable(wrapper wrapper.cpp)