cmake_minimum_required(VERSION 2.8)
project(laq)

set(CMAKE_BUILD_TYPE Debug)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -gp -std=c99")

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(CMAKE_EXE_LINKER_FLAGS "-pagezero_size 10000 -image_base 100000000")
endif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(BUILD_SHARED_LIBRARIES OFF)
  set(CMAKE_EXE_LINKER_FLAGS "-static")
endif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

# parson
include_directories(vendor/parson)

# avro
include_directories(vendor/avro/lang/c/include)
link_directories(vendor/avro/lang/c/lib)

# luajit
include_directories(vendor/LuaJIT-2.0.4/src)
link_directories(vendor/LuaJIT-2.0.4/src)

set(SOURCE_FILES vendor/parson/parson.c main.c)
add_executable(laq ${SOURCE_FILES})
target_link_libraries(laq m luajit dl avro z)
