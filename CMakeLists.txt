cmake_minimum_required(VERSION 2.8)
project(laq)

set(CMAKE_BUILD_TYPE Debug)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99")
set(SOURCE_FILES main.c)
set(LIBS luajit avro m z dl)

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(CMAKE_EXE_LINKER_FLAGS "-pagezero_size 10000 -image_base 100000000")
  # parson
  include_directories(vendor/parson)
  add_definitions(-DWITH_PARSON)
  set(SOURCE_FILES "${SOURCE_FILES} vendor/parson/parson.c")
endif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(BUILD_SHARED_LIBRARIES OFF)
  set(CMAKE_EXE_LINKER_FLAGS "-static")
  # jansson
  include_directories(vendor/avro/lang/c/jansson/src)
endif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

# avro
include_directories(vendor/avro/lang/c/include)
link_directories(vendor/avro/lang/c/lib)

# luajit
include_directories(vendor/LuaJIT-2.0.4/src)
link_directories(vendor/LuaJIT-2.0.4/src)

add_executable(laq ${SOURCE_FILES})
target_link_libraries(laq ${LIBS})
