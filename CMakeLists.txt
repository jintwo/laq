cmake_minimum_required(VERSION 2.8)
project(laq)

set(CMAKE_BUILD_TYPE Debug)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99")
set(SOURCE_FILES main.c)
set(LIBS luajit avro m z dl)
set(VENDOR_PATH "${PROJECT_SOURCE_DIR}/vendor")

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(CMAKE_EXE_LINKER_FLAGS "-pagezero_size 10000 -image_base 100000000")
  # parson
  include_directories(${VENDOR_PATH}/parson)
  add_definitions(-DWITH_PARSON)
endif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(BUILD_SHARED_LIBRARIES OFF)
  set(CMAKE_EXE_LINKER_FLAGS "-static")
  # jansson
  include_directories(${VENDOR_PATH}/avro/lang/c/jansson/src)
endif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

# avro
include_directories(${VENDOR_PATH}/avro/lang/c/include)
link_directories(${VENDOR_PATH}/avro/lang/c/lib)

# luajit
include_directories(${VENDOR_PATH}/LuaJIT-2.0.4/src)
link_directories(${VENDOR_PATH}/LuaJIT-2.0.4/src)

# parson
add_library(parson OBJECT ${VENDOR_PATH}/parson/parson.c)

add_executable(laq ${SOURCE_FILES} $<TARGET_OBJECTS:parson>)
target_link_libraries(laq "${LIBS}")
