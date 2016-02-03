cmake_minimum_required(VERSION 2.8)
project(laq)

set(CMAKE_BUILD_TYPE Debug)

set(SOURCE_FILES main.c utils.c)
set(LIBS uv pthread luajit avro m z dl)
set(VENDOR_PATH "${PROJECT_SOURCE_DIR}/vendor")

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99")
  set(CMAKE_EXE_LINKER_FLAGS "-pagezero_size 10000 -image_base 100000000")
endif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=gnu99")
  set(LIBS ${LIBS} rt)
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

# uv
include_directories(${VENDOR_PATH}/libuv/include)
link_directories(${VENDOR_PATH}/libuv/lib)

add_executable(laq ${SOURCE_FILES})
target_link_libraries(laq "${LIBS}")
