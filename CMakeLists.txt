cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ros)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost)
find_package(lz4)
find_package(BZip2)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
add_definitions(-std=c++17)
include_directories(src/include)

set(EXTENSION_SOURCES 
  src/functions/ros_bag_functions.cpp 
  src/message_def_parser.cpp
  src/message_parser.cpp
  src/ros_reader_options.cpp
  src/ros_bag_reader.cpp
  src/ros_scanner.cpp
  src/ros_extension.cpp
  src/ros_transform.cpp
  src/ros_value.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto BZip2::BZip2 lz4::lz4)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto BZip2::BZip2 lz4::lz4)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
