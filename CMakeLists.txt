cmake_minimum_required(VERSION 3.5)

set(VCPKG $ENV{VCPKG_ROOT})
set(CMAKE_TOOLCHAIN_FILE ${VCPKG}/scripts/buildsystems/vcpkg.cmake)

# Set extension name here
set(TARGET_NAME glue)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..

find_package(OpenSSL REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS s3 glue)
find_package(nlohmann_json CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(${AWSSDK_INCLUDE_DIRS})

set(EXTENSION_SOURCES src/glue_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto ${AWSSDK_LINK_LIBRARIES} nlohmann_json::nlohmann_json)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto ${AWSSDK_LINK_LIBRARIES} nlohmann_json::nlohmann_json)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
