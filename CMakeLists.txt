cmake_minimum_required(VERSION 3.15)
project(dcdn VERSION 1.0)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(
        -Wthread-safety
        -Werror=thread-safety
    )
endif()

# 默认构建类型为 Debug
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose the type of build." FORCE)
endif()

# setup C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/deps/src/libdatachannel-0.23.1/deps/plog/include
    ${CMAKE_SOURCE_DIR}/deps/src/libdatachannel-0.23.1/deps/json/include
)

set(NO_EXAMPLES ON CACHE BOOL "Disable building datachannel examples" FORCE)
set(CURL_DISABLE_LDAP ON CACHE BOOL "Disable LDAP support in libcurl" FORCE)
add_subdirectory(util)
add_subdirectory(dcdn)
add_subdirectory(deps/src/curl-8.15.0)
add_subdirectory(deps/src/libdatachannel-0.23.1)
add_subdirectory(deps/src/sqlite_orm-1.9.1)
