cmake_minimum_required(VERSION 3.10)
project(util)

set(CMAKE_CXX_STANDARD 17)


find_package(OpenSSL REQUIRED)


add_executable(HttpClientTest HttpClientTest.cpp)
target_link_libraries(HttpClientTest PRIVATE
    curl
    z
    OpenSSL::SSL
    OpenSSL::Crypto
)

add_executable(HttpDownloaderTest HttpDownloaderTest.cpp)
target_link_libraries(HttpDownloaderTest PRIVATE
    curl
    z
    OpenSSL::SSL
    OpenSSL::Crypto
)
