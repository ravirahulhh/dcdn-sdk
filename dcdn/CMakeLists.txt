cmake_minimum_required(VERSION 3.10)
project(dcdn)
set(CMAKE_CXX_STANDARD 17)
set(JSON_BuildTests OFF CACHE INTERNAL "")
find_package(OpenSSL REQUIRED)

# Source files
add_library(dcdn  STATIC 
    Cert.cpp		
    DownloadManager.cpp	
    FileManager.cpp		
    MainManager.cpp		
    UploadManager.cpp	
    WebSocketManager.cpp
    Config.cpp		
    # HttpDownloader.cpp	
    MainManagerTest.cpp	
    WebRtcManager.cpp
    p2p_downloader.cpp
    p2p_single_task.cpp
    DownloadManager.cpp
    
)

include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/deps/src/curl-8.15.0/include
    ${CMAKE_SOURCE_DIR}/deps/src/libdatachannel-0.23.1/include
    ${CMAKE_SOURCE_DIR}/deps/src/sqlite_orm-1.9.1/include
    ${CMAKE_SOURCE_DIR}/deps/src/libdatachannel-0.23.1/deps/plog/include
)

# Link libraries
target_link_libraries(dcdn PRIVATE
    curl
    datachannel
    usrsctp
    juice
    sqlite3
    z
    OpenSSL::SSL 
    OpenSSL::Crypto
)


add_executable(MainTest MainManagerTest.cpp)
target_link_libraries(MainTest PRIVATE dcdn)

# add_executable(p2p_downloader_test p2p_downloader_test.cpp)
# add_executable(fake_uploader p2p_test_peer_uploader.cpp)

# target_link_libraries(p2p_downloader_test PRIVATE dcdn)
# target_link_libraries(fake_uploader PRIVATE dcdn)

add_executable(dl_manager_test DownloadeManagerTest.cpp)
target_link_libraries(dl_manager_test PRIVATE dcdn)


