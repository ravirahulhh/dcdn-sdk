#ifndef _DCDN_SDK_FILE_MANAGER_H_
#define _DCDN_SDK_FILE_MANAGER_H_

#include <thread>

#include "BaseManager.h"
#include "EventLoop.h"
#include "util/HttpClient.h"

NS_BEGIN(dcdn)

struct FileItem
{
    uint64_t id;
    std::string block_hash;
    std::string file_hash;
    std::string file_path;
    uint64_t file_size;
    uint64_t block_start;
    uint64_t block_end;
    uint64_t last_report;
    std::string created_at;
};

class StorageRef;
class FileManager: public BaseManager, public EventLoop<FileManager>
{
public:
    FileManager(MainManager* man);
    ~FileManager();

private:
    void run();

    std::shared_ptr<StorageRef> getDB();
    int createTable();

    void handleEvent(std::shared_ptr<Event> evt);
    void handleAddFile(std::shared_ptr<Event> evt);

private:
    std::shared_ptr<StorageRef> mDB;
    util::HttpClient mClient;
};

NS_END

#endif
