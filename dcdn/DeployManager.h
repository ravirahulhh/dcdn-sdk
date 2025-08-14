#ifndef _DCDN_SDK_DEPLOY_MANAGER_H_
#define _DCDN_SDK_DEPLOY_MANAGER_H_

#include <nlohmann/json.hpp>
#include <sqlite_orm/sqlite_orm.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "BaseManager.h"
#include "DownloadManager.h"
#include "EventLoop.h"
#include "FileManager.h"
#include "common/Common.h"
#include "util/FileHash.h"

NS_BEGIN(dcdn)

enum class DeployStatus : int
{
    DOWNLOADING = 1,
    COMPLETED = 2,
    FAILED = 3
};

struct DeployTask
{
    std::string jobId;
    std::string fileHash;
    std::string url;
    uint64_t blockStart = 0;
    uint64_t blockEnd = 0;
    std::string blockHash;
    DeployStatus status = DeployStatus::DOWNLOADING;
    std::string downloadPath;
    uint64_t createTime = 0;
    uint64_t updateTime = 0;
};

// sqlite_orm 存储映射
inline auto makeDeployStorage(const std::string& filename)
{
    using namespace sqlite_orm;
    return make_storage(
        filename,
        make_table(
            "deploy_tasks",
            make_column("job_id", &DeployTask::jobId, primary_key()),
            make_column("file_hash", &DeployTask::fileHash),
            make_column("url", &DeployTask::url),
            make_column("block_start", &DeployTask::blockStart),
            make_column("block_end", &DeployTask::blockEnd),
            make_column("block_hash", &DeployTask::blockHash),
            // 关键修改：强制 NOT NULL + 默认值，确保 extractor 不会遇到 NULL
            make_column("status", &DeployTask::status, not_null(), default_value(1)),
            make_column("download_path", &DeployTask::downloadPath),
            make_column("create_time", &DeployTask::createTime),
            make_column("update_time", &DeployTask::updateTime)));
}

using DeployStorage = decltype(makeDeployStorage(""));
using DeployStoragePtr = std::shared_ptr<DeployStorage>;

struct DeployManagerOption
{
    std::shared_ptr<FileManager> fileMgr;
    std::shared_ptr<DownloadManager> downloadMgr;
};

class DeployManager: public BaseManager, public EventLoop<DeployManager>
{
public:
    using json = nlohmann::json;

    explicit DeployManager(MainManager* man);
    ~DeployManager() override;

    // 新增初始化方法，通过option注入依赖
    int Init(const DeployManagerOption& opt);

private:
    friend class EventLoop<DeployManager>;

    // 新增：主动初始化数据库
    bool initDB();
    int createTable();
    DeployStoragePtr getDB(); // 简化版getDB

    void run() override;
    void handleDeployMsgEvent(std::shared_ptr<Event> evt);
    void checkDownloadStatus();

    bool saveDeployTask(const DeployTask& task);
    bool updateDeployTaskStatus(const std::string& jobId, DeployStatus status);
    std::vector<DeployTask> loadDownloadingTasks();
    void resubmitDownloadTasks();

    void reportToServer(const std::string& jobId, bool success);

    uint64_t getCurrentTimestamp() const
    {
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    std::filesystem::path dbPath() const
    {
        std::filesystem::path dbFile(mMan->Option().WorkDir);
        dbFile.append("deploy.db");
        return dbFile;
    }

    int calculateFileHash(const std::string& filePath, std::string& hashResult)
    {
        // 直接调用FileHash::calculate，保持相同的返回值和参数结构
        int result = FileHash::calculate(filePath, hashResult);

        // 可以在这里添加额外的日志或处理逻辑
        if (result != ErrorCodeOk) {
            logError << "Failed to calculate file hash for: " << filePath << ", error code: " << result;
        }

        return result; // 返回与内部调用相同的错误码
    }

    int getFileSize(const std::string& filePath, uint64_t& fileSize)
    {
        int result = FileHash::getFileSize(filePath, fileSize);

        // 添加额外的日志记录
        if (result != ErrorCodeOk) {
            logError << "Failed to get file size for: " << filePath << ", error code: " << result;
        }

        return result; // 返回原始错误码
    }

private:
    DeployStoragePtr mDB;
    std::shared_ptr<FileManager> mFileMgr;
    std::shared_ptr<DownloadManager> mDownloadMgr;
    util::HttpClient mClient;
    std::mutex mTaskMutex;
    std::mutex mDbMutex; // 普通锁即可，无需递归锁
    std::unordered_map<std::string, uint64_t> mJobToTaskMap;
    bool mInited = false; // 标记是否已初始化
};

NS_END

#endif // _DCDN_SDK_DEPLOY_MANAGER_H_
