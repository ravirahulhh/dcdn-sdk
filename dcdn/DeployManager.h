#ifndef _DCDN_SDK_DEPLOY_MANAGER_H_
#define _DCDN_SDK_DEPLOY_MANAGER_H_

#include "BaseManager.h"
#include "EventLoop.h"
#include "FileManager.h"
#include "DownloadManager.h"
#include "common/Common.h" // 包含BlockInfo定义
#include <sqlite_orm/sqlite_orm.h>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <filesystem>

NS_BEGIN(dcdn)

enum class DeployStatus {
    PENDING = 0,
    DOWNLOADING = 1,
    COMPLETED = 2,
    FAILED = 3
};

struct DeployTask {
    std::string job_id;
    std::string file_hash;
    std::string url;
    uint64_t block_start = 0;
    uint64_t block_end = 0;
    std::string block_hash;
    DeployStatus status = DeployStatus::PENDING;
    std::string download_path;
    uint64_t create_time = 0;
    uint64_t update_time = 0;
};

// sqlite_orm 存储映射
inline auto makeDeployStorage(const std::string& filename) {
    using namespace sqlite_orm;
    return make_storage(
        filename,
        make_table(
            "deploy_tasks",
            make_column("job_id", &DeployTask::job_id, primary_key()),
            make_column("file_hash", &DeployTask::file_hash),
            make_column("url", &DeployTask::url),
            make_column("block_start", &DeployTask::block_start),
            make_column("block_end", &DeployTask::block_end),
            make_column("block_hash", &DeployTask::block_hash),
            make_column("status", &DeployTask::status),
            make_column("download_path", &DeployTask::download_path),
            make_column("create_time", &DeployTask::create_time),
            make_column("update_time", &DeployTask::update_time)
        )
    );
}

using DeployStorage = decltype(makeDeployStorage(""));
using DeployStoragePtr = std::shared_ptr<DeployStorage>;

class DeployManager : public BaseManager, public EventLoop<DeployManager> {
public:
    using json = nlohmann::json;

    explicit DeployManager(MainManager* man);
    ~DeployManager() override;

private:
    friend class EventLoop<DeployManager>;

    int createTable();
    DeployStoragePtr getDB();

    void run() override;
    void handleDeployMsgEvent(std::shared_ptr<Event> evt);
    void checkDownloadStatus();

    bool saveDeployTask(const DeployTask& task);
    bool updateDeployTaskStatus(const std::string& job_id, DeployStatus status);
    std::vector<DeployTask> loadDownloadingTasks();
    void resubmitDownloadTasks();

    void reportToServer(const std::string& job_id, bool success);

    uint64_t getCurrentTimestamp() const {
        return std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

    std::filesystem::path dbPath() const {
        std::filesystem::path dbFile(mMan->Option().WorkDir);
        dbFile.append("deploy.db");
        return dbFile;
    }

private:
    DeployStoragePtr mDB;
    std::shared_ptr<FileManager> mFileMgr;
    std::shared_ptr<DownloadManager> mDownloadMgr;
    util::HttpClient mClient;
    std::mutex mTaskMutex;
    std::unordered_map<std::string, std::string> mJobToTaskMap;
};

NS_END

#endif // _DCDN_SDK_DEPLOY_MANAGER_H_
