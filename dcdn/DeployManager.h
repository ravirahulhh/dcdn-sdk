#ifndef _DCDN_SDK_DEPLOY_MANAGER_H_
#define _DCDN_SDK_DEPLOY_MANAGER_H_

#include "BaseManager.h"
#include "EventLoop.h"
#include "FileManager.h"
#include "DownloadManager.h"
#include "util/HttpDownloader.h"
#include "common/Common.h"  // 包含BlockInfo定义
#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <vector>
#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>

NS_BEGIN(dcdn)

// 部署任务状态枚举
enum class DeployStatus {
    PENDING = 0,       // 待处理
    DOWNLOADING = 1,   // 下载中
    COMPLETED = 2,     // 完成
    FAILED = 3         // 失败
};

// 部署任务数据结构（与BlockInfo字段对应）
struct DeployTask {
    std::string job_id;          // 服务端任务唯一标识（主键）
    std::string file_hash;       // 文件整体哈希（对应BlockInfo::file_hash）
    std::string url;             // 下载URL
    uint64_t block_start = 0;    // 区块起始位置（对应BlockInfo::block_start）
    uint64_t block_end = 0;      // 区块结束位置（对应BlockInfo::block_end）
    std::string block_hash;      // 区块哈希（对应BlockInfo::block_hash）
    DeployStatus status = DeployStatus::PENDING;  // 任务状态
    std::string download_path;   // 下载文件路径
    uint64_t create_time = 0;    // 创建时间戳
    uint64_t update_time = 0;    // 更新时间戳
};

// 部署管理器类
class DeployManager : public BaseManager, public EventLoop<DeployManager> {
public:
    using json = nlohmann::json;
    explicit DeployManager(MainManager* man);
    ~DeployManager() override;

private:
    // 事件处理函数注册（友元声明）
    friend class EventLoop<DeployManager>;

    // 数据库初始化
    int createTable();

    // 获取数据库连接
    std::shared_ptr<StorageRef> getDB();

    // 主循环逻辑
    void run() override;

    // 处理部署任务事件
    void handleDeployMsgEvent(std::shared_ptr<Event> evt);

    // 检查下载状态
    void checkDownloadStatus();

    // 保存部署任务到数据库
    bool saveDeployTask(const DeployTask& task);

    // 更新任务状态
    bool updateDeployTaskStatus(const std::string& job_id, DeployStatus status);

    // 加载所有下载中任务
    std::vector<DeployTask> loadDownloadingTasks();

    // 重启时重新提交任务
    void resubmitDownloadTasks();

    // 向服务端上报结果
    void reportToServer(const std::string& job_id, bool success);

    // 获取当前时间戳（秒）
    uint64_t getCurrentTimestamp() {
        return std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

private:
    // 数据库路径
    std::filesystem::path dbPath() const {
        std::filesystem::path dbFile(mMan->Option().WorkDir);
        dbFile.append("deploy.db");
        return dbFile;
    }

private:
    std::shared_ptr<StorageRef> mDB;                  // 数据库连接
    std::shared_ptr<FileManager> mFileMgr;            // 文件管理器实例
    std::shared_ptr<DownloadManager> mDownloadMgr;    // 下载管理器实例
    util::HttpClient mClient;                         // HTTP客户端（用于上报）
    std::mutex mTaskMutex;                            // 任务映射表锁
    std::unordered_map<std::string, std::string> mJobToTaskMap;  // job_id -> 下载任务ID
};

// 数据库表映射（sqlite_orm）
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
using DeployStorageRef = std::shared_ptr<DeployStorage>;

NS_END

#endif  // _DCDN_SDK_DEPLOY_MANAGER_H_
