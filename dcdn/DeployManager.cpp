#include "DeployManager.h"

#include <openssl/md5.h>
#include <plog/Log.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <thread>

#include "Event.h"
#include "MainManager.h"

// ==== sqlite_orm 与 DeployStatus 枚举映射适配 START ====
namespace sqlite_orm {

template<>
struct type_printer<dcdn::DeployStatus>: public integer_printer
{
};

template<>
struct statement_binder<dcdn::DeployStatus>
{
    int bind(sqlite3_stmt* stmt, int index, const dcdn::DeployStatus& value)
    {
        return statement_binder<int>().bind(stmt, index, static_cast<int>(value));
    }
};

template<>
struct field_printer<dcdn::DeployStatus>
{
    std::string operator()(const dcdn::DeployStatus& t) const
    {
        return std::to_string(static_cast<int>(t));
    }
};

template<>
struct row_extractor<dcdn::DeployStatus>
{
    static dcdn::DeployStatus extract(const char* row_value)
    {
        return static_cast<dcdn::DeployStatus>(std::atoi(row_value));
    }
    static dcdn::DeployStatus extract(sqlite3_stmt* stmt, int columnIndex)
    {
        return static_cast<dcdn::DeployStatus>(sqlite3_column_int(stmt, columnIndex));
    }
};

} // namespace sqlite_orm
// ==== sqlite_orm 与 DeployStatus 枚举映射适配 END ====

NS_BEGIN(dcdn)

DeployManager::DeployManager(MainManager* man): BaseManager(man) {}

DeployManager::~DeployManager()
{
    std::lock_guard<std::mutex> lock(mTaskMutex);
    mJobToTaskMap.clear();
}

int DeployManager::Init(const DeployManagerOption& opt)
{
    if (mInited) {
        logWarn << "DeployManager already initialized";
        return 0;
    }

    // 从option注入依赖
    mFileMgr = opt.fileMgr;
    mDownloadMgr = opt.downloadMgr;

    // 校验依赖是否有效
    if (!mFileMgr) {
        logWarn << "FileManager instance is null in DeployManager";
        return -1;
    }
    if (!mDownloadMgr) {
        logWarn << "DownloadManager instance is null in DeployManager";
        return -1;
    }

    // 初始化数据库（改为主动加载，而非懒加载）
    if (!initDB()) { // 新增：主动初始化数据库
        logError << "Failed to initialize database";
        return -1;
    }

    // 初始化数据库表
    if (createTable() != 0) {
        logError << "DeployManager database initialization failed";
        return -1;
    } else {
        logInfo << "DeployManager database initialized successfully";
    }

    // 注册事件处理器
    registerHandler(EventType::DeployMsg, &DeployManager::handleDeployMsgEvent);

    // 重新提交未完成的任务
    resubmitDownloadTasks();

    mInited = true;
    return 0;
}

// 新增：主动初始化数据库实例
bool DeployManager::initDB()
{
    try {
        // 直接创建数据库实例，而非懒加载
        auto storage = std::make_shared<DeployStorage>(makeDeployStorage(dbPath().string()));
        mDB = storage;
        logInfo << "Database instance created successfully at: " << dbPath().string();
        return true;
    } catch (const std::exception& e) {
        logError << "Initialize database failed: " << e.what();
        return false;
    }
}

int DeployManager::createTable()
{
    std::lock_guard<std::mutex> lock(mDbMutex); // 仅保护表结构同步

    auto db = getDB();
    if (!db) {
        logError << "Failed to get database connection";
        return -1;
    }

    try {
        db->sync_schema();
        return 0;
    } catch (const std::exception& e) {
        logError << "Create deploy_tasks table failed: " << e.what();
        return -1;
    }
}

// 简化getDB：仅返回已初始化的实例，无需加锁（因为mDB仅在Init阶段赋值）
DeployStoragePtr DeployManager::getDB()
{
    return mDB;
}

void DeployManager::run()
{
    if (!mInited) {
        logError << "DeployManager not initialized, cannot run";
        return;
    }

    logInfo << "DeployManager started";
    while (true) {
        checkDownloadStatus();
        waitAllEvents(std::chrono::seconds(1));
    }
    logInfo << "DeployManager stopped";
}

void DeployManager::handleDeployMsgEvent(std::shared_ptr<Event> evt)
{
    logInfo << "Received DeployMsg event";

    auto* e = static_cast<ArgEvent<DeployMsgArg>*>(evt.get());
    if (!e) {
        logWarn << "Invalid DeployMsg event: wrong argument type";
        return;
    }
    const auto& arg = e->Arg();

    if (arg.jobId.empty()) {
        logWarn << "DeployMsg missing required field: jobId";
        return;
    }

    if (arg.fileHash.empty() && arg.url.empty()) {
        logWarn << "DeployMsg invalid: missing url or hash (jobId: " << arg.jobId << ")";
        reportToServer(arg.jobId, false);
        return;
    }

    if (!mFileMgr) {
        logError << "FileManager is not available (jobId: " << arg.jobId << ")";
        reportToServer(arg.jobId, false);
        return;
    }

    std::string downloadPath = mFileMgr->NewDownloadPath(0);
    if (downloadPath.empty()) {
        logWarn << "Failed to get download path (jobId: " << arg.jobId << ")";
        reportToServer(arg.jobId, false);
        return;
    }

    DeployTask task;
    task.jobId = arg.jobId;
    task.fileHash = arg.fileHash;
    task.blockHash = arg.blockInfo.hash;
    task.blockStart = arg.blockInfo.start;
    task.blockEnd = arg.blockInfo.end;
    task.url = arg.url;
    task.status = DeployStatus::DOWNLOADING;
    task.downloadPath = downloadPath;
    task.createTime = getCurrentTimestamp();
    task.updateTime = task.createTime;

    if (!saveDeployTask(task)) {
        logWarn << "Failed to save deploy task (jobId: " << arg.jobId << ")";
        reportToServer(arg.jobId, false);
        return;
    }

    if (!mDownloadMgr) {
        logError << "DownloadManager is not available (jobId: " << arg.jobId << ")";
        updateDeployTaskStatus(arg.jobId, DeployStatus::FAILED);
        reportToServer(arg.jobId, false);
        return;
    }

    FileDownloadOptions opts;
    opts.OutputPath = downloadPath;
    if (task.blockStart > 0 || task.blockEnd > 0) {
        opts.HasRange = true;
        opts.RangeStart = task.blockStart;
        opts.RangeEnd = task.blockEnd;
    }

    uint64_t taskId = mDownloadMgr->AddDownloadTask(task.url, task.fileHash, opts);
    if (taskId == 0) {
        logWarn << "Failed to create download task (jobId: " << arg.jobId << ")";
        updateDeployTaskStatus(arg.jobId, DeployStatus::FAILED);
        reportToServer(arg.jobId, false);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mTaskMutex);
        mJobToTaskMap[arg.jobId] = taskId;
    }
    logInfo << "Deploy task initialized (jobId: " << arg.jobId << ", taskId: " << taskId << ")";
}

void DeployManager::checkDownloadStatus()
{
    logInfo << "Starting download status check";

    auto tasks = loadDownloadingTasks();
    logInfo << "Loaded " << tasks.size() << " downloading tasks for status check";

    if (tasks.empty()) {
        logInfo << "No downloading tasks to check, exiting status check";
        return;
    }

    if (!mDownloadMgr || !mFileMgr) {
        logWarn << "FileManager or DownloadManager is unavailable, skip status check";
        return;
    }

    for (const auto& task : tasks) {
        logInfo << "Checking status for task (jobId: " << task.jobId << ", fileHash: " << task.fileHash
                << ", url: " << task.url << ", path: " << task.downloadPath << ")";

        uint64_t taskId = 0;
        {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            auto it = mJobToTaskMap.find(task.jobId);
            if (it == mJobToTaskMap.end()) {
                logWarn << "No download task ID found for job: " << task.jobId;
                continue;
            }
            taskId = it->second;
            logInfo << "Found corresponding download task ID: " << taskId << " for jobId: " << task.jobId;
        }

        auto downloadStatus = mDownloadMgr->GetTaskStatus(taskId);

        double percent = 0.0;
        if (downloadStatus.TotalSize > 0) {
            percent = (100.0 * downloadStatus.Downloaded) / downloadStatus.TotalSize;
        }
        logInfo << "Current status for task taskId: " << taskId << "% "
                << "percent: " << percent << "% "
                << "Downloaded: " << downloadStatus.Downloaded << "/" << downloadStatus.TotalSize << " bytes "
                << "Speed: " << downloadStatus.Speed / 1024.0 << " KB/s "
                << "Status: " << static_cast<int>(downloadStatus.Status);

        switch (downloadStatus.Status) {
            case TaskStatus::Completed: {
                logInfo << "Task (jobId: " << task.jobId << ") has completed downloading";

                std::string fileMd5;
                try {
                    std::ifstream file(task.downloadPath, std::ios::binary);
                    if (!file.is_open()) {
                        logError << "Failed to open file for MD5 calculation: " << task.downloadPath;
                        updateDeployTaskStatus(task.jobId, DeployStatus::FAILED);
                        reportToServer(task.jobId, false);
                        break;
                    }

                    MD5_CTX md5Context;
                    MD5_Init(&md5Context);

                    const size_t bufferSize = 8192;
                    char buffer[bufferSize];
                    while (file.read(buffer, bufferSize)) {
                        MD5_Update(&md5Context, buffer, file.gcount());
                    }
                    // 处理最后一部分数据
                    if (file.gcount() > 0) {
                        MD5_Update(&md5Context, buffer, file.gcount());
                    }

                    unsigned char md5Digest[MD5_DIGEST_LENGTH];
                    MD5_Final(md5Digest, &md5Context);

                    // 转换为十六进制字符串
                    std::stringstream ss;
                    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
                        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(md5Digest[i]);
                    }
                    fileMd5 = ss.str();
                    logInfo << "Calculated MD5 for " << task.downloadPath << ": " << fileMd5;
                } catch (const std::exception& e) {
                    logError << "Exception during MD5 calculation: " << e.what();
                    updateDeployTaskStatus(task.jobId, DeployStatus::FAILED);
                    reportToServer(task.jobId, false);
                    break;
                }
                FileDownloadDoneArg doneArg;
                doneArg.fileHash = task.fileHash;
                doneArg.blockInfo.hash = fileMd5;
                doneArg.blockInfo.start = task.blockStart;
                doneArg.blockInfo.end = task.blockEnd;
                doneArg.url = task.url;
                doneArg.filePath = task.downloadPath;

                mFileMgr->PostEvent(
                    std::make_shared<ArgEvent<FileDownloadDoneArg>>(EventType::FileDownloadDone, std::move(doneArg)));

                updateDeployTaskStatus(task.jobId, DeployStatus::COMPLETED);
                logInfo << "Updated task status to COMPLETED (jobId: " << task.jobId << ")";

                {
                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mJobToTaskMap.erase(task.jobId);
                }

                reportToServer(task.jobId, true);
                logInfo << "Deploy task completed (jobId: " << task.jobId << ")";
                break;
            }
            case TaskStatus::Failed:
            case TaskStatus::Cancelled: {
                logWarn << "Task (jobId: " << task.jobId << ") download failed or was cancelled with status: "
                        << static_cast<int>(downloadStatus.Status);

                FileDownloadFailedArg failArg;
                failArg.filePath = task.downloadPath;

                mFileMgr->PostEvent(
                    std::make_shared<ArgEvent<FileDownloadFailedArg>>(
                        EventType::FileDownloadFailed, std::move(failArg)));

                updateDeployTaskStatus(task.jobId, DeployStatus::FAILED);
                logInfo << "Updated task status to FAILED (jobId: " << task.jobId << ")";

                {
                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mJobToTaskMap.erase(task.jobId);
                }

                reportToServer(task.jobId, false);
                logWarn << "Deploy task failed (jobId: " << task.jobId << ")";
                break;
            }
            default: {
                logInfo << "Task (jobId: " << task.jobId
                        << ") is still in progress with status: " << static_cast<int>(downloadStatus.Status);

                DeployTask updateTask = task;
                updateTask.updateTime = getCurrentTimestamp();
                saveDeployTask(updateTask);
                break;
            }
        }
    }

    logInfo << "Completed download status check for all tasks";
}

bool DeployManager::saveDeployTask(const DeployTask& task)
{
    std::lock_guard<std::mutex> lock(mDbMutex); // 保护写操作

    auto db = getDB();
    if (!db) {
        logError << "Database instance is null when saving task (jobId: " << task.jobId << ")";
        return false;
    }

    try {
        db->replace(task);
        return true;
    } catch (const std::exception& e) {
        logError << "Save deploy task failed (jobId: " << task.jobId << "): " << e.what();
        return false;
    }
}

bool DeployManager::updateDeployTaskStatus(const std::string& jobId, DeployStatus status)
{
    std::lock_guard<std::mutex> lock(mDbMutex); // 保护写操作

    auto db = getDB();
    if (!db) {
        logError << "Database instance is null when updating status (jobId: " << jobId << ")";
        return false;
    }

    try {
        using namespace sqlite_orm;
        logInfo << "Updating task status - jobId: " << jobId << ", new status: " << static_cast<int>(status)
                << ", timestamp: " << getCurrentTimestamp();

        db->update_all(
            set(c(&DeployTask::status) = status, c(&DeployTask::updateTime) = getCurrentTimestamp()),
            where(c(&DeployTask::jobId) == jobId));

        logInfo << "Successfully updated status for jobId: " << jobId;
        return true;
    } catch (const std::exception& e) {
        logError << "Update task status failed (jobId: " << jobId << "): " << e.what();
        return false;
    }
}

std::vector<DeployTask> DeployManager::loadDownloadingTasks()
{
    std::lock_guard<std::mutex> lock(mDbMutex); // 保护读操作（避免与写操作冲突）

    auto db = getDB();
    if (!db) {
        logError << "Database instance is null when loading tasks";
        return {};
    }

    try {
        using namespace sqlite_orm;
        auto tasks = db->get_all<DeployTask>(where(c(&DeployTask::status) == DeployStatus::DOWNLOADING));
        logInfo << "Loaded " << tasks.size() << " downloading tasks from database";
        return tasks;
    } catch (const std::exception& e) {
        logError << "Load downloading tasks failed: " << e.what();
        return {};
    }
}

void DeployManager::resubmitDownloadTasks()
{
    logInfo << "Resubmitting incomplete deploy tasks";
    if (!mFileMgr || !mDownloadMgr) {
        logWarn << "Core managers unavailable, skip task resubmission";
        return;
    }

    auto tasks = loadDownloadingTasks();
    if (tasks.empty()) {
        logInfo << "No incomplete tasks to resubmit";
        return;
    }

    for (const auto& task : tasks) {
        if (task.downloadPath.empty()) {
            logWarn << "Invalid download path for task " << task.jobId << ", skipping";
            continue;
        }

        FileDownloadOptions opts;
        opts.OutputPath = task.downloadPath;
        if (task.blockStart > 0 || task.blockEnd > 0) {
            opts.HasRange = true;
            opts.RangeStart = task.blockStart;
            opts.RangeEnd = task.blockEnd;
        }

        uint64_t taskId = mDownloadMgr->AddDownloadTask(task.url, task.fileHash, opts);
        if (taskId == 0) {
            logWarn << "Failed to resubmit task " << task.jobId;
            updateDeployTaskStatus(task.jobId, DeployStatus::FAILED);
        } else {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            mJobToTaskMap[task.jobId] = taskId;
            logInfo << "Resubmitted task " << task.jobId << " with new task_id: " << taskId;
        }
    }
}

void DeployManager::reportToServer(const std::string& jobId, bool success)
{
    try {
        json event;
        event["type"] = "deploy_result";
        event["kvs"] = {{"job_id", jobId}, {"code", success ? "0" : "1"}};

        json request;
        request["events"] = {event};

        json response;
        mMan->AsyncApiPost(nullptr, "/api/v1/report_event", request, this, nullptr, nullptr);
    } catch (const std::exception& e) {
        logError << "Exception during report for job " << jobId << ": " << e.what();
    }
}

NS_END