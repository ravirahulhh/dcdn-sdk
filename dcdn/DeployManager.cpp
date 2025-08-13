#include "DeployManager.h"

#include <plog/Log.h>

#include <chrono>
#include <filesystem>
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
        LOGW << "DeployManager already initialized";
        return 0;
    }

    // 从option注入依赖
    mFileMgr = opt.fileMgr;
    mDownloadMgr = opt.downloadMgr;

    // 校验依赖是否有效
    if (!mFileMgr) {
        LOGW << "FileManager instance is null in DeployManager";
        return -1;
    }
    if (!mDownloadMgr) {
        LOGW << "DownloadManager instance is null in DeployManager";
        return -1;
    }

    // 初始化数据库（改为主动加载，而非懒加载）
    if (!initDB()) { // 新增：主动初始化数据库
        LOGE << "Failed to initialize database";
        return -1;
    }

    // 初始化数据库表
    if (createTable() != 0) {
        LOGE << "DeployManager database initialization failed";
        return -1;
    } else {
        LOGI << "DeployManager database initialized successfully";
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
        LOGI << "Database instance created successfully at: " << dbPath().string();
        return true;
    } catch (const std::exception& e) {
        LOGE << "Initialize database failed: " << e.what();
        return false;
    }
}

int DeployManager::createTable()
{
    std::lock_guard<std::mutex> lock(mDbMutex); // 仅保护表结构同步

    auto db = getDB();
    if (!db) {
        LOGE << "Failed to get database connection";
        return -1;
    }

    try {
        db->sync_schema();
        return 0;
    } catch (const std::exception& e) {
        LOGE << "Create deploy_tasks table failed: " << e.what();
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
        LOGE << "DeployManager not initialized, cannot run";
        return;
    }

    LOGI << "DeployManager started";
    while (true) {
        checkDownloadStatus();
        waitAllEvents(std::chrono::seconds(1));
    }
    LOGI << "DeployManager stopped";
}

void DeployManager::handleDeployMsgEvent(std::shared_ptr<Event> evt)
{
    LOGI << "Received DeployMsg event";

    auto* e = static_cast<ArgEvent<DeployMsgArg>*>(evt.get());
    if (!e) {
        LOGW << "Invalid DeployMsg event: wrong argument type";
        return;
    }
    const auto& arg = e->Arg();

    if (arg.jobId.empty()) {
        LOGW << "DeployMsg missing required field: jobId";
        return;
    }

    if (arg.fileHash.empty() && arg.url.empty()) {
        LOGW << "DeployMsg invalid: missing url or hash (jobId: " << arg.jobId << ")";
        reportToServer(arg.jobId, false);
        return;
    }

    if (!mFileMgr) {
        LOGE << "FileManager is not available (jobId: " << arg.jobId << ")";
        reportToServer(arg.jobId, false);
        return;
    }

    std::string downloadPath = mFileMgr->NewDownloadPath(0);
    if (downloadPath.empty()) {
        LOGW << "Failed to get download path (jobId: " << arg.jobId << ")";
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
        LOGW << "Failed to save deploy task (jobId: " << arg.jobId << ")";
        reportToServer(arg.jobId, false);
        return;
    }

    if (!mDownloadMgr) {
        LOGE << "DownloadManager is not available (jobId: " << arg.jobId << ")";
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
        LOGW << "Failed to create download task (jobId: " << arg.jobId << ")";
        updateDeployTaskStatus(arg.jobId, DeployStatus::FAILED);
        reportToServer(arg.jobId, false);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mTaskMutex);
        mJobToTaskMap[arg.jobId] = taskId;
    }
    LOGI << "Deploy task initialized (jobId: " << arg.jobId << ", taskId: " << taskId << ")";
}

void DeployManager::checkDownloadStatus()
{
    LOGI << "Starting download status check";

    auto tasks = loadDownloadingTasks();
    LOGI << "Loaded " << tasks.size() << " downloading tasks for status check";

    if (tasks.empty()) {
        LOGI << "No downloading tasks to check, exiting status check";
        return;
    }

    if (!mDownloadMgr || !mFileMgr) {
        LOGW << "FileManager or DownloadManager is unavailable, skip status check";
        return;
    }

    for (const auto& task : tasks) {
        LOGI << "Checking status for task (jobId: " << task.jobId << ", fileHash: " << task.fileHash
             << ", url: " << task.url << ", path: " << task.downloadPath << ")";

        uint64_t taskId = 0;
        {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            auto it = mJobToTaskMap.find(task.jobId);
            if (it == mJobToTaskMap.end()) {
                LOGW << "No download task ID found for job: " << task.jobId;
                continue;
            }
            taskId = it->second;
            LOGI << "Found corresponding download task ID: " << taskId << " for jobId: " << task.jobId;
        }

        auto downloadStatus = mDownloadMgr->GetTaskStatus(taskId);

        double percent = 0.0;
        if (downloadStatus.TotalSize > 0) {
            percent = (100.0 * downloadStatus.Downloaded) / downloadStatus.TotalSize;
        }
        LOGI << "Current status for task taskId: " << taskId << "% "
             << "percent: " << percent << "% "
             << "Downloaded: " << downloadStatus.Downloaded << "/" << downloadStatus.TotalSize << " bytes "
             << "Speed: " << downloadStatus.Speed / 1024.0 << " KB/s "
             << "Status: " << static_cast<int>(downloadStatus.Status);

        switch (downloadStatus.Status) {
            case TaskStatus::Completed: {
                LOGI << "Task (jobId: " << task.jobId << ") has completed downloading";

                FileDownloadDoneArg doneArg;
                doneArg.fileHash = task.fileHash;
                doneArg.blockInfo.hash = task.blockHash;
                doneArg.blockInfo.start = task.blockStart;
                doneArg.blockInfo.end = task.blockEnd;
                doneArg.url = task.url;
                doneArg.filePath = task.downloadPath;
                LOGI << "1";

                mFileMgr->PostEvent(
                    std::make_shared<ArgEvent<FileDownloadDoneArg>>(EventType::FileDownloadDone, std::move(doneArg)));
                LOGI << "2";
                updateDeployTaskStatus(task.jobId, DeployStatus::COMPLETED);
                LOGI << "Updated task status to COMPLETED (jobId: " << task.jobId << ")";

                {
                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mJobToTaskMap.erase(task.jobId);
                }

                reportToServer(task.jobId, true);
                LOGI << "Deploy task completed (jobId: " << task.jobId << ")";
                break;
            }
            case TaskStatus::Failed:
            case TaskStatus::Cancelled: {
                LOGW << "Task (jobId: " << task.jobId
                     << ") download failed or was cancelled with status: " << static_cast<int>(downloadStatus.Status);

                FileDownloadFailedArg failArg;
                failArg.filePath = task.downloadPath;

                mFileMgr->PostEvent(
                    std::make_shared<ArgEvent<FileDownloadFailedArg>>(
                        EventType::FileDownloadFailed, std::move(failArg)));

                updateDeployTaskStatus(task.jobId, DeployStatus::FAILED);
                LOGI << "Updated task status to FAILED (jobId: " << task.jobId << ")";

                {
                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mJobToTaskMap.erase(task.jobId);
                }

                reportToServer(task.jobId, false);
                LOGW << "Deploy task failed (jobId: " << task.jobId << ")";
                break;
            }
            default: {
                LOGI << "Task (jobId: " << task.jobId
                     << ") is still in progress with status: " << static_cast<int>(downloadStatus.Status);

                DeployTask updateTask = task;
                updateTask.updateTime = getCurrentTimestamp();
                saveDeployTask(updateTask);
                break;
            }
        }
    }

    LOGI << "Completed download status check for all tasks";
}

bool DeployManager::saveDeployTask(const DeployTask& task)
{
    std::lock_guard<std::mutex> lock(mDbMutex); // 保护写操作

    auto db = getDB();
    if (!db) {
        LOGE << "Database instance is null when saving task (jobId: " << task.jobId << ")";
        return false;
    }

    try {
        db->replace(task);
        return true;
    } catch (const std::exception& e) {
        LOGE << "Save deploy task failed (jobId: " << task.jobId << "): " << e.what();
        return false;
    }
}

bool DeployManager::updateDeployTaskStatus(const std::string& jobId, DeployStatus status)
{
    std::lock_guard<std::mutex> lock(mDbMutex); // 保护写操作

    auto db = getDB();
    if (!db) {
        LOGE << "Database instance is null when updating status (jobId: " << jobId << ")";
        return false;
    }

    try {
        using namespace sqlite_orm;
        LOGI << "Updating task status - jobId: " << jobId << ", new status: " << static_cast<int>(status)
             << ", timestamp: " << getCurrentTimestamp();

        db->update_all(
            set(c(&DeployTask::status) = status, c(&DeployTask::updateTime) = getCurrentTimestamp()),
            where(c(&DeployTask::jobId) == jobId));

        LOGI << "Successfully updated status for jobId: " << jobId;
        return true;
    } catch (const std::exception& e) {
        LOGE << "Update task status failed (jobId: " << jobId << "): " << e.what();
        return false;
    }
}

std::vector<DeployTask> DeployManager::loadDownloadingTasks()
{
    std::lock_guard<std::mutex> lock(mDbMutex); // 保护读操作（避免与写操作冲突）

    auto db = getDB();
    if (!db) {
        LOGE << "Database instance is null when loading tasks";
        return {};
    }

    try {
        using namespace sqlite_orm;
        auto tasks = db->get_all<DeployTask>(where(c(&DeployTask::status) == DeployStatus::DOWNLOADING));
        LOGI << "Loaded " << tasks.size() << " downloading tasks from database";
        return tasks;
    } catch (const std::exception& e) {
        LOGE << "Load downloading tasks failed: " << e.what();
        return {};
    }
}

void DeployManager::resubmitDownloadTasks()
{
    LOGI << "Resubmitting incomplete deploy tasks";
    if (!mFileMgr || !mDownloadMgr) {
        LOGW << "Core managers unavailable, skip task resubmission";
        return;
    }

    auto tasks = loadDownloadingTasks();
    if (tasks.empty()) {
        LOGI << "No incomplete tasks to resubmit";
        return;
    }

    for (const auto& task : tasks) {
        if (task.downloadPath.empty()) {
            LOGW << "Invalid download path for task " << task.jobId << ", skipping";
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
            LOGW << "Failed to resubmit task " << task.jobId;
            updateDeployTaskStatus(task.jobId, DeployStatus::FAILED);
        } else {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            mJobToTaskMap[task.jobId] = taskId;
            LOGI << "Resubmitted task " << task.jobId << " with new task_id: " << taskId;
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
        LOGE << "Exception during report for job " << jobId << ": " << e.what();
    }
}

NS_END