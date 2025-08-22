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

// sqlite_orm 与 DeployStatus 枚举映射适配 START
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
// sqlite_orm 与 DeployStatus 枚举映射适配 END

NS_BEGIN(dcdn)

DeployManager::DeployManager(MainManager* man): BaseManager(man), mShouldExit(false) {}

DeployManager::~DeployManager()
{
    stop(); // 确保线程正确退出

    std::lock_guard<std::mutex> lock(mTaskMutex);
    mJobToTaskMap.clear();
    mTaskToJobMap.clear();

    std::lock_guard<std::mutex> eventLock(mEventMutex);
    while (!mEventQueue.empty()) {
        mEventQueue.pop();
    }
}

void DeployManager::stop()
{
    mShouldExit = true;
    mEventCond.notify_one(); // 唤醒等待的线程

    if (mWorkerThread.joinable()) {
        mWorkerThread.join();
    }
}

int DeployManager::Init(const DeployManagerOption& opt)
{
    if (mInited) {
        logWarn << "DeployManager already initialized";
        return 0;
    }

    mFileMgr = opt.fileMgr;
    mDownloadMgr = opt.downloadMgr;

    if (!mFileMgr) {
        logWarn << "FileManager instance is null in DeployManager";
        return -1;
    }
    if (!mDownloadMgr) {
        logWarn << "DownloadManager instance is null in DeployManager";
        return -1;
    }

    if (!initDB()) {
        logError << "Failed to initialize database";
        return -1;
    }

    if (createTable() != 0) {
        logError << "DeployManager database initialization failed";
        return -1;
    } else {
        logInfo << "DeployManager database initialized successfully";
    }

    registerHandler(EventType::DeployMsg, &DeployManager::handleDeployMsgEvent);

    mInited = true;
    return 0;
}

bool DeployManager::initDB()
{
    try {
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
    std::lock_guard<std::mutex> lock(mDbMutex);

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

    // 启动工作线程
    mWorkerThread = std::thread(&DeployManager::workerMain, this);
    logInfo << "DeployManager worker thread started";
    while (true) {
        // Main loop only handles events, flush tasks are handled by separate threads
        waitAllEvents(std::chrono::milliseconds(1000));
    }
}

void DeployManager::workerMain()
{
    // 重新提交未完成的任务
    resubmitDownloadTasks();
    logInfo << "DeployManager started";

    // 事件处理循环
    while (!mShouldExit) {
        std::unique_lock<std::mutex> lock(mEventMutex);

        // 等待事件或退出信号
        mEventCond.wait(lock, [this]() { return !mEventQueue.empty() || mShouldExit; });

        if (mShouldExit) {
            break;
        }

        // 处理所有待处理事件
        while (!mEventQueue.empty() && !mShouldExit) {
            auto event = mEventQueue.front();
            mEventQueue.pop();

            // 解锁以允许新事件入队
            lock.unlock();

            // 处理任务状态变化
            handleTaskStatusChange(event.taskId, event.fromStatus, event.toStatus);

            // 重新加锁以检查下一个事件
            lock.lock();
        }
    }

    logInfo << "DeployManager worker thread exited";
}

void DeployManager::handleDeployMsgEvent(std::shared_ptr<Event> evt)
{
    logInfo << "Received DeployMsg event";

    auto* jsonEvent = static_cast<ArgEvent<json>*>(evt.get());
    if (!jsonEvent) {
        logWarn << "Invalid DeployMsg event: wrong argument type (expected ArgEvent<json>)";
        return;
    }

    try {
        const json& payload = jsonEvent->Arg();
        logInfo << "Deploy message payload: " << payload.dump(2);
        if (!payload.contains("deploy_file") || !payload["deploy_file"].is_object()) {
            logWarn << "DeployMsg missing required field: deploy_file (must be an object)";
            return;
        }

        const json& deployFile = payload["deploy_file"];
        std::string jobId;
        if (!deployFile.contains("job_id") || !deployFile["job_id"].is_string()) {
            logWarn << "DeployMsg missing required field: job_id";
            return;
        }
        jobId = deployFile["job_id"].get<std::string>();
        if (jobId.empty()) {
            logWarn << "DeployMsg has empty job_id";
            return;
        }

        auto existingTask = getTaskByJobId(jobId);
        if (existingTask.has_value()) {
            logInfo << "Deploy task with job_id " << jobId << " already exists, ignoring";
            return;
        }

        std::string fileHash;
        if (deployFile.contains("file_hash") && deployFile["file_hash"].is_string()) {
            fileHash = deployFile["file_hash"].get<std::string>();
        }

        std::string url;
        if (deployFile.contains("url") && deployFile["url"].is_string()) {
            url = deployFile["url"].get<std::string>();
        }

        if (fileHash.empty() && url.empty()) {
            logWarn << "DeployMsg invalid: missing url or file_hash (job_id: " << jobId << ")";
            reportToServer(jobId, false);
            return;
        }

        std::string blockHash;
        uint64_t blockStart = 0;
        uint64_t blockEnd = 0;

        if (deployFile.contains("block_info") && deployFile["block_info"].is_object()) {
            const json& blockInfo = deployFile["block_info"];
            if (blockInfo.contains("hash") && blockInfo["hash"].is_string()) {
                blockHash = blockInfo["hash"].get<std::string>();
            }
            if (blockInfo.contains("start") && blockInfo["start"].is_number()) {
                blockStart = blockInfo["start"].get<uint64_t>();
            }
            if (blockInfo.contains("end") && blockInfo["end"].is_number()) {
                blockEnd = blockInfo["end"].get<uint64_t>();
            }
        }

        if (!mFileMgr) {
            logError << "FileManager is not available (job_id: " << jobId << ")";
            reportToServer(jobId, false);
            return;
        }

        std::string downloadPath = mFileMgr->NewDownloadPath(blockEnd > blockStart ? blockEnd - blockStart : 0);
        if (downloadPath.empty()) {
            logWarn << "Failed to get download path (job_id: " << jobId << ")";
            reportToServer(jobId, false);
            return;
        }

        DeployTask task;
        task.jobId = jobId;
        task.fileHash = fileHash;
        task.blockHash = blockHash;
        task.blockStart = blockStart;
        task.blockEnd = blockEnd;
        task.url = url;
        task.status = DeployStatus::DOWNLOADING;
        task.downloadPath = downloadPath;
        task.createTime = getCurrentTimestamp();
        task.updateTime = task.createTime;

        if (!saveDeployTask(task)) {
            logWarn << "Failed to save deploy task (job_id: " << jobId << ")";
            reportToServer(jobId, false);
            return;
        }

        if (!mDownloadMgr) {
            logError << "DownloadManager is not available (job_id: " << jobId << ")";
            updateDeployTaskStatus(jobId, DeployStatus::FAILED);
            reportToServer(jobId, false);
            return;
        }

        FileDownloadOptions opts;
        opts.OutputPath = downloadPath;
        // 设置回调函数
        opts.taskStateChangeEventCallback =
            std::bind(&DeployManager::taskStateChangeEventCallback, this, std::placeholders::_1);

        if (task.blockStart > 0 || task.blockEnd > 0) {
            opts.HasRange = true;
            opts.RangeStart = task.blockStart;
            opts.RangeEnd = task.blockEnd;
        }

        if (!task.fileHash.empty()) {
            opts.Strategy = DownloadStrategy::P2P_ONLY;
        }

        uint64_t taskId = mDownloadMgr->AddDownloadTask(task.url, task.fileHash, opts);
        if (taskId == 0) {
            logWarn << "Failed to create download task (job_id: " << jobId << ")";
            updateDeployTaskStatus(jobId, DeployStatus::FAILED);
            reportToServer(jobId, false);
            return;
        }

        {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            mJobToTaskMap[jobId] = taskId;
            mTaskToJobMap[taskId] = jobId; // 记录反向映射
        }
        logInfo << "Deploy task initialized (job_id: " << jobId << ", taskId: " << taskId << ")";

    } catch (const std::exception& e) {
        logError << "Error processing deploy message: " << e.what();
    } catch (...) {
        logError << "Unknown error processing deploy message";
    }
}

// 回调函数实现 - 仅负责将事件入队
void DeployManager::taskStateChangeEventCallback(const dcdn::DMEvent& ev)
{
    if (auto* p = dynamic_cast<const dcdn::ETaskStatusChanged*>(&ev)) {
        logInfo << "Task id :" << p->id << " status changed from:" << static_cast<int>(p->from)
                << " to:" << static_cast<int>(p->to);

        // 将事件放入队列并唤醒工作线程
        std::lock_guard<std::mutex> lock(mEventMutex);
        mEventQueue.push({p->id, p->from, p->to});
        mEventCond.notify_one();
    }
}

// 状态处理函数 - 在工作线程中执行
void DeployManager::handleTaskStatusChange(uint64_t taskId, TaskStatus fromStatus, TaskStatus toStatus)
{
    logInfo << "Handling task status change - taskId: " << taskId << ", from: " << static_cast<int>(fromStatus)
            << ", to: " << static_cast<int>(toStatus);

    // 获取对应的任务
    auto taskOpt = getTaskByTaskId(taskId);
    if (!taskOpt.has_value()) {
        logWarn << "No deploy task found for taskId: " << taskId;
        return;
    }
    const auto& task = taskOpt.value();

    switch (toStatus) {
        case TaskStatus::Completed: {
            logInfo << "Task (jobId: " << task.jobId << ") has completed downloading";

            std::string localFileHash;
            int hashError = calculateFileHash(task.downloadPath, localFileHash);
            if (hashError != ErrorCodeOk) {
                FileDownloadFailedArg failArg;
                failArg.FilePath = task.downloadPath;

                mFileMgr->PostEvent(
                    std::make_shared<ArgEvent<FileDownloadFailedArg>>(
                        EventType::FileDownloadFailed, std::move(failArg)));

                updateDeployTaskStatus(task.jobId, DeployStatus::FAILED);
                logInfo << "Updated task status to FAILED (jobId: " << task.jobId << ")";
                break;
            }

            uint64_t blockEnd = task.blockEnd;
            if (task.blockEnd == 0) {
                uint64_t filesize;
                int getFileSizeError = getFileSize(task.downloadPath, filesize);
                if (getFileSizeError != ErrorCodeOk) {
                    FileDownloadFailedArg failArg;
                    failArg.FilePath = task.downloadPath;

                    mFileMgr->PostEvent(
                        std::make_shared<ArgEvent<FileDownloadFailedArg>>(
                            EventType::FileDownloadFailed, std::move(failArg)));

                    updateDeployTaskStatus(task.jobId, DeployStatus::FAILED);
                    logInfo << "Updated task status to FAILED (jobId: " << task.jobId << ")";
                    break;
                }
                blockEnd = task.blockStart + filesize;
            }
            std::string fileHash = task.fileHash;
            if (task.blockStart == 0 && task.blockEnd == 0) {
                if (task.fileHash.empty()) {
                    fileHash = localFileHash;
                } else if (!task.fileHash.empty() && task.fileHash != localFileHash) {
                    logError << "File hash mismatch (jobId: " << task.jobId << ")";
                    FileDownloadFailedArg failArg;
                    failArg.FilePath = task.downloadPath;

                    mFileMgr->PostEvent(
                        std::make_shared<ArgEvent<FileDownloadFailedArg>>(
                            EventType::FileDownloadFailed, std::move(failArg)));
                    updateDeployTaskStatus(task.jobId, DeployStatus::FAILED);
                    reportToServer(task.jobId, false);
                    break;
                }
            }

            FileDownloadDoneArg doneArg;
            doneArg.FileHash = fileHash;
            doneArg.BlockInfo.Hash = localFileHash;
            doneArg.BlockInfo.Start = task.blockStart;
            doneArg.BlockInfo.End = blockEnd;
            doneArg.Url = task.url;
            doneArg.FilePath = task.downloadPath;

            mFileMgr->PostEvent(
                std::make_shared<ArgEvent<FileDownloadDoneArg>>(EventType::FileDownloadDone, std::move(doneArg)));

            updateDeployTaskStatus(task.jobId, DeployStatus::COMPLETED);
            logInfo << "Updated task status to COMPLETED (jobId: " << task.jobId << ")";

            {
                std::lock_guard<std::mutex> lock(mTaskMutex);
                mJobToTaskMap.erase(task.jobId);
                mTaskToJobMap.erase(taskId);
            }

            reportToServer(task.jobId, true);
            logInfo << "Deploy task completed (jobId: " << task.jobId << ")";
            break;
        }
        case TaskStatus::Failed:
        case TaskStatus::Cancelled: {
            logWarn << "Task (jobId: " << task.jobId
                    << ") download failed or was cancelled with status: " << static_cast<int>(toStatus);

            FileDownloadFailedArg failArg;
            failArg.FilePath = task.downloadPath;

            mFileMgr->PostEvent(
                std::make_shared<ArgEvent<FileDownloadFailedArg>>(EventType::FileDownloadFailed, std::move(failArg)));

            updateDeployTaskStatus(task.jobId, DeployStatus::FAILED);
            logInfo << "Updated task status to FAILED (jobId: " << task.jobId << ")";

            {
                std::lock_guard<std::mutex> lock(mTaskMutex);
                mJobToTaskMap.erase(task.jobId);
                mTaskToJobMap.erase(taskId);
            }

            reportToServer(task.jobId, false);
            logWarn << "Deploy task failed (jobId: " << task.jobId << ")";
            break;
        }
        default: {
            logInfo << "Task (jobId: " << task.jobId
                    << ") is still in progress with status: " << static_cast<int>(toStatus);

            DeployTask updateTask = task;
            updateTask.updateTime = getCurrentTimestamp();
            saveDeployTask(updateTask);
            break;
        }
    }
}

bool DeployManager::saveDeployTask(const DeployTask& task)
{
    std::lock_guard<std::mutex> lock(mDbMutex);

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
    std::lock_guard<std::mutex> lock(mDbMutex);

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
    std::lock_guard<std::mutex> lock(mDbMutex);

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
        opts.taskStateChangeEventCallback =
            std::bind(&DeployManager::taskStateChangeEventCallback, this, std::placeholders::_1);

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
            mTaskToJobMap[taskId] = task.jobId;
            logInfo << "Resubmitted task " << task.jobId << " with new task_id: " << taskId;
        }
    }
}

void DeployManager::reportToServer(const std::string& jobId, bool success)
{
    try {
        json events = json::array();
        events.push_back({{"type", "deploy_result"}, {"kvs", {{"job_id", jobId}, {"code", success ? "1" : "-1"}}}});

        json request;
        request["events"] = events;

        logInfo << "Reporting deploy result to server: " << request.dump();
        json response;
        mMan->AsyncApiPostWithToken(nullptr, "/api/v1/report_event", request, this, nullptr, nullptr);
    } catch (const std::exception& e) {
        logError << "Exception during report for job " << jobId << ": " << e.what();
    }
}

std::optional<DeployTask> DeployManager::getTaskByTaskId(uint64_t taskId)
{
    std::string jobId;
    {
        std::lock_guard<std::mutex> lock(mTaskMutex);
        auto it = mTaskToJobMap.find(taskId);
        if (it == mTaskToJobMap.end()) {
            return std::nullopt;
        }
        jobId = it->second;
    }

    return getTaskByJobId(jobId);
}

std::optional<DeployTask> DeployManager::getTaskByJobId(const std::string& jobId)
{
    std::lock_guard<std::mutex> lock(mDbMutex);
    auto db = getDB();
    if (!db) {
        logError << "Database instance is null when getting task by jobId: " << jobId;
        return std::nullopt;
    }

    try {
        using namespace sqlite_orm;
        auto tasks = db->get_all<DeployTask>(where(c(&DeployTask::jobId) == jobId));
        if (tasks.empty()) {
            return std::nullopt;
        }
        return tasks[0];
    } catch (const std::exception& e) {
        logError << "Get task by jobId failed (jobId: " << jobId << "): " << e.what();
        return std::nullopt;
    }
}

NS_END
