#include "DeployManager.h"
#include "MainManager.h"
#include "Event.h"
#include <plog/Log.h>
#include <chrono>
#include <thread>
#include <filesystem>

NS_BEGIN(dcdn)

DeployManager::DeployManager(MainManager* man) : BaseManager(man) {
    // 直接持有核心模块
    mFileMgr = man->mFileMgr;
    mDownloadMgr = man->mDownloadMgr;

    // 模块有效性校验
    if (!mFileMgr) logWarn << "FileManager instance is null in DeployManager";
    if (!mDownloadMgr) logWarn << "DownloadManager instance is null in DeployManager";

    // 初始化数据库
    if (createTable() != ErrorCodeOk) {
        logError << "DeployManager database initialization failed";
    } else {
        logInfo << "DeployManager database initialized successfully";
    }

    // 注册事件处理器
    registerHandler(EventType::DeployMsg, &DeployManager::handleDeployMsgEvent);

    // 重启时恢复未完成任务
    resubmitDownloadTasks();
}

DeployManager::~DeployManager() {
    std::lock_guard<std::mutex> lock(mTaskMutex);
    mJobToTaskMap.clear();
}

int DeployManager::createTable() {
    auto db = getDB();
    if (!db) {
        logError << "Failed to get database connection";
        return -1;
    }

    try {
        db->sync_schema();  // 创建表结构（如不存在）
        return ErrorCodeOk;
    } catch (const std::exception& e) {
        logError << "Create deploy_tasks table failed: " << e.what();
        return -1;
    }
}

std::shared_ptr<StorageRef> DeployManager::getDB() {
    if (!mDB) {
        try {
            auto storage = std::make_shared<DeployStorage>(makeDeployStorage(dbPath().string()));
            mDB = std::static_pointer_cast<StorageRef>(storage);
        } catch (const std::exception& e) {
            logError << "Initialize database failed: " << e.what();
            return nullptr;
        }
    }
    return mDB;
}

void DeployManager::run() {
    logInfo << "DeployManager started";
    while (true) {
        // 每1秒检查一次下载状态
        checkDownloadStatus();
        waitAllEvents(std::chrono::seconds(1));
    }
    logInfo << "DeployManager stopped";
}

void DeployManager::handleDeployMsgEvent(std::shared_ptr<Event> evt) {
    logInfo << "Received DeployMsg event";
    auto* e = static_cast<ArgEvent<DeployMsgArg>*>(evt.get());
    if (!e) {
        logWarn << "Invalid DeployMsg event: wrong argument type";
        return;
    }
    const auto& arg = e->Arg();

    // 校验必填字段
    if (arg.job_id.empty()) {
        logWarn << "DeployMsg missing required field: job_id";
        return;
    }

    // 校验下载源（URL必须存在，哈希至少存在一个）
    bool hasValidHash = !arg.file_hash.empty() || !arg.block_hash.empty();
    if (!hasValidHash || arg.url.empty()) {
        logWarn << "DeployMsg invalid: missing url or hash (job_id: " << arg.job_id << ")";
        reportToServer(arg.job_id, false);
        return;
    }

    // 检查FileManager可用性
    if (!mFileMgr) {
        logError << "FileManager is not available (job_id: " << arg.job_id << ")";
        reportToServer(arg.job_id, false);
        return;
    }

    // 获取下载路径
    std::string downloadPath = mFileMgr->NewDownloadPath(0);
    if (downloadPath.empty()) {
        logWarn << "Failed to get download path (job_id: " << arg.job_id << ")";
        reportToServer(arg.job_id, false);
        return;
    }

    // 构建部署任务
    DeployTask task;
    task.job_id = arg.job_id;
    task.file_hash = arg.file_hash;       // 来自BlockInfo的文件哈希
    task.block_hash = arg.block_hash;     // 来自BlockInfo的区块哈希
    task.block_start = arg.block_start;   // 来自BlockInfo的区块起始
    task.block_end = arg.block_end;       // 来自BlockInfo的区块结束
    task.url = arg.url;
    task.status = DeployStatus::DOWNLOADING;
    task.download_path = downloadPath;
    task.create_time = getCurrentTimestamp();
    task.update_time = task.create_time;

    // 保存任务到数据库
    if (!saveDeployTask(task)) {
        logWarn << "Failed to save deploy task (job_id: " << arg.job_id << ")";
        reportToServer(arg.job_id, false);
        return;
    }

    // 检查DownloadManager可用性
    if (!mDownloadMgr) {
        logError << "DownloadManager is not available (job_id: " << arg.job_id << ")";
        updateDeployTaskStatus(arg.job_id, DeployStatus::FAILED);
        reportToServer(arg.job_id, false);
        return;
    }

    // 提交下载任务
    FileDownloadOptions opts;
    opts.outputPath = downloadPath;
    if (task.block_start > 0 || task.block_end > 0) {
        opts.hasRange = true;
        opts.rangeStart = task.block_start;
        opts.rangeEnd = task.block_end;
    }

    std::string taskId = mDownloadMgr->addDownloadTask(task.url, task.file_hash, opts);

    if (taskId.empty()) {
        logWarn << "Failed to create download task (job_id: " << arg.job_id << ")";
        updateDeployTaskStatus(arg.job_id, DeployStatus::FAILED);
        reportToServer(arg.job_id, false);
        return;
    }

    // 记录任务ID映射
    std::lock_guard<std::mutex> lock(mTaskMutex);
    mJobToTaskMap[arg.job_id] = taskId;
    logInfo << "Deploy task initialized (job_id: " << arg.job_id << ", task_id: " << taskId << ")";
}

void DeployManager::checkDownloadStatus() {
    logVerb << "Checking download status for active tasks";
    auto tasks = loadDownloadingTasks();
    if (tasks.empty()) {
        logVerb << "No active download tasks";
        return;
    }

    if (!mDownloadMgr || !mFileMgr) {
        logWarn << "FileManager or DownloadManager is unavailable, skip status check";
        return;
    }

    for (const auto& task : tasks) {
        std::string taskId;
        {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            auto it = mJobToTaskMap.find(task.job_id);
            if (it == mJobToTaskMap.end()) {
                logWarn << "No download task ID found for job: " << task.job_id;
                continue;
            }
            taskId = it->second;
        }

        // 查询下载状态
        auto downloadStatus = mDownloadMgr->getTaskStatus(taskId);
        switch (downloadStatus.status) {
            case TaskStatus::Completed: {
                // 构建下载完成事件（匹配FileManager要求）
                FileDownloadDoneArg doneArg;
                doneArg.file_hash = task.file_hash;
                doneArg.block_hash = task.block_hash;
                doneArg.block_start = task.block_start;
                doneArg.block_end = task.block_end;
                doneArg.url = task.url;
                doneArg.file_path = task.download_path;

                // 通知FileManager处理完成文件
                postEvent(EventType::FileDownloadDone, doneArg);

                // 更新任务状态并清理映射
                updateDeployTaskStatus(task.job_id, DeployStatus::COMPLETED);
                {
                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mJobToTaskMap.erase(task.job_id);
                }

                // 上报成功结果
                reportToServer(task.job_id, true);
                logInfo << "Deploy task completed (job_id: " << task.job_id << ")";
                break;
            }
            case TaskStatus::Failed:
            case TaskStatus::Cancelled: {
                // 构建下载失败事件
                FileDownloadFailedArg failArg;
                failArg.file_path = task.download_path;

                // 通知FileManager处理失败文件
                postEvent(EventType::FileDownloadFailed, failArg);

                // 更新任务状态并清理映射
                updateDeployTaskStatus(task.job_id, DeployStatus::FAILED);
                {
                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mJobToTaskMap.erase(task.job_id);
                }

                // 上报失败结果
                reportToServer(task.job_id, false);
                logWarn << "Deploy task failed (job_id: " << task.job_id << ")";
                break;
            }
            default:
                // 下载中，更新时间戳
                DeployTask updateTask = task;
                updateTask.update_time = getCurrentTimestamp();
                saveDeployTask(updateTask);
                logVerb << "Deploy task in progress (job_id: " << task.job_id << ")";
                break;
        }
    }
}

bool DeployManager::saveDeployTask(const DeployTask& task) {
    auto db = getDB();
    if (!db) return false;

    try {
        db->replace(task);  // 存在则更新，不存在则插入
        return true;
    } catch (const std::exception& e) {
        logError << "Save deploy task failed (job_id: " << task.job_id << "): " << e.what();
        return false;
    }
}

bool DeployManager::updateDeployTaskStatus(const std::string& job_id, DeployStatus status) {
    auto db = getDB();
    if (!db) return false;

    try {
        db->update_all(
            set(
                c(&DeployTask::status) = status,
                c(&DeployTask::update_time) = getCurrentTimestamp()
            ),
            where(c(&DeployTask::job_id) == job_id)
        );
        return true;
    } catch (const std::exception& e) {
        logError << "Update task status failed (job_id: " << job_id << "): " << e.what();
        return false;
    }
}

std::vector<DeployTask> DeployManager::loadDownloadingTasks() {
    auto db = getDB();
    if (!db) return {};

    try {
        return db->get_all<DeployTask>(
            where(c(&DeployTask::status) == DeployStatus::DOWNLOADING)
        );
    } catch (const std::exception& e) {
        logError << "Load downloading tasks failed: " << e.what();
        return {};
    }
}

void DeployManager::resubmitDownloadTasks() {
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
        if (task.download_path.empty()) {
            logWarn << "Invalid download path for task " << task.job_id << ", skipping";
            continue;
        }

        // 重建下载参数
        FileDownloadOptions opts;
        opts.outputPath = task.download_path;
        if (task.block_start > 0 || task.block_end > 0) {
            opts.hasRange = true;
            opts.rangeStart = task.block_start;
            opts.rangeEnd = task.block_end;
        }

        // 重新提交下载任务
        std::string verifyHash = !task.file_hash.empty() ? task.file_hash : task.block_hash;
        std::string taskId = mDownloadMgr->addDownloadTask(task.url, verifyHash, opts);

        if (taskId.empty()) {
            logWarn << "Failed to resubmit task " << task.job_id;
            updateDeployTaskStatus(task.job_id, DeployStatus::FAILED);
        } else {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            mJobToTaskMap[task.job_id] = taskId;
            logInfo << "Resubmitted task " << task.job_id << " with new task_id: " << taskId;
        }
    }
}

void DeployManager::reportToServer(const std::string& job_id, bool success) {
    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logWarn << "MainManager instance is null, cannot report job " << job_id;
        return;
    }

    try {
        json report;
        report["job_id"] = job_id;
        report["code"] = success ? 1 : -1;  // 1:成功，-1:失败

        json response;
        long httpCode = mainMgr->ApiPost(mClient, "/api/v1/deploy/report", report, response);
        if (httpCode != 200) {
            logWarn << "Report failed for job " << job_id << " (HTTP code: " << httpCode << ")";
        } else {
            logInfo << "Successfully reported job " << job_id;
        }
    } catch (const std::exception& e) {
        logError << "Exception during report for job " << job_id << ": " << e.what();
    }
}

NS_END
