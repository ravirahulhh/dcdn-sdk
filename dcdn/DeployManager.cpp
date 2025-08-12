#include "DeployManager.h"
#include "MainManager.h"
#include "Event.h"
#include <plog/Log.h>
#include <filesystem>
#include <chrono>
#include <thread>

// ==== sqlite_orm 与 DeployStatus 枚举映射适配 START ====
namespace sqlite_orm {

template<>
struct type_printer<dcdn::DeployStatus> : public integer_printer {};

template<>
struct statement_binder<dcdn::DeployStatus> {
    int bind(sqlite3_stmt* stmt, int index, const dcdn::DeployStatus& value) {
        return statement_binder<int>().bind(stmt, index, static_cast<int>(value));
    }
};

template<>
struct field_printer<dcdn::DeployStatus> {
    std::string operator()(const dcdn::DeployStatus& t) const {
        return std::to_string(static_cast<int>(t));
    }
};

template<>
struct row_extractor<dcdn::DeployStatus> {
    static dcdn::DeployStatus extract(const char* row_value) {
        return static_cast<dcdn::DeployStatus>(std::atoi(row_value));
    }
    static dcdn::DeployStatus extract(sqlite3_stmt* stmt, int columnIndex) {
        return static_cast<dcdn::DeployStatus>(sqlite3_column_int(stmt, columnIndex));
    }
};

} // namespace sqlite_orm
// ==== sqlite_orm 与 DeployStatus 枚举映射适配 END ====

NS_BEGIN(dcdn)

DeployManager::DeployManager(MainManager* man) : BaseManager(man) {
    mFileMgr = man->getFileManager();
    mDownloadMgr = man->getDownloadManager();

    if (!mFileMgr) {
        LOGW << "FileManager instance is null in DeployManager";
    }
    if (!mDownloadMgr) {
        LOGW << "DownloadManager instance is null in DeployManager";
    }

    if (createTable() != 0) {
        LOGE << "DeployManager database initialization failed";
    } else {
        LOGI << "DeployManager database initialized successfully";
    }

    registerHandler(EventType::DeployMsg, &DeployManager::handleDeployMsgEvent);

    resubmitDownloadTasks();
}

DeployManager::~DeployManager() {
    std::lock_guard<std::mutex> lock(mTaskMutex);
    mJobToTaskMap.clear();
}

int DeployManager::createTable() {
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

DeployStoragePtr DeployManager::getDB() {
    if (!mDB) {
        try {
            auto storage = std::make_shared<DeployStorage>(makeDeployStorage(dbPath().string()));
            mDB = storage;
        } catch (const std::exception& e) {
            LOGE << "Initialize database failed: " << e.what();
            return nullptr;
        }
    }
    return mDB;
}

void DeployManager::run() {
    LOGI << "DeployManager started";
    while (true) {
        checkDownloadStatus();
        waitAllEvents(std::chrono::seconds(1));
    }
    LOGI << "DeployManager stopped";
}

void DeployManager::handleDeployMsgEvent(std::shared_ptr<Event> evt) {
    LOGI << "Received DeployMsg event";

    auto* e = static_cast<ArgEvent<DeployMsgArg>*>(evt.get());
    if (!e) {
        LOGW << "Invalid DeployMsg event: wrong argument type";
        return;
    }
    const auto& arg = e->Arg();

    if (arg.job_id.empty()) {
        LOGW << "DeployMsg missing required field: job_id";
        return;
    }

    if (arg.file_hash.empty() || arg.url.empty()) {
        LOGW << "DeployMsg invalid: missing url or hash (job_id: " << arg.job_id << ")";
        reportToServer(arg.job_id, false);
        return;
    }

    if (!mFileMgr) {
        LOGE << "FileManager is not available (job_id: " << arg.job_id << ")";
        reportToServer(arg.job_id, false);
        return;
    }

    std::string downloadPath = mFileMgr->NewDownloadPath(0);
    if (downloadPath.empty()) {
        LOGW << "Failed to get download path (job_id: " << arg.job_id << ")";
        reportToServer(arg.job_id, false);
        return;
    }

    DeployTask task;
    task.job_id = arg.job_id;
    task.file_hash = arg.file_hash;
    task.block_hash = arg.block_info.hash;
    task.block_start = arg.block_info.start;
    task.block_end = arg.block_info.end;
    task.url = arg.url;
    task.status = DeployStatus::DOWNLOADING;
    task.download_path = downloadPath;
    task.create_time = getCurrentTimestamp();
    task.update_time = task.create_time;

    if (!saveDeployTask(task)) {
        LOGW << "Failed to save deploy task (job_id: " << arg.job_id << ")";
        reportToServer(arg.job_id, false);
        return;
    }

    if (!mDownloadMgr) {
        LOGE << "DownloadManager is not available (job_id: " << arg.job_id << ")";
        updateDeployTaskStatus(arg.job_id, DeployStatus::FAILED);
        reportToServer(arg.job_id, false);
        return;
    }

    FileDownloadOptions opts;
    opts.outputPath = downloadPath;
    if (task.block_start > 0 || task.block_end > 0) {
        opts.hasRange = true;
        opts.rangeStart = task.block_start;
        opts.rangeEnd = task.block_end;
    }

    std::string taskId = mDownloadMgr->addDownloadTask(task.url, task.file_hash, opts);
    if (taskId.empty()) {
        LOGW << "Failed to create download task (job_id: " << arg.job_id << ")";
        updateDeployTaskStatus(arg.job_id, DeployStatus::FAILED);
        reportToServer(arg.job_id, false);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mTaskMutex);
        mJobToTaskMap[arg.job_id] = taskId;
    }
    LOGI << "Deploy task initialized (job_id: " << arg.job_id << ", task_id: " << taskId << ")";
}

void DeployManager::checkDownloadStatus() {
    auto tasks = loadDownloadingTasks();
    if (tasks.empty()) {
        return;
    }

    if (!mDownloadMgr || !mFileMgr) {
        LOGW << "FileManager or DownloadManager is unavailable, skip status check";
        return;
    }

    for (const auto& task : tasks) {
        std::string taskId;
        {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            auto it = mJobToTaskMap.find(task.job_id);
            if (it == mJobToTaskMap.end()) {
                LOGW << "No download task ID found for job: " << task.job_id;
                continue;
            }
            taskId = it->second;
        }

        auto downloadStatus = mDownloadMgr->getTaskStatus(taskId);

        switch (downloadStatus.status) {
            case TaskStatus::Completed: {
                FileDownloadDoneArg doneArg;
                doneArg.file_hash = task.file_hash;
                doneArg.block_info.hash = task.block_hash;
                doneArg.block_info.start = task.block_start;
                doneArg.block_info.end = task.block_end;
                doneArg.url = task.url;
                doneArg.file_path = task.download_path;

                mFileMgr->PostEvent(std::make_shared<ArgEvent<FileDownloadDoneArg>>(EventType::FileDownloadDone, std::move(doneArg)));

                updateDeployTaskStatus(task.job_id, DeployStatus::COMPLETED);

                {
                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mJobToTaskMap.erase(task.job_id);
                }

                reportToServer(task.job_id, true);
                LOGI << "Deploy task completed (job_id: " << task.job_id << ")";
                break;
            }
            case TaskStatus::Failed:
            case TaskStatus::Cancelled: {
                FileDownloadFailedArg failArg;
                failArg.file_path = task.download_path;

                mFileMgr->PostEvent(std::make_shared<ArgEvent<FileDownloadFailedArg>>(EventType::FileDownloadFailed, std::move(failArg)));

                updateDeployTaskStatus(task.job_id, DeployStatus::FAILED);

                {
                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mJobToTaskMap.erase(task.job_id);
                }

                reportToServer(task.job_id, false);
                LOGW << "Deploy task failed (job_id: " << task.job_id << ")";
                break;
            }
            default: {
                DeployTask updateTask = task;
                updateTask.update_time = getCurrentTimestamp();
                saveDeployTask(updateTask);
                break;
            }
        }
    }
}

bool DeployManager::saveDeployTask(const DeployTask& task) {
    auto db = getDB();
    if (!db) return false;

    try {
        db->replace(task);
        return true;
    } catch (const std::exception& e) {
        LOGE << "Save deploy task failed (job_id: " << task.job_id << "): " << e.what();
        return false;
    }
}

bool DeployManager::updateDeployTaskStatus(const std::string& job_id, DeployStatus status) {
    auto db = getDB();
    if (!db) return false;

    try {
        using namespace sqlite_orm;
        db->update_all(
            set(
                c(&DeployTask::status) = status,
                c(&DeployTask::update_time) = getCurrentTimestamp()
            ),
            where(c(&DeployTask::job_id) == job_id)
        );
        return true;
    } catch (const std::exception& e) {
        LOGE << "Update task status failed (job_id: " << job_id << "): " << e.what();
        return false;
    }
}

std::vector<DeployTask> DeployManager::loadDownloadingTasks() {
    auto db = getDB();
    if (!db) return {};

    try {
        using namespace sqlite_orm;
        return db->get_all<DeployTask>(
            where(c(&DeployTask::status) == DeployStatus::DOWNLOADING)
        );
    } catch (const std::exception& e) {
        LOGE << "Load downloading tasks failed: " << e.what();
        return {};
    }
}

void DeployManager::resubmitDownloadTasks() {
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
        if (task.download_path.empty()) {
            LOGW << "Invalid download path for task " << task.job_id << ", skipping";
            continue;
        }

        FileDownloadOptions opts;
        opts.outputPath = task.download_path;
        if (task.block_start > 0 || task.block_end > 0) {
            opts.hasRange = true;
            opts.rangeStart = task.block_start;
            opts.rangeEnd = task.block_end;
        }

        std::string taskId = mDownloadMgr->addDownloadTask(task.url, task.file_hash, opts);
        if (taskId.empty()) {
            LOGW << "Failed to resubmit task " << task.job_id;
            updateDeployTaskStatus(task.job_id, DeployStatus::FAILED);
        } else {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            mJobToTaskMap[task.job_id] = taskId;
            LOGI << "Resubmitted task " << task.job_id << " with new task_id: " << taskId;
        }
    }
}

void DeployManager::reportToServer(const std::string& job_id, bool success) {
    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        LOGW << "MainManager instance is null, cannot report job " << job_id;
        return;
    }
    try {
        json report;
        std::string peer_id = mMan->Cfg().PeerId();
        if (peer_id.empty()) {
            LOGW << "peer_id is empty, using default";
            peer_id = "unknown_peer";
        }

        json event;
        event["peer_id"] = peer_id;
        event["type"] = "deploy_result";
        event["kvs"] = {
            {"job_id", job_id},
            {"code", success ? "0" : "1"}
        };

        json request;
        request["events"] = {event};

        json response;
        long httpCode = mainMgr->ApiPost(mClient, "/api/v1/report_event", request, response);
        if (httpCode != 200) {
            LOGW << "Report failed for job " << job_id << " (HTTP code: " << httpCode << ")";
        } else {
            LOGI << "Successfully reported job " << job_id;
        }
    } catch (const std::exception& e) {
        LOGE << "Exception during report for job " << job_id << ": " << e.what();
    }
}

NS_END
