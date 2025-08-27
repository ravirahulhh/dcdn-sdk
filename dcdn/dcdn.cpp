#include "dcdn.h"

#include "DownloadManager.h"
#include "MainManager.h"

extern "C" {

int DcdnInit(const DcdnInitOption* opt)
{
    using namespace dcdn;

    MainManagerOption mopt;
    mopt.ApiKey = opt->ApiKey;
    mopt.WorkDir = opt->WorkDir;
    mopt.DeviceInfo = opt->Device;
    mopt.DeviceId = opt->Device.Id;
    mopt.DiskInfo = opt->Disk;
    mopt.ServerCfg = opt->ServerCfg;

    logInfo << "init main_manager ...";
    int ret = MainManager::Init(mopt);
    if (ret != 1) {
        logError << "init main_manager fail";
        return 1;
    }

    logInfo << "start main_manager ...";
    auto mainMgr = MainManager::Singlet();
    mainMgr->Start();

    return ret;
}

void DcdnUpdateNetworkInfo(const DcdnNetworkInfo* info)
{
    using namespace dcdn;

    if (!info) {
        logError << "DcdnUpdateNetworkInfo: info is null";
        return;
    }

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnUpdateNetworkInfo: MainManager not initialized";
        return;
    }

    // 更新网络信息到主管理器
    // 这里可以添加具体的网络信息更新逻辑
    logInfo << "Updated network info - IP: " << (info->ip ? info->ip : "null") << ", Port: " << info->port
            << ", Protocol: " << info->protocol << ", NAT Type: " << info->nat_type;
}

const char* DcdnGetHttpServer()
{
    using namespace dcdn;

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnGetHttpServer: MainManager not initialized";
        return nullptr;
    }

    // 返回HTTP服务器地址，这里返回API根URL
    static std::string httpServer = mainMgr->Cfg().ApiRootUrl();
    return httpServer.c_str();
}

int DcdnCreateDownloadTask(uint64_t* taskId, const DcdnDownloadTaskOption* opt)
{
    using namespace dcdn;

    if (!taskId || !opt || !opt->Url) {
        logError << "DcdnCreateDownloadTask: invalid parameters";
        return DcdnErrorCodeErr;
    }

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnCreateDownloadTask: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    auto downloadMgr = std::dynamic_pointer_cast<DownloadManager>(mainMgr->GetDownloadManager());
    if (!downloadMgr) {
        logError << "DcdnCreateDownloadTask: DownloadManager not available";
        return DcdnErrorCodeErr;
    }

    try {
        // 设置下载选项
        FileDownloadOptions options;
        options.OutputPath = opt->SavePath ? opt->SavePath : "";
        options.HasRange = (opt->Offset > 0);
        options.RangeStart = opt->Offset;
        options.Strategy = opt->DisableP2P ? DownloadStrategy::HTTP_ONLY : DownloadStrategy::HYBRID;

        // 创建下载任务
        TaskId id = downloadMgr->AddDownloadTask(opt->Url, "", options);
        *taskId = id;

        // 如果有回调函数，需要设置订阅
        if (opt->Callback) {
            downloadMgr->Subscribe([opt, id](const DMEvent& ev) {
                if (ev.id == id) {
                    opt->Callback(id, opt->UserData);
                }
            });
        }

        logInfo << "Created download task " << id << " for URL: " << opt->Url;
        return DcdnErrorCodeOk;
    } catch (const std::exception& e) {
        logError << "DcdnCreateDownloadTask failed: " << e.what();
        return DcdnErrorCodeErr;
    }
}

int DcdnCreateStreamTask(uint64_t* taskId, const DcdnStreamTaskOption* opt)
{
    using namespace dcdn;

    if (!taskId || !opt || !opt->Url) {
        logError << "DcdnCreateStreamTask: invalid parameters";
        return DcdnErrorCodeErr;
    }

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnCreateStreamTask: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    auto downloadMgr = std::dynamic_pointer_cast<DownloadManager>(mainMgr->GetDownloadManager());
    if (!downloadMgr) {
        logError << "DcdnCreateStreamTask: DownloadManager not available";
        return DcdnErrorCodeErr;
    }

    try {
        // 设置流任务选项
        FileDownloadOptions options;
        options.HasRange = (opt->Offset > 0);
        options.RangeStart = opt->Offset;
        options.Strategy = DownloadStrategy::Stream;
        options.maxStreamBufferBytes = opt->MaxBuf;

        // 创建流任务（不指定输出路径，表示流模式）
        TaskId id = downloadMgr->AddDownloadTask(opt->Url, "", options);
        *taskId = id;

        // 设置流数据回调
        if (opt->Callback) {
            options.StreamCb = opt->Callback;
            options.StreamDataCbReceiver = opt->UserData;
        }

        logInfo << "Created stream task " << id << " for URL: " << opt->Url;
        return DcdnErrorCodeOk;
    } catch (const std::exception& e) {
        logError << "DcdnCreateStreamTask failed: " << e.what();
        return DcdnErrorCodeErr;
    }
}

int DcdnTaskStatusIsEnd(DcdnTaskStatus st)
{
    return st < DcdnTaskIdle || st >= DcdnTaskCompleted;
}

int DcdnCancelTask(uint64_t taskId)
{
    using namespace dcdn;

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnCancelTask: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    auto downloadMgr = std::dynamic_pointer_cast<DownloadManager>(mainMgr->GetDownloadManager());
    if (!downloadMgr) {
        logError << "DcdnCancelTask: DownloadManager not available";
        return DcdnErrorCodeErr;
    }

    try {
        bool success = downloadMgr->CancelDownloadTask(taskId);
        if (success) {
            logInfo << "Cancelled task " << taskId;
            return DcdnErrorCodeOk;
        } else {
            logError << "Failed to cancel task " << taskId;
            return DcdnErrorCodeUnknownTask;
        }
    } catch (const std::exception& e) {
        logError << "DcdnCancelTask failed: " << e.what();
        return DcdnErrorCodeErr;
    }
}

int DcdnPauseTask(uint64_t taskId)
{
    using namespace dcdn;

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnPauseTask: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    auto downloadMgr = std::dynamic_pointer_cast<DownloadManager>(mainMgr->GetDownloadManager());
    if (!downloadMgr) {
        logError << "DcdnPauseTask: DownloadManager not available";
        return DcdnErrorCodeErr;
    }

    try {
        bool success = downloadMgr->PauseDownloadTask(taskId);
        if (success) {
            logInfo << "Paused task " << taskId;
            return DcdnErrorCodeOk;
        } else {
            logError << "Failed to pause task " << taskId;
            return DcdnErrorCodeUnknownTask;
        }
    } catch (const std::exception& e) {
        logError << "DcdnPauseTask failed: " << e.what();
        return DcdnErrorCodeErr;
    }
}

int DcdnResumeTask(uint64_t taskId)
{
    using namespace dcdn;

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnResumeTask: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    auto downloadMgr = std::dynamic_pointer_cast<DownloadManager>(mainMgr->GetDownloadManager());
    if (!downloadMgr) {
        logError << "DcdnResumeTask: DownloadManager not available";
        return DcdnErrorCodeErr;
    }

    try {
        bool success = downloadMgr->ResumeDownloadTask(taskId);
        if (success) {
            logInfo << "Resumed task " << taskId;
            return DcdnErrorCodeOk;
        } else {
            logError << "Failed to resume task " << taskId;
            return DcdnErrorCodeUnknownTask;
        }
    } catch (const std::exception& e) {
        logError << "DcdnResumeTask failed: " << e.what();
        return DcdnErrorCodeErr;
    }
}

int DcdnRemoveTask(uint64_t taskId)
{
    using namespace dcdn;

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnRemoveTask: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    auto downloadMgr = std::dynamic_pointer_cast<DownloadManager>(mainMgr->GetDownloadManager());
    if (!downloadMgr) {
        logError << "DcdnRemoveTask: DownloadManager not available";
        return DcdnErrorCodeErr;
    }

    try {
        // 先取消任务，然后移除
        bool success = downloadMgr->CancelDownloadTask(taskId);
        if (success) {
            // TODO: 删除文件
            logInfo << "Removed task " << taskId;
            return DcdnErrorCodeOk;
        } else {
            logError << "Failed to remove task " << taskId;
            return DcdnErrorCodeUnknownTask;
        }
    } catch (const std::exception& e) {
        logError << "DcdnRemoveTask failed: " << e.what();
        return DcdnErrorCodeErr;
    }
}

int DcdnGetTaskInfo(DcdnTaskInfo* info)
{
    using namespace dcdn;

    if (!info) {
        logError << "DcdnGetTaskInfo: info is null";
        return DcdnErrorCodeErr;
    }

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnGetTaskInfo: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    auto downloadMgr = std::dynamic_pointer_cast<DownloadManager>(mainMgr->GetDownloadManager());
    if (!downloadMgr) {
        logError << "DcdnGetTaskInfo: DownloadManager not available";
        return DcdnErrorCodeErr;
    }

    try {
        auto task = downloadMgr->GetTaskStatus(info->TaskId);

        // 转换任务状态
        switch (task.Status) {
            case dcdn::TaskStatus::Pending:
                info->Status = DcdnTaskIdle;
                break;
            case dcdn::TaskStatus::Running:
                info->Status = DcdnTaskRunning;
                break;
            case dcdn::TaskStatus::Paused:
                info->Status = DcdnTaskPaused;
                break;
            case dcdn::TaskStatus::Completed:
                info->Status = DcdnTaskCompleted;
                break;
            case dcdn::TaskStatus::Failed:
                info->Status = DcdnTaskFail;
                break;
            case dcdn::TaskStatus::Cancelled:
                info->Status = DcdnTaskCancelled;
                break;
            default:
                info->Status = DcdnTaskIdle;
                break;
        }

        info->ReceivedBytes = task.Downloaded;
        info->ContentLength = task.TotalSize;

        return DcdnErrorCodeOk;
    } catch (const std::exception& e) {
        logError << "DcdnGetTaskInfo failed: " << e.what();
        return DcdnErrorCodeUnknownTask;
    }
}

int DcdnReadTaskData(size_t taskId, DcdnReadTaskDataHandle handle, void* userData)
{
    using namespace dcdn;

    if (!handle) {
        logError << "DcdnReadTaskData: handle is null";
        return DcdnErrorCodeErr;
    }

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnReadTaskData: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    auto downloadMgr = std::dynamic_pointer_cast<DownloadManager>(mainMgr->GetDownloadManager());
    if (!downloadMgr) {
        logError << "DcdnReadTaskData: DownloadManager not available";
        return DcdnErrorCodeErr;
    }

    try {
        auto buffer = downloadMgr->ReadData(taskId);
        if (!buffer) {
            logDebug << "No data available for task " << taskId;
            return DcdnErrorCodeOk;
        }

        // 遍历缓冲区链表并调用回调函数
        auto current = buffer;
        while (current) {
            // 使用正确的方法名称
            handle(current->Offset(), current->Data(), current->Length(), userData);
            current = current->Next();
        }

        return DcdnErrorCodeOk;
    } catch (const std::exception& e) {
        logError << "DcdnReadTaskData failed: " << e.what();
        return DcdnErrorCodeErr;
    }
}

int DcdnUpdateUploadDir(const DcdnUploadDir* d)
{
    using namespace dcdn;

    if (!d || !d->Path) {
        logError << "DcdnUpdateUploadDir: invalid parameters";
        return DcdnErrorCodeErr;
    }

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnUpdateUploadDir: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    auto uploadMgr = mainMgr->GetFileManager(); // 文件管理器负责上传目录
    if (!uploadMgr) {
        logError << "DcdnUpdateUploadDir: FileManager not available";
        return DcdnErrorCodeErr;
    }

    try {
        // 这里需要根据实际的 FileManager 实现来更新上传目录
        logInfo << "Updated upload directory: " << d->Path << " with capacity: " << d->Capacity;

        // 实际实现需要调用 FileManager 的相关方法
        // uploadMgr->UpdateUploadDir(d->Path, d->Capacity);

        return DcdnErrorCodeOk;
    } catch (const std::exception& e) {
        logError << "DcdnUpdateUploadDir failed: " << e.what();
        return DcdnErrorCodeErr;
    }
}

int DcdnRemoveUploadDir(const char* path)
{
    using namespace dcdn;

    if (!path) {
        logError << "DcdnRemoveUploadDir: path is null";
        return DcdnErrorCodeErr;
    }

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnRemoveUploadDir: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    auto uploadMgr = mainMgr->GetFileManager(); // 文件管理器负责上传目录
    if (!uploadMgr) {
        logError << "DcdnRemoveUploadDir: FileManager not available";
        return DcdnErrorCodeErr;
    }

    try {
        // 这里需要根据实际的 FileManager 实现来移除上传目录
        logInfo << "Removed upload directory: " << path;

        // 实际实现需要调用 FileManager 的相关方法
        // uploadMgr->RemoveUploadDir(path);

        return DcdnErrorCodeOk;
    } catch (const std::exception& e) {
        logError << "DcdnRemoveUploadDir failed: " << e.what();
        return DcdnErrorCodeErr;
    }
}

int DcdnSetMaxUploadSpeed(uint64_t bytesPerSec)
{
    using namespace dcdn;

    auto mainMgr = MainManager::Singlet();
    if (!mainMgr) {
        logError << "DcdnSetMaxUploadSpeed: MainManager not initialized";
        return DcdnErrorCodeErr;
    }

    try {
        // 设置上传速率限制 - 需要获取非const引用
        const_cast<Config&>(mainMgr->Cfg()).SetUploadRate(bytesPerSec);

        logInfo << "Set max upload speed to " << bytesPerSec << " bytes/sec";
        return DcdnErrorCodeOk;
    } catch (const std::exception& e) {
        logError << "DcdnSetMaxUploadSpeed failed: " << e.what();
        return DcdnErrorCodeErr;
    }
}
}
