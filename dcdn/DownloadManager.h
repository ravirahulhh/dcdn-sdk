#ifndef _DCDN_SDK_DOWNLOAD_MANAGER_H_
#define _DCDN_SDK_DOWNLOAD_MANAGER_H_

#include "p2p_downloader.h"
#include "util/Downloader.h"
#include "util/HttpDownloader.h"
#include <sqlite3.h>
#include <functional>
#include <atomic>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>

namespace dcdn {

enum class DownloadStrategy {
    HTTP_ONLY,
    P2P_ONLY,
    HYBRID
};

enum class TaskStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled
};

// 表示使用方提交的一个总的下载任务
struct DownloadTask {
    std::string id;
    std::string url;
    std::string contentHash;
    size_t totalSize = 0;
    size_t downloaded = 0;
    double speed = 0; // bytes/sec
    std::chrono::system_clock::time_point startTime;
    std::chrono::system_clock::time_point lastUpdate;
    std::atomic<bool> paused{false};
    std::atomic<bool> cancelled{false};
    TaskStatus status = TaskStatus::Pending;

    // 已下载区间，用于断点续传
    std::vector<std::pair<size_t, size_t>> completedRanges;

    // copy assignment 
    DownloadTask& operator=(const DownloadTask& other) {
        id = other.id;
        url = other.url;
        contentHash = other.contentHash;
        totalSize = other.totalSize;
        downloaded = other.downloaded;
        speed = other.speed;
        startTime = other.startTime;
        lastUpdate = other.lastUpdate;
        paused = other.paused.load();
        cancelled = other.cancelled.load();
        status = other.status;
        completedRanges = other.completedRanges;
        return *this;
    }
    // copy constructor
    DownloadTask(const DownloadTask& other) { *this = other; }
    DownloadTask() {}

    std::string serialize() const;
    static DownloadTask deserialize(const std::string& data);
};

// 回调定义
using StreamCallback = std::function<void(const char* data, size_t size, size_t offset)>;
using BufferReadyCallback = std::function<void(const std::string& taskId, size_t start, size_t end)>;

struct FileDownloadOptions {
    std::string outputPath;
    std::shared_ptr<std::ostream> outputStream;
    StreamCallback streamCallback;
    size_t chunkSize;
    bool keepPartialOnCancel = false;  // 取消后是否保留半成品文件

    FileDownloadOptions() : chunkSize(100*1024 * 1024) {} 
};

// DownloadManager 职责: 任务编排器 + 状态管理器 + 断点续传控制器
// 兼容三种策略（HTTP_ONLY / P2P_ONLY / HYBRID），
// 支持媒体流式读取、带宽比例控制、
// 任务持久化以及进度合并
class DownloadManager {
public:
    explicit DownloadManager();
    ~DownloadManager();

    // 配置
    void setStrategy(DownloadStrategy strategy);
    void setMaxConcurrentDownloads(size_t max);
    void setPersistPath(const std::string& path);

    // 任务管理,返回任务 ID
    std::string addDownloadTask(
        const std::string& url, 
        const std::string& contentHash = "",
        const FileDownloadOptions& options = {}
    );
    bool cancelDownloadTask(const std::string& taskId);
    bool pauseDownloadTask(const std::string& taskId);
    bool resumeDownloadTask(const std::string& taskId);

    // 状态查询
    DownloadTask getTaskStatus(const std::string& taskId) const;
    std::vector<DownloadTask> getAllTasks() const;
    double getOverallSpeed() const;

    // 流式播放支持
    void setBufferReadyCallback(const std::string& taskId, BufferReadyCallback callback);
    void removeBufferReadyCallback(const std::string& taskId);
    std::vector<std::pair<size_t, size_t>> getAvailableRanges(const std::string& taskId) const;

    // 带宽控制
    void setHttpBandwidthRatio(float ratio); // 0.0-1.0
    void setP2pBandwidthRatio(float ratio);  // 0.0-1.0

private:
    // 内部任务分片（持久化/统计用）
    struct SubTask {
        size_t offset;
        size_t length;
        std::shared_ptr<void> downloaderTask;
        bool completed = false;
        int retryCount = 0;
    };

    // 数据持久化助手
    class PersistenceHelper {
    public:
        explicit PersistenceHelper(const std::string& dbPath);
        ~PersistenceHelper();

        bool saveTask(const DownloadTask& task);
        bool loadTasks(std::vector<DownloadTask>& tasks);
        bool deleteTask(const std::string& taskId);
        bool saveSubTasks(const std::string& taskId, const std::vector<SubTask>& subtasks);
        bool loadSubTasks(const std::string& taskId, std::vector<SubTask>& subtasks);

    private:
        sqlite3* db_;
        std::mutex dbMutex_;
    };

    // 内部工具方法
    void loadPersistedTasks();
    void persistTask(const DownloadTask& task);
    void removePersistedTask(const std::string& taskId);
    void updateTaskProgress(const std::string& taskId, size_t downloaded);
    void calculateSpeedLocked(DownloadTask& t,
                                 std::chrono::system_clock::time_point now);
    void checkTaskCompletion(const std::string& taskId);
    void notifyBufferReady(const std::string& taskId, size_t start, size_t end);

    void startHttpDownload(const std::string& taskId);
    void startP2pDownload(const std::string& taskId);
    void startHybridDownload(const std::string& taskId);

    // 在探测到 Content-Length 后切片
    void splitTask(const std::string& taskId, size_t totalSize);
    void onSubTaskCompleted(const std::string& taskId, size_t subtaskIndex);

    // 成员变量
    DownloadStrategy strategy_ = DownloadStrategy::HTTP_ONLY;
    size_t maxConcurrent_ = 4;
    std::string persistPath_;

    std::unique_ptr<download::P2PDownloader> p2pDownloader_;
    std::unique_ptr<util::HttpDownloader> httpDownloader_;
    std::unique_ptr<PersistenceHelper> dbHelper_;

    mutable std::mutex tasksMutex_;
    std::unordered_map<std::string, DownloadTask> tasks_;
    std::unordered_map<std::string, std::vector<SubTask>> subTasks_;
    std::unordered_map<std::string, FileDownloadOptions> taskOptions_;
    std::unordered_map<std::string, BufferReadyCallback> bufferCallbacks_;

    float httpBandwidthRatio_ = 0.5f;
    float p2pBandwidthRatio_ = 0.5f;
};

} // namespace dcdn

#endif // _DCDN_SDK_DOWNLOAD_MANAGER_H_
