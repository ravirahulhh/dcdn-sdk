#ifndef _DCDN_SDK_DOWNLOAD_MANAGER_H_
#define _DCDN_SDK_DOWNLOAD_MANAGER_H_

#include <sqlite3.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "P2PDownloader.h"
#include "util/HttpDownloader.h"

namespace dcdn {

// 下载策略
enum class DownloadStrategy
{
    HTTP_ONLY, // 仅使用 HTTP 下载
    P2P_ONLY, // 仅使用 P2P 下载
    HYBRID // 混合模式（HTTP + P2P）
};

// 任务状态
enum class TaskStatus
{
    Pending, // 等待开始
    Running, // 正在运行
    Paused, // 已暂停
    Completed, // 已完成
    Failed, // 下载失败
    Cancelled // 已取消
};

// 表示使用方提交的一个总的下载任务（公有成员：首字母大写驼峰）
struct DownloadTask
{
    uint64_t Id = 0;                 // 任务 ID（唯一标识）
    std::string Url;                 // 下载 URL
    std::string ContentHash;         // 内容哈希（可选）
    size_t TotalSize = 0;            // 此次任务需下载的总长度（区间下载为区间长度；整文件为文件大小）
    size_t Downloaded = 0;           // 已下载字节数
    double Speed = 0;                // 下载速度（bytes/sec）
    std::chrono::system_clock::time_point StartTime;   // 任务开始时间
    std::chrono::system_clock::time_point LastUpdate;  // 上次进度更新时间
    std::atomic<bool> Paused{false};    // 是否暂停
    std::atomic<bool> Cancelled{false}; // 是否取消
    TaskStatus Status = TaskStatus::Pending;  // 当前任务状态

    // 已下载的区间（用于断点续传；尚未实现区间合并逻辑，这里仅保留接口）
    std::vector<std::pair<size_t, size_t>> CompletedRanges;

    // 赋值与构造
    DownloadTask& operator=(const DownloadTask& other)
    {
        Id = other.Id;
        Url = other.Url;
        ContentHash = other.ContentHash;
        TotalSize = other.TotalSize;
        Downloaded = other.Downloaded;
        Speed = other.Speed;
        StartTime = other.StartTime;
        LastUpdate = other.LastUpdate;
        Paused = other.Paused.load();
        Cancelled = other.Cancelled.load();
        Status = other.Status;
        CompletedRanges = other.CompletedRanges;
        return *this;
    }

    DownloadTask(const DownloadTask& other) { *this = other; }
    DownloadTask() {}

    // 按需持久化的序列化接口
    std::string Serialize() const;
    static DownloadTask Deserialize(const std::string& data);
};

// 回调定义（类型名保持不变；参数名不受命名规范强制要求）
using StreamCallback = std::function<void(const char* data, size_t size, size_t offset)>;
using BufferReadyCallback = std::function<void(uint64_t taskId, size_t start, size_t end)>;

// 文件下载选项（公有成员：首字母大写驼峰）
struct FileDownloadOptions
{
    std::string OutputPath;                     // 输出文件路径
    std::shared_ptr<std::ostream> OutputStream; // 输出流（可替代 OutputPath）
    StreamCallback StreamCb;                    // 流式回调（数据到达时调用）
    size_t ChunkSize;                           // 分片大小（默认 10MB）

    // 是否按区间下载
    // 若 HasRange = true：
    //   - RangeStart 有效；
    //   - RangeEnd == SIZE_MAX 表示下载到 EOF
    bool HasRange = false;
    size_t RangeStart = 0;
    size_t RangeEnd = SIZE_MAX;                 // inclusive；SIZE_MAX 表示未知结尾

    // 写入策略：
    //   - true  => 输出文件仅包含该区间内容，按相对偏移写入（0..length-1）
    //   - false => 按绝对偏移写入（RangeStart..RangeEnd）；可能导致生成稀疏文件
    bool WriteRangeToSeparateFile = true;

    // 下载策略
    DownloadStrategy Strategy = DownloadStrategy::HTTP_ONLY;

    FileDownloadOptions()
        : ChunkSize(10 * 1024 * 1024),
          RangeStart(0),
          RangeEnd(SIZE_MAX),
          Strategy(DownloadStrategy::HTTP_ONLY)
    {}
};

// DownloadManager 职责:
//  - 任务编排器
//  - 状态管理器
//  - 断点续传控制器
// 支持三种策略（HTTP_ONLY / P2P_ONLY / HYBRID）
// 支持媒体流式读取、带宽比例控制、任务持久化以及进度合并
class DownloadManager
{
public:
    explicit DownloadManager();
    ~DownloadManager();

    // ===== 配置（public 函数：首字母大写驼峰） =====
        // 配置
    void SetStrategy(DownloadStrategy strategy);
    void SetMaxConcurrentDownloads(size_t max);
    void SetPersistPath(const std::string& path);

    // ===== 任务管理：返回任务 ID =====
    uint64_t AddDownloadTask(
        const std::string& url,
        const std::string& contentHash = "",
        const FileDownloadOptions& options = {});
    bool CancelDownloadTask(uint64_t taskId);
    bool PauseDownloadTask(uint64_t taskId);
    bool ResumeDownloadTask(uint64_t taskId);

    // ===== 状态查询 =====
    DownloadTask GetTaskStatus(uint64_t taskId) const;
    std::vector<DownloadTask> GetAllTasks() const;
    double GetOverallSpeed() const;

    // ===== 流式播放支持 =====
    void SetBufferReadyCallback(uint64_t taskId, BufferReadyCallback callback);
    void RemoveBufferReadyCallback(uint64_t taskId);
    std::vector<std::pair<size_t, size_t>> GetAvailableRanges(uint64_t taskId) const;

    // ===== 带宽控制 =====
    void SetHttpBandwidthRatio(float ratio); // 0.0-1.0
    void SetP2pBandwidthRatio(float ratio);  // 0.0-1.0

private:
    // 内部任务分片（持久化/统计用；内部结构名与成员保留小驼峰风格以便区分）
    struct SubTask
    {
        size_t offset;                         // 分片起始偏移
        size_t length;                         // 分片长度
        std::shared_ptr<void> downloaderTask;  // 分片对应的下载任务对象
        bool completed = false;                // 分片是否完成
        int retryCount = 0;                    // 重试次数
    };

    // 数据持久化助手
    class PersistenceHelper
    {
    public:
        explicit PersistenceHelper(const std::string& dbPath);
        ~PersistenceHelper();

        bool saveTask(const DownloadTask& task);
        bool loadTasks(std::vector<DownloadTask>& tasks);
        bool deleteTask(uint64_t taskId);
        bool saveSubTasks(uint64_t taskId, const std::vector<SubTask>& subtasks);
        bool loadSubTasks(uint64_t taskId, std::vector<SubTask>& subtasks);

    private:
        sqlite3* mDb = nullptr;
        std::mutex mDbMutex;
    };

    // ===== 内部工具方法（private：首字母小写驼峰） =====
    void loadPersistedTasks();
    void persistTask(const DownloadTask& task);
    void removePersistedTask(uint64_t taskId);
    void updateTaskProgress(uint64_t taskId, size_t downloaded);
    void calculateSpeedLocked(DownloadTask& t, std::chrono::system_clock::time_point now);
    void checkTaskCompletion(uint64_t taskId);
    void notifyBufferReady(const uint64_t& taskId, size_t start, size_t end);

    void startHttpDownload(const uint64_t& taskId);
    void startP2pDownload(const uint64_t& taskId);
    void startHybridDownload(const uint64_t& taskId);

    // 在探测到 Content-Length 后 / 或已知范围时进行分片调度
    // totalSize：此次任务要下载的长度
    // baseOffset：此次任务的起始绝对偏移（整文件为0；区间下载为 RangeStart）
    void splitTask(uint64_t taskId, size_t totalSize, size_t baseOffset);

    bool maybeFinalizeTask(uint64_t taskId); // 幂等完成判定（去掉尾随下划线以符合规范）

private:
    // ===== 私有成员变量（m + 首字母大写驼峰） =====
    DownloadStrategy mStrategy = DownloadStrategy::HTTP_ONLY;
    size_t mMaxConcurrent = 4;
    std::string mPersistPath;

    std::unique_ptr<download::P2PDownloader> mP2pDownloader;
    std::unique_ptr<util::HttpDownloader> mHttpDownloader;
    std::unique_ptr<PersistenceHelper> mDbHelper;

    mutable std::mutex mTasksMutex;
    std::unordered_map<uint64_t, DownloadTask> mTasks;               // 所有任务信息
    std::unordered_map<uint64_t, std::vector<SubTask>> mSubTasks;    // 任务的分片信息
    std::unordered_map<uint64_t, FileDownloadOptions> mTaskOptions;  // 每个任务的下载选项
    std::unordered_map<uint64_t, BufferReadyCallback> mBufferCallbacks; // 缓存就绪回调

    float mHttpBandwidthRatio = 0.5f; // HTTP 带宽占比
    float mP2pBandwidthRatio = 0.5f;  // P2P 带宽占比
};

} // namespace dcdn

#endif // _DCDN_SDK_DOWNLOAD_MANAGER_H_
