#ifndef _DCDN_SDK_DOWNLOAD_MANAGER_H_
#define _DCDN_SDK_DOWNLOAD_MANAGER_H_

#include <sqlite3.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "BaseManager.h"
#include "EventLoop.h"
#include "Event.h"

#include "P2PDownloader.h"
#include "util/HttpDownloader.h"

namespace dcdn {

// ===================== 下载策略 / 任务状态 =====================
enum class DownloadStrategy
{
    HTTP_ONLY,  // 仅使用 HTTP 下载
    P2P_ONLY,   // 仅使用 P2P 下载
    HYBRID      // 混合模式（HTTP + P2P）
};

enum class TaskStatus
{
    Pending,     // 等待开始
    Running,     // 正在运行
    Paused,      // 已暂停
    Completed,   // 已完成
    Failed,      // 下载失败
    Cancelled    // 已取消
};

// ===================== 任务与选项（公有成员：大驼峰） =====================
struct DownloadTask
{
    uint64_t Id = 0;                 // 任务 ID（唯一标识）
    std::string Url;                 // 下载 URL
    std::string ContentHash;         // 内容哈希（可选）
    size_t TotalSize = 0;            // 需下载的总长度（区间为区间长度；整文件为文件大小）
    size_t Downloaded = 0;           // 已下载字节数
    double Speed = 0;                // 下载速度（bytes/sec）
    std::chrono::system_clock::time_point StartTime;   // 任务开始时间
    std::chrono::system_clock::time_point LastUpdate;  // 上次进度更新时间
    std::atomic<bool> Paused{false};    // 是否暂停
    std::atomic<bool> Cancelled{false}; // 是否取消
    TaskStatus Status = TaskStatus::Pending;           // 当前任务状态

    std::vector<std::pair<size_t, size_t>> CompletedRanges; // 已完成区间

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

    std::string Serialize() const;
    static DownloadTask Deserialize(const std::string& data);
};

using StreamCallback = std::function<void(const char* data, size_t size, size_t offset)>;
using BufferReadyCallback = std::function<void(uint64_t taskId, size_t start, size_t end)>;

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
    size_t RangeEnd = SIZE_MAX;                 // inclusive；SIZE_MAX 表示未知结尾（下载到 EOF）

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

// ===================== DownloadManager =====================
// - 继承 BaseManager（线程包装）+ EventLoop<DownloadManager>（事件循环）
// - public 函数/类名：大驼峰；private 成员/函数：m前缀 + 小驼峰
class DownloadManager : public BaseManager, public EventLoop<DownloadManager>
{
public:
    explicit DownloadManager();
    ~DownloadManager();

    // ===== 配置 =====
    void SetStrategy(DownloadStrategy strategy);
    void SetMaxConcurrentDownloads(size_t max);
    void SetPersistPath(const std::string& path);

    // ===== 任务管理 =====
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
    // ===================== 持久化辅助 =====================
    struct SubTask
    {
        size_t offset;
        size_t length;
        std::shared_ptr<void> downloaderTask;
        bool completed = false;
        int retryCount = 0;
    };

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

private:
    // ===================== 事件循环/线程 =====================
    void run() override; // BaseManager 要求实现线程主函数

    // 处理 FunctionCall 事件：把 std::function<void()> 直接执行
    void handleFunctionCall(std::shared_ptr<Event> evt);

    // 把 lambda 丢进事件线程串行执行（cmdQueue）
    template<typename R>
    R runSyncOnLoop(std::function<R()> fn);
    void runAsyncOnLoop(std::function<void()> fn);

    // ===================== 下载器通知与处理 =====================
    // 统一 downloader 通知入口（HttpDownloader 使用）
    static void coreNotifyCallback(std::shared_ptr<dcdn::util::DownloaderTask> task, void* receiver);
    void onDownloaderNotify(std::shared_ptr<dcdn::util::DownloaderTask> task);
    void processDownloaderEvent(std::shared_ptr<dcdn::util::DownloaderTask> task);

private:
    // ===================== 原内部工具方法（保留小驼峰） =====================
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

    bool maybeFinalizeTask(uint64_t taskId); // 幂等完成判定

private:
    // ===================== 事件循环下的"核心容器"（原 CoreContext） =====================
    struct ActiveSubTask
    {
        enum class Transport { HTTP, P2P };
        uint64_t parentTaskId = 0;
        Transport transport = Transport::HTTP;
        size_t offset = 0;
        size_t length = 0;
        int index = 0;
        std::shared_ptr<dcdn::util::DownloaderTask> downloader;
        std::shared_ptr<std::fstream> file;
        bool isProbe = false;
        uint64_t actualGot = 0;
        // NEW: 看门狗用的时间戳（最后一次有读进展的时间）
        std::chrono::steady_clock::time_point lastTouched = std::chrono::steady_clock::now();
    };
    struct Range { size_t start; size_t end; };

    // downloader 原生事件（用 map 去重）：raw* -> task shared_ptr
    std::unordered_map<dcdn::util::DownloaderTask*, std::shared_ptr<dcdn::util::DownloaderTask>> mDlEvents;
    // raw* -> ActiveSubTask
    std::unordered_map<dcdn::util::DownloaderTask*, ActiveSubTask> mActiveByPtr;
    // 父任务 -> 正在运行的 downloader
    std::unordered_map<uint64_t, std::vector<std::shared_ptr<dcdn::util::DownloaderTask>>> mDlByTask;
    // 父任务 -> 待调度分片
    std::unordered_map<uint64_t, std::deque<Range>> mPendingRanges;
    // 父任务 -> 下一个分片序号
    std::unordered_map<uint64_t, size_t> mNextRangeIdx;
    // 父任务 -> 共享随机写文件句柄
    std::unordered_map<uint64_t, std::shared_ptr<std::fstream>> mParentFiles;
    // 已取消的 raw*（丢弃迟到通知）
    std::unordered_set<dcdn::util::DownloaderTask*> mCancelledRaw;

private:
    // ===================== 任务/配置/下载器句柄 =====================
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


template<typename R>
R DownloadManager::runSyncOnLoop(std::function<R()> fn)
{
    // 使用 FunctionCall 事件把任务丢进事件循环串行执行
    this->PostEvent(std::make_shared<ArgEvent<std::function<void()>>>(
        EventType::FunctionCall,
        [prom = std::make_shared<std::promise<R>>(), fn = std::move(fn)]() mutable {
            try { prom->set_value(fn()); }
            catch (...) { try { prom->set_exception(std::current_exception()); } catch (...) {} }
        }
    ));
    // 上面需要把 promise 暴露出去，所以再拿一次引用
    // 为了简单，这里拆两步：先创建 promise，再再投个事件设置值
    auto prom = std::make_shared<std::promise<R>>();
    auto fut = prom->get_future();
    this->PostEvent(std::make_shared<ArgEvent<std::function<void()>>>(
        EventType::FunctionCall,
        [prom, fn = std::move(fn)]() mutable {
            try { prom->set_value(fn()); }
            catch (...) { try { prom->set_exception(std::current_exception()); } catch (...) {} }
        }
    ));
    return fut.get();
}

inline void DownloadManager::runAsyncOnLoop(std::function<void()> fn)
{
    this->PostEvent(std::make_shared<ArgEvent<std::function<void()>>>(
        EventType::FunctionCall, std::move(fn)));
}

} // namespace dcdn

#endif // _DCDN_SDK_DOWNLOAD_MANAGER_H_
