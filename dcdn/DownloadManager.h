// DownloadManager.h
#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "BaseManager.h"
#include "Event.h"
#include "EventLoop.h"
#include "ThreadAnnotations.h"
#include "nlohmann/json_fwd.hpp"

namespace dcdn {

// forward declaration
namespace util {
class DownloaderTaskBuffer;
class HttpDownloader;
class DownloaderTask;
} // namespace util

namespace download {
class P2PDownloader;
} // namespace download

struct IFileStore
{
    virtual ~IFileStore() = default;
    // 按绝对偏移写入（若选择直写而非利用内部随机写文件）
    virtual bool Write(uint64_t taskId, size_t absOffset, const void* data, size_t len) = 0;
    // 预分配/截断到 totalSize（通常用于 OutputPath 文件）
    virtual bool Preallocate(uint64_t taskId, const std::string& path, size_t totalSize) = 0;
    // 关闭任务关联的文件（如有）
    virtual void Close(uint64_t taskId) = 0;
    // 删除文件
    virtual void Remove(const std::string& path) = 0;
};

struct IPersistence
{
    virtual ~IPersistence() = default;
    virtual void SaveTask(const std::string& serialized) = 0;
    virtual void RemoveTask(uint64_t id) = 0;
    virtual std::vector<std::string> LoadAll() = 0;
};

struct IClock
{
    virtual ~IClock() = default;
    virtual std::chrono::steady_clock::time_point NowSteady() const = 0;
    virtual std::chrono::system_clock::time_point NowWall() const = 0;
};

// 简化事件总线接口（可替代 DownloadManager 内部订阅）
struct IEventBus
{
    using Handler = std::function<void(const struct DMEvent&)>;
    using SubId = uint64_t;
    virtual ~IEventBus() = default;
    virtual SubId Subscribe(Handler h) = 0;
    virtual void Unsubscribe(SubId id) = 0;
    virtual void Publish(const struct DMEvent& ev) = 0;
};

// ===================== 策略 / 状态 / 公共类型 =====================
enum class DownloadStrategy
{
    HTTP_ONLY,
    P2P_ONLY,
    HYBRID,
    Stream, // audio. video etc.
};

enum class TaskStatus
{
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled
};

using TaskId = uint64_t;
typedef void(*StreamDataReadyCallback)(TaskId taskId, void* receiver);

struct FileDownloadOptions
{
    std::string OutputPath; // 若为空则可使用 OutputStream
    std::shared_ptr<std::ostream> OutputStream; // 可选；与 OutputPath 二选一

    size_t ChunkSize = 1u << 20; // 默认 1MB

    // Range
    bool HasRange = false;
    size_t RangeStart = 0;
    size_t RangeEnd = SIZE_MAX; // inclusive；SIZE_MAX 表示未知结尾（下载到 EOF）
    bool WriteRangeToSeparateFile = true; // 写入相对 Range 的局部文件（若使用 OutputPath）

    DownloadStrategy Strategy = DownloadStrategy::HTTP_ONLY;
    size_t MaxConcurrent = 4;

    IEventBus::Handler taskStateChangeEventCallback;

    // === Stream-only tuning ===
    void *StreamDataCbReceiver = nullptr;
    StreamDataReadyCallback StreamCb = nullptr;
    size_t StreamWarmupBytes = 256 * 1024; // 首播HTTP预热，默认256KB
    bool StreamFallbackHttpIfNoP2P = true; // P2P空/失败时是否回退HTTP
    bool HttpWarmupEnabled = false; // Stream 模式下是否启用HTTP预热
    size_t maxStreamBufferBytes = 50 * 1024 * 1024; // 流式传输最大缓存
};

inline FileDownloadOptions MakeDefaultOptions(DownloadStrategy strategy)
{
    FileDownloadOptions opt;
    opt.Strategy = strategy;

    if (strategy == DownloadStrategy::Stream) {
        opt.ChunkSize = 64 * 1024; // 小片（64KB），适合首屏快速响应
        opt.MaxConcurrent = 1; // 顺序下载，避免乱序
        opt.HasRange = false; // 默认整文件，按需裁剪
        opt.WriteRangeToSeparateFile = false; // 不分片写独立文件，直接顺序流
        opt.OutputPath.clear(); // 不落地文件，通常走 OutputStream 或回调
        opt.OutputStream = nullptr; // 留给调用方设置
        opt.StreamWarmupBytes = 256 * 1024;
        opt.StreamFallbackHttpIfNoP2P = true;
    } else {
        opt.ChunkSize = 1u << 20; // 1MB
        opt.MaxConcurrent = 8; //
    }

    return opt;
}

struct DownloadTask
{
    TaskId Id = 0;
    std::string Url;
    std::string ContentHash;

    size_t TotalSize = 0;
    size_t Downloaded = 0;

    double Speed = 0.0;
    std::chrono::system_clock::time_point StartTime{};
    std::chrono::system_clock::time_point LastUpdate{};

    std::atomic<bool> Paused{false};
    std::atomic<bool> Cancelled{false};
    TaskStatus Status = TaskStatus::Pending;

    std::vector<std::pair<size_t, size_t>> CompletedRanges;

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
    DownloadTask(const DownloadTask& other)
    {
        *this = other;
    }
    DownloadTask() = default;

    std::string Serialize() const;
    static DownloadTask Deserialize(const std::string& data);
};

// P2P 计划片段
struct PeerChunk
{
    std::string peerId;
    size_t start = 0;
    size_t end = 0; // inclusive
    std::string url; // url and hash can not be both empty
    std::string hash; //
    std::string iceUfrag;
    std::string icePwd;
    std::string remoteSdp;
};

// ===================== 领域事件（供上层订阅/总线分发） =====================
struct DMEvent
{
    TaskId id;
    DMEvent(TaskId id): id(id) {}
    virtual ~DMEvent() = default;
};

struct ETaskStatusChanged: public DMEvent
{
    TaskStatus from;
    TaskStatus to;
    ETaskStatusChanged(TaskId id, TaskStatus from, TaskStatus to): DMEvent(id), from(from), to(to) {}
};

struct ETaskProgress: DMEvent
{
    size_t downloaded;
    size_t total;
    ETaskProgress(TaskId id, size_t downloaded, size_t total): DMEvent(id), downloaded(downloaded), total(total) {}
};
// NOTE: not used currently but keep it in case
// struct EChunkScheduled: DMEvent
// {
//     enum class Via
//     {
//         HTTP,
//         P2P
//     } via;
//     size_t start;
//     size_t end;
//     size_t index;
//     EChunkScheduled(Via via, TaskId id, size_t start, size_t end, size_t index)
//         : via(via), DMEvent(id), start(start), end(end), index(index)
//     {
//     }
// };

struct ETaskError: DMEvent
{
    int code;
    std::string message;
    ETaskError(TaskId id, int code, const std::string& message): DMEvent(id), code(code), message(message) {}
};

struct EStreamBytes: DMEvent
{
    // pointer to the head of a linked list of buffers
    std::shared_ptr<util::DownloaderTaskBuffer> head;
    size_t start; // absolute
    size_t end; // inclusive
    bool contiguous; // 可选：是否保证 [start,end] 在单一连续切片内
    EStreamBytes(TaskId id, std::shared_ptr<util::DownloaderTaskBuffer> head, size_t start, size_t end, bool contiguous)
        : DMEvent(id), head(std::move(head)), start(start), end(end), contiguous(contiguous)
    {
    }
};

class DownloadManager: public BaseManager, public EventLoop<DownloadManager>
{
public:
    struct Dependencies
    {
        std::shared_ptr<util::HttpDownloader> http;
        std::shared_ptr<download::P2PDownloader> p2p;
        std::shared_ptr<IFileStore> files; // 随机写/预分配等（可选，若为空使用内部fstream）
        std::shared_ptr<IPersistence> persist; // 任务持久化（可选）
        std::shared_ptr<IClock> clock; // 时间源（可替换为仿真时钟）
        std::shared_ptr<IEventBus> bus; // 外置事件总线（可选）
    };
    explicit DownloadManager(std::shared_ptr<util::HttpDownloader> http, std::shared_ptr<download::P2PDownloader> p2p);
    explicit DownloadManager(const Dependencies& injected);
    explicit DownloadManager(Dependencies deps);
    ~DownloadManager();

    void SetStrategy(DownloadStrategy strategy);
    void SetMaxConcurrentDownloads(size_t max);
    void SetPersistPath(const std::string& path);

    // Thread Safe
    TaskId AddDownloadTask(
        const std::string& url,
        const std::string& contentHash = "",
        const FileDownloadOptions& options = {});
    bool CancelDownloadTask(TaskId taskId);
    bool PauseDownloadTask(TaskId taskId);
    bool ResumeDownloadTask(TaskId taskId);

    // Thread Safe
    DownloadTask GetTaskStatus(TaskId taskId) const;
    std::vector<DownloadTask> GetAllTasks() const;
    double GetOverallSpeed() const;

    // 所有的事件订阅最终都走这里
    using SubId = uint64_t;
    using Handler = std::function<void(const DMEvent&)>;
    SubId Subscribe(Handler h);
    void Unsubscribe(SubId id);

    // 供上层调用的便捷接口(对事件订阅的语法糖封装)
    void SubscribeStream(TaskId taskId, StreamDataReadyCallback callback);
    void RemoveStreamCallback(TaskId taskId);
    std::shared_ptr<util::DownloaderTaskBuffer> ReadData(TaskId taskId);

    void Init();

private:
    // ===================== 线程 / 事件循环 =====================
    void run() override REQUIRES(dm_thread());
    void handleFunctionCall(std::shared_ptr<Event> evt);

    static void coreNotifyCallbackHttp(std::shared_ptr<util::DownloaderTask> task, void* receiver);
    static void coreNotifyCallbackP2p(std::shared_ptr<util::DownloaderTask> task, void* receiver);

    // —— 对 downloader 的"一次事件"做读取, 转成领域事件交给 FSM
    void processHttpEvent(std::shared_ptr<util::DownloaderTask> ev) REQUIRES(dm_thread());
    void processP2pEvent(std::shared_ptr<util::DownloaderTask> ev) REQUIRES(dm_thread());

private:
    // ===================== FSM：命令 / IO / 计划 / 看门狗 -> 状态转移 =====================
    struct IOData
    {
        TaskId id; // parent task id
        size_t absOffset = 0; // 数据的绝对偏移
        std::shared_ptr<util::DownloaderTaskBuffer> head; // data buffer chain head
        size_t segStart = 0; // 所属分片起始偏移
        size_t segEnd = 0; // 所属分片结束偏移（闭区间）

        bool isProbe = false; // 是否为探测任务
        size_t probeLenSnapshot = 0; // 仅isProbe时有效,表示data数据长度
        size_t contentLenHint = 0; // 仅isProbe时有效,探测到的 content-length
        std::string peerId; // 如果是 P2P，记录 peerId；HTTP 留空
    };
    struct IOEnd
    {
        TaskId id;
        bool ok = false; // 下载是否成功
        size_t actuallyGot = 0; // 实际收到的数据长度
        size_t segStart = 0;
        size_t segEnd = 0;
        bool isProbe = false;

        std::string peerId;
        size_t contentLen = 0; // probe 可选字段：返回的 content-length
    };
    struct TaskFailed
    {
        int errorCode;
        std::string message;
    };
    struct FinalizeCheck
    {
        TaskId id;
    };
    struct PlanReady
    {
        TaskId id;
        std::vector<PeerChunk> chunks;
        size_t totalSizeHint = 0;
        size_t startHint = 0, endHint = SIZE_MAX; // 可选：这次覆盖的目标区间
    };
    struct StallFound
    {
        TaskId id;
        size_t segStart;
        size_t segEnd;
    };

    struct CmdAdd
    {
        std::string url, hash;
        FileDownloadOptions opt;
    };
    struct CmdPause
    {
        TaskId id;
    };
    struct CmdResume
    {
        TaskId id;
    };
    struct CmdCancel
    {
        TaskId id;
    };

    // outside must hold mTasksMutex when call this
    // the only place that changes task status
    void transitionLocked_(TaskId id, TaskStatus to) REQUIRES(mTasksMutex, dm_thread());
    void applyFsm_(const IOData& ev) REQUIRES(dm_thread());
    void applyFsm_(const IOEnd& ev) REQUIRES(dm_thread());
    void applyFsm_(TaskId id, const TaskFailed& ev) REQUIRES(dm_thread());
    void applyFsm_(TaskId id, const FinalizeCheck&) REQUIRES(dm_thread());
    void applyFsm_(TaskId, const PlanReady&) REQUIRES(dm_thread());
    void applyFsm_(TaskId, const StallFound&) REQUIRES(dm_thread());
    void applyFsm_(TaskId, const CmdPause&) REQUIRES(dm_thread());
    void applyFsm_(TaskId, const CmdResume&) REQUIRES(dm_thread());
    void applyFsm_(TaskId, const CmdCancel&) REQUIRES(dm_thread());
    void applyFsm_(TaskId, const CmdAdd&) REQUIRES(dm_thread());

private:
    // ===================== 调度/副作用（由 FSM 调用） =====================
    // —— HTTP
    void ensurePreallocate_(TaskId id, size_t totalSize) REQUIRES(dm_thread());
    void splitTask(TaskId id, size_t probedDataLen, size_t totalSize, size_t baseOffset) REQUIRES(dm_thread());
    // 规划 HTTP 范围并派发首批分片（替代原 splitTask）
    void planAndDispatchHttp_(
        TaskId id,
        const FileDownloadOptions& opts,
        size_t probedDataLen,
        size_t totalSize,
        size_t baseOffset) REQUIRES(dm_thread());
    void cancelHttpProbeIfAny_(TaskId id) REQUIRES(dm_thread());
    void scheduleHttpProbe_(TaskId id, const std::string& url) REQUIRES(dm_thread());
    void scheduleHttpChunk_(TaskId id, const std::string& url, size_t start, size_t end) REQUIRES(dm_thread());

    // —— P2P
    bool p2pQueryPeersAsync_(TaskId id, size_t start, size_t end) REQUIRES(dm_thread());
    void onP2PPeerQuerySuccess_(TaskId id, nlohmann::json res, size_t start, size_t end) REQUIRES(dm_thread());
    void onP2PPeerQueryFail_(TaskId id, int errCode, size_t start) REQUIRES(dm_thread());

    void scheduleP2PChunks_(TaskId id, const std::vector<PeerChunk>& plan, size_t start, size_t end)
        REQUIRES(dm_thread());
    bool startOneP2PChunk_(TaskId id, const PeerChunk& pc) REQUIRES(dm_thread());

    void cancelAllSubs_(TaskId id, bool http = true, bool p2p = true) REQUIRES(dm_thread());
    void updateTaskProgress(TaskId id, size_t downloaded) REQUIRES(dm_thread());
    void calculateSpeedLocked(DownloadTask& t, std::chrono::system_clock::time_point now) REQUIRES(mTasksMutex);
    void
    notifyDataReady(const TaskId& taskId, std::shared_ptr<util::DownloaderTaskBuffer> head, size_t start, size_t end)
        REQUIRES(dm_thread());

private:
    // 运行时状态
    struct ActiveSubTask
    {
        enum class Transport
        {
            HTTP,
            P2P
        };
        Transport transport = Transport::HTTP;
        TaskId parentTaskId = 0;

        size_t offset = 0; // 负责的绝对起点
        size_t length = 0; // 负责的长度
        int index = 0; // 分片序号

        std::shared_ptr<util::DownloaderTask> downloader; // 引擎子任务
        std::shared_ptr<std::fstream> file; // 写入文件fd（可选）
        bool isProbe = false; // HTTP Probe
        uint64_t actualGot = 0;

        std::chrono::steady_clock::time_point lastTouched = std::chrono::steady_clock::now();
    };

    struct Range
    {
        size_t start;
        size_t end; // inclusive
    };

    mutable dcdn::AnnotatedMutex mEventMutex;
    // —— HTTP
    // taskid --> util::DownloaderTask --> ActiveSubTask
    std::unordered_map<TaskId, std::vector<std::shared_ptr<util::DownloaderTask>>> mDlByTaskHttp
        GUARDED_BY(dm_thread());
    std::unordered_map<util::DownloaderTask*, ActiveSubTask> mActiveByPtrHttp GUARDED_BY(dm_thread());
    std::unordered_map<util::DownloaderTask*, std::shared_ptr<util::DownloaderTask>> mDlEventsHttp
        GUARDED_BY(mEventMutex);

    // —— P2P
    std::unordered_map<TaskId, std::vector<std::shared_ptr<util::DownloaderTask>>> mDlByTaskP2p GUARDED_BY(dm_thread());
    std::unordered_map<util::DownloaderTask*, ActiveSubTask> mActiveByPtrP2p GUARDED_BY(dm_thread());
    std::unordered_map<util::DownloaderTask*, std::shared_ptr<util::DownloaderTask>> mDlEventsP2p
        GUARDED_BY(mEventMutex);

    // 待调度队列与文件句柄
    std::unordered_map<TaskId, std::deque<Range>> mPendingRanges GUARDED_BY(dm_thread());
    std::unordered_map<TaskId, size_t> mNextRangeIdx GUARDED_BY(dm_thread());
    std::unordered_map<TaskId, std::shared_ptr<std::fstream>> mParentFiles GUARDED_BY(dm_thread());

    std::unordered_set<util::DownloaderTask*> mCancelledRawHttp
        GUARDED_BY(mEventMutex); // access by downloader thread and dm_thread
    std::unordered_set<util::DownloaderTask*> mCancelledRawP2p
        GUARDED_BY(mEventMutex); // access by downloader thread and dm_thread

    // p2p query management
    struct P2PTaskState
    {
        void* queryReqId = nullptr;
        bool queryInFlight = false;
        bool queryDone = false;
        int lastQueryErr = 0;
        size_t offset = 0;

        std::vector<PeerChunk> plan;
    };
    // taskID -> {offset -> state}
    std::unordered_map<TaskId, std::unordered_map<size_t, P2PTaskState>> p2pStates_ GUARDED_BY(dm_thread());

    std::vector<Range> computeHttpRanges_(size_t rangeStart, size_t absEnd, size_t chunk) const;
    // 纯函数：计算用于切分的 chunk 大小
    // 规则：
    //  - 若提供了 ChunkSize 则优先使用；
    //  - 否则默认 1MB；
    //  - 若 MaxConcurrent>0，则与 totalSize/MaxConcurrent 取 max（保证并发下每块不至过小）；
    //  - Stream 模式下不按并发放大（保持顺序步长）。
    size_t computeChunkSize_(size_t totalSize, const FileDownloadOptions& opts, bool isStream) const;

private:
    DownloadStrategy mStrategy = DownloadStrategy::HTTP_ONLY;
    size_t mMaxConcurrent = 4;
    std::string mPersistPath;

    Dependencies mDeps;

    std::shared_ptr<download::P2PDownloader> mP2pDownloader;
    std::shared_ptr<util::HttpDownloader> mHttpDownloader;

    mutable dcdn::AnnotatedMutex mTasksMutex;
    std::unordered_map<TaskId, DownloadTask> mTasks GUARDED_BY(mTasksMutex);
    std::unordered_map<TaskId, FileDownloadOptions> mTaskOptions GUARDED_BY(mTasksMutex);

    mutable dcdn::AnnotatedMutex mSubMutex;
    SubId mNextSubId GUARDED_BY(mSubMutex){1};
    std::unordered_map<SubId, Handler> mSubscribers GUARDED_BY(mSubMutex);
    // SubscribeStream/RemoveStreamCallback
    std::unordered_map<TaskId, SubId> mStreamSubByTask GUARDED_BY(mSubMutex);
    float mHttpBandwidthRatio = 0.5f;
    float mP2pBandwidthRatio = 0.5f;

    std::atomic<TaskId> mLastCreatedTaskId GUARDED_BY(dm_thread()){0};

#ifdef DCDN_DM_TESTING
    friend class dcdn::TestHook;
#endif

private:
    void watchdogHandleMayStalledSubtask(
        const ActiveSubTask& st,
        std::chrono::steady_clock::time_point now,
        const std::chrono::seconds& stallTimeout) REQUIRES(dm_thread());
    void handleHttpSubtaskStall(const ActiveSubTask& st, std::chrono::steady_clock::time_point now)
        REQUIRES(dm_thread());
    void handleP2pSubtaskStall(const ActiveSubTask& st, std::chrono::steady_clock::time_point now)
        REQUIRES(dm_thread());
    bool canFinalize_(TaskId id) REQUIRES(dm_thread());

private:
    class PersistenceHelper
    {
    public:
        explicit PersistenceHelper(const std::string& dbPath);
        ~PersistenceHelper();

        bool saveTask(const DownloadTask& task);
        bool loadTasks(std::vector<DownloadTask>& tasks);
        bool deleteTask(TaskId taskId);

        struct SubTaskRec
        {
            size_t offset = 0;
            size_t length = 0;
            bool completed = false;
            int retryCount = 0;
        };
        bool saveSubTasks(TaskId taskId, const std::vector<SubTaskRec>& subtasks);
        bool loadSubTasks(TaskId taskId, std::vector<SubTaskRec>& subtasks);

    private:
        void* mDb = nullptr;
        std::mutex mDbMutex;
    };

    std::unique_ptr<PersistenceHelper> mDbHelper;

    void publish_(const DMEvent& ev) REQUIRES(dm_thread());

private:
    // Stream 模式per-task 状态，标记当前处于Warmup(HTTP) 还是 P2P 阶段，并记住 warmup 结束位置
    struct StreamState
    {
        enum class Phase
        {
            None,
            WarmupHttp,
            P2P
        } phase = Phase::None;
        size_t warmupEnd = 0; // inclusive
    };
    std::unordered_map<TaskId, StreamState> mStreamState GUARDED_BY(dm_thread());

private:
    DmLoopThreadCap loop_cap_;
    DmLoopThreadCap dm_thread()
    {
        return loop_cap_;
    }
};

} // namespace dcdn
