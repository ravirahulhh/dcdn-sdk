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
#include "nlohmann/json_fwd.hpp"

namespace dcdn {

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
    HYBRID
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
using StreamCallback = std::function<void(const char* data, size_t size, size_t offset)>;
using BufferReadyCallback = std::function<void(TaskId taskId, size_t start, size_t end)>;

struct FileDownloadOptions
{
    std::string OutputPath; // 若为空则可使用 OutputStream
    std::shared_ptr<std::ostream> OutputStream; // 可选；与 OutputPath 二选一
    StreamCallback StreamCb; // 可选：直写回调

    size_t ChunkSize = 1u << 20; // 默认 1MB

    // Range
    bool HasRange = false;
    size_t RangeStart = 0;
    size_t RangeEnd = SIZE_MAX; // inclusive；SIZE_MAX 表示未知结尾（下载到 EOF）
    bool WriteRangeToSeparateFile = true; // 写入相对 Range 的局部文件（若使用 OutputPath）

    // 策略与并发
    DownloadStrategy Strategy = DownloadStrategy::HTTP_ONLY;
    size_t MaxConcurrent = 4;
};

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
    virtual ~DMEvent() = default;
};
struct ETaskStatusChanged: DMEvent
{
    TaskId id;
    TaskStatus from;
    TaskStatus to;
    ETaskStatusChanged(TaskId id, TaskStatus from, TaskStatus to): id(id), from(from), to(to) {}
};
struct ETaskProgress: DMEvent
{
    TaskId id;
    size_t downloaded;
    size_t total;
    ETaskProgress(TaskId id, size_t downloaded, size_t total): id(id), downloaded(downloaded), total(total) {}
};
struct EBufferReady: DMEvent
{
    TaskId id;
    size_t start;
    size_t end;
    EBufferReady(TaskId id, size_t start, size_t end): id(id), start(start), end(end) {}
};
struct EChunkScheduled: DMEvent
{
    enum class Via
    {
        HTTP,
        P2P
    } via;
    TaskId id;
    size_t start;
    size_t end;
    size_t index;
    EChunkScheduled(Via via, TaskId id, size_t start, size_t end, size_t index)
        : via(via), id(id), start(start), end(end), index(index)
    {
    }
};
struct ETaskError: DMEvent
{
    TaskId id;
    int code;
    std::string message;
    ETaskError(TaskId id, int code, const std::string& message): id(id), code(code), message(message) {}
};

namespace util {
class DownloaderTaskBuffer;
class HttpDownloader;
class DownloaderTask;
} // namespace util

namespace download {
class P2PDownloader;
} // namespace download

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

    // ===== 配置 =====
    void SetStrategy(DownloadStrategy strategy);
    void SetMaxConcurrentDownloads(size_t max);
    void SetPersistPath(const std::string& path);

    // ===== 任务管理（线程安全：投递到事件循环）=====
    TaskId AddDownloadTask(
        const std::string& url,
        const std::string& contentHash = "",
        const FileDownloadOptions& options = {});
    bool CancelDownloadTask(TaskId taskId);
    bool PauseDownloadTask(TaskId taskId);
    bool ResumeDownloadTask(TaskId taskId);

    // ===== 状态查询（加锁快照）=====
    DownloadTask GetTaskStatus(TaskId taskId) const;
    std::vector<DownloadTask> GetAllTasks() const;
    double GetOverallSpeed() const;

    // ===== 数据区间可用（播放器/解复用）=====
    void SetBufferReadyCallback(TaskId taskId, BufferReadyCallback callback);
    void RemoveBufferReadyCallback(TaskId taskId);
    std::vector<std::pair<size_t, size_t>> GetAvailableRanges(TaskId taskId) const;

    // ===== 带宽控制（预留）=====
    void SetHttpBandwidthRatio(float ratio); // 0.0-1.0
    void SetP2pBandwidthRatio(float ratio); // 0.0-1.0

    // ===== 内部轻量订阅（同时也会向外部 IEventBus 发布）=====
    using SubId = uint64_t;
    using Handler = std::function<void(const DMEvent&)>;
    SubId Subscribe(Handler h);
    void Unsubscribe(SubId id);

    void Init();

private:
    // ===================== 线程 / 事件循环 =====================
    void run() override; // BaseManager 线程主函数
    void handleFunctionCall(std::shared_ptr<Event> evt);

    // ===================== downloader 通知（HTTP / P2P） =====================
    static void coreNotifyCallbackHttp(std::shared_ptr<util::DownloaderTask> task, void* receiver);
    static void coreNotifyCallbackP2p(std::shared_ptr<util::DownloaderTask> task, void* receiver);

    // —— 对 downloader 的"一次事件"做读取, 转成领域事件交给 FSM
    void processHttpEvent(std::shared_ptr<util::DownloaderTask> ev);
    void processP2pEvent(std::shared_ptr<util::DownloaderTask> ev);

private:
    // ===================== FSM：命令 / IO / 计划 / 看门狗 -> 状态转移 =====================
    struct IOData
    {
        TaskId id; // parent task id
        size_t absOffset = 0; // 数据的绝对偏移
        std::shared_ptr<util::DownloaderTaskBuffer> data; // 数据缓冲区
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

        std::string peerId; // 如果是 P2P 填 peerId
        size_t contentLen = 0; // probe 可选字段：返回的 content-length
    };
    struct TaskFailed
    {
        int errorCode; // 错误码
        std::string message; // 错误信息
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

    // the only place that changes task status
    void transition_(TaskId id, TaskStatus to); // need hold std::lock_guard<std::mutex> lk(mTasksMutex);
    void applyFsm_(const IOData& ev);
    void applyFsm_(const IOEnd& ev);
    void applyFsm_(TaskId id, const TaskFailed& ev);
    void applyFsm_(TaskId id, const FinalizeCheck&);
    void applyFsm_(TaskId, const PlanReady&);
    void applyFsm_(TaskId, const StallFound&);
    void applyFsm_(TaskId, const CmdPause&);
    void applyFsm_(TaskId, const CmdResume&);
    void applyFsm_(TaskId, const CmdCancel&);
    void applyFsm_(TaskId, const CmdAdd&);

private:
    // ===================== 调度/副作用（由 FSM 调用） =====================
    // —— HTTP
    void ensurePreallocate_(TaskId id, size_t totalSize);
    void splitTask(TaskId id, size_t probedDataLen, size_t totalSize, size_t baseOffset); // HTTP-only 分片
    void scheduleHttpProbe_(TaskId id, const std::string& url);
    void scheduleHttpChunk_(TaskId id, const std::string& url, size_t start, size_t end);

    // —— P2P
    bool p2pQueryPeersAsync_(TaskId id, size_t start, size_t end);
    void onP2PPeerQuerySuccess_(TaskId id, nlohmann::json& res, size_t start, size_t end);
    void onP2PPeerQueryFail_(TaskId id, int errCode, size_t start);
    void scheduleP2PChunks_(TaskId id, const std::vector<PeerChunk>& plan, size_t start, size_t end);
    bool startOneP2PChunk_(TaskId id, const PeerChunk& pc);

    // —— 通用
    // bool maybeFinalizeTask(TaskId id); // 幂等完成判定
    void cancelAllSubs_(TaskId id, bool http = true, bool p2p = true);
    void updateTaskProgress(TaskId id, size_t downloaded);
    void calculateSpeedLocked(DownloadTask& t, std::chrono::system_clock::time_point now);
    void notifyBufferReady(const TaskId& taskId, size_t start, size_t end);
private:
    bool canFinalize_(TaskId id);
private:
    // ===================== CoreContext（只在 loop 线程写，多处读需加锁） =====================
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

    // —— HTTP
    // taskid --> util::DownloaderTask --> ActiveSubTask
    std::unordered_map<TaskId, std::vector<std::shared_ptr<util::DownloaderTask>>> mDlByTaskHttp;
    std::unordered_map<util::DownloaderTask*, ActiveSubTask> mActiveByPtrHttp;
    std::unordered_map<util::DownloaderTask*, std::shared_ptr<util::DownloaderTask>> mDlEventsHttp;

    // —— P2P
    std::unordered_map<util::DownloaderTask*, std::shared_ptr<util::DownloaderTask>> mDlEventsP2p;
    std::unordered_map<TaskId, std::vector<std::shared_ptr<util::DownloaderTask>>> mDlByTaskP2p;
    std::unordered_map<util::DownloaderTask*, ActiveSubTask> mActiveByPtrP2p;

    // 待调度队列与文件句柄
    std::unordered_map<TaskId, std::deque<Range>> mPendingRanges;
    std::unordered_map<TaskId, size_t> mNextRangeIdx;
    std::unordered_map<TaskId, std::shared_ptr<std::fstream>> mParentFiles;

    // cancel mark (using to drop late callbacks)
    std::unordered_set<util::DownloaderTask*> mCancelledRawHttp;
    std::unordered_set<util::DownloaderTask*> mCancelledRawP2p;

    // P2P peers 查询管理（按任务 + offset 记录状态）
    struct P2PTaskState
    {
        void* queryReqId = nullptr; // 外部请求句柄(HTTP API 的异步 id）
        bool queryInFlight = false;
        bool queryDone = false;
        int lastQueryErr = 0;
        size_t offset = 0;

        std::vector<PeerChunk> plan; // 解析后的 peers 计划
    };
    // taskID -> (offset -> state)
    std::unordered_map<TaskId, std::unordered_map<size_t, P2PTaskState>> p2pStates_;

private:
    // ===================== 任务/配置/下载器句柄 + 依赖 =====================
    DownloadStrategy mStrategy = DownloadStrategy::HTTP_ONLY;
    size_t mMaxConcurrent = 4;
    std::string mPersistPath;

    // 注入的外部依赖
    Dependencies mDeps;

    // 兼容旧实现：仍然持有（默认从 mDeps 填充）
    std::shared_ptr<download::P2PDownloader> mP2pDownloader;
    std::shared_ptr<util::HttpDownloader> mHttpDownloader;

    // 任务 & 选项 & 回调
    mutable std::mutex mTasksMutex;
    std::unordered_map<TaskId, DownloadTask> mTasks;
    std::unordered_map<TaskId, FileDownloadOptions> mTaskOptions;
    std::unordered_map<TaskId, BufferReadyCallback> mBufferCallbacks;

    // 内部轻量订阅（可与 IEventBus 并存）
    mutable std::mutex mSubMutex;
    SubId mNextSubId{1};
    std::unordered_map<SubId, Handler> mSubscribers;

    // HTTP/P2P 带宽占比（暂未使用，预留）
    float mHttpBandwidthRatio = 0.5f;
    float mP2pBandwidthRatio = 0.5f;

    std::atomic<TaskId> mLastCreatedTaskId{0};

#ifdef DCDN_DM_TESTING
    friend class dcdn::TestHook;
#endif
private:
    // ===================== Watchdog  =====================
    void watchdogHandleMayStalledSubtask(
        const ActiveSubTask& st,
        std::chrono::steady_clock::time_point now,
        const std::chrono::seconds& stallTimeout);
    void handleHttpSubtaskStall(const ActiveSubTask& st, std::chrono::steady_clock::time_point now);
    void handleP2pSubtaskStall(const ActiveSubTask& st, std::chrono::steady_clock::time_point now);

private:
    // ===================== Persistence（可选，占位） =====================
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
        void* mDb = nullptr; // 如要用 sqlite3，可在 cpp 里替换
        std::mutex mDbMutex;
    };

    std::unique_ptr<PersistenceHelper> mDbHelper;

    // ====== 统一对外事件发布（内部订阅 + 外置总线）=====
    void publish_(const DMEvent& ev);
};

} // namespace dcdn