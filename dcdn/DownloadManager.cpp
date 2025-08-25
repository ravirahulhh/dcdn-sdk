#include "DownloadManager.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cstdio> // std::remove
#include <deque>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "MainManager.h"
#include "P2PDownloader.h"
using namespace dcdn;
using nlohmann::json;

namespace {
static constexpr const char* kApiPeersByHashOrUrl = "/api/v1/query_peers_by_file";

static TaskId genTaskId()
{
    static std::atomic<uint64_t> cnt{0};
    static const uint64_t base = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    return base + cnt++;
}

struct DrainResult
{
    size_t bytesWritten = 0;
    size_t firstWriteAbsBegin = SIZE_MAX; // 仅用于回调范围合并
    size_t lastWriteAbsEnd = 0;
};

// 统一写入工具：将 [bufBeg,bufEnd] 与 [segBeg,segEnd] 求交集，裁剪后写入。
// - 写入优先级：OutputStream -> OutputPath(随机写 fstream) -> IFileStore(若注入)；
//   如果注入了 IFileStore，优先使用 IFileStore（便于测试与替换）。
static void write_bytes_clipped(
    const uint8_t* data,
    size_t bufLen,
    size_t bufOffsetAbs, // buffer 描述
    size_t clipBeg,
    size_t clipEnd, // 分片负责区间（闭区间）
    const FileDownloadOptions& opts,
    std::shared_ptr<std::fstream> file,
    TaskId taskId,
    DownloadManager::Dependencies deps,
    DrainResult& agg // 汇总
)
{
    if (!data || bufLen == 0)
        return;

    const size_t bufBeg = bufOffsetAbs;
    const size_t bufEnd = bufOffsetAbs + (bufLen - 1);

    if (clipBeg > clipEnd || bufBeg > bufEnd)
        return;

    const size_t wrBeg = (bufBeg > clipBeg) ? bufBeg : clipBeg;
    const size_t wrEnd = (bufEnd < clipEnd) ? bufEnd : clipEnd;
    if (wrBeg > wrEnd)
        return;

    const size_t accept = wrEnd - wrBeg + 1;
    const size_t srcOff = wrBeg - bufBeg;
    if (bufBeg < clipBeg) {
        logWarn << "[write_bytes_clipped] bufBeg(" << bufBeg << ") < clipBeg(" << clipBeg << ")";
    } else if (bufEnd > clipEnd) {
        logWarn << "[write_bytes_clipped] bufEnd(" << bufEnd << ") > clipEnd(";
    }
    // 1) 如果是“按 Range 写到独立文件”，则把 wrBeg 折算成相对 RangeStart 的偏移
    // 2) 否则按资源绝对偏移 wrBeg 写入
    size_t dstOff = wrBeg;
    if (opts.HasRange && opts.WriteRangeToSeparateFile) {
        // 防御：避免 RangeStart > wrBeg 导致 size_t underflow
        if (wrBeg >= opts.RangeStart) {
            dstOff = wrBeg - opts.RangeStart;
        } else {
            // 异常：日志并回退为 0
            logWarn << "[write_bytes_clipped] wrBeg(" << wrBeg << ") < RangeStart(" << opts.RangeStart
                    << "), fallback dstOff=0";
            dstOff = 0;
        }
    }

    bool wrote = false;

    // IFileStore first
    if (deps.files) {
        wrote = deps.files->Write(taskId, dstOff, data + srcOff, accept);
        if (!wrote) {
            logWarn << "[write_bytes_clipped] IFileStore::Write failed, taskId=" << taskId << " dstOff=" << dstOff
                    << " accept=" << accept;
        }
    }

    // —— 没有 IFileStore 才走 OutputStream / 本地文件
    if (!wrote) {
        if (opts.OutputStream) {
            try {
                // 这里假定 OutputStream 只用于串流（顺序写）。如果存在乱序写，建议不要使用 OutputStream。
                // 若要严格保证位置，可考虑 dynamic_cast 到 std::ostream* 是否可 seekp，
                // 但通常 OutputStream 代表“顺序消耗”，这里不 seek。
                opts.OutputStream->write(reinterpret_cast<const char*>(data + srcOff), accept);
                wrote = true;
            } catch (...) {
                wrote = false;
                logWarn << "[write_bytes_clipped] OutputStream write threw exception";
            }
        } else if (!opts.OutputPath.empty()) {
            // 优先使用已缓存的父文件句柄
            if (file && file->good()) {
                try {
                    file->seekp(static_cast<std::streamoff>(dstOff), std::ios::beg);
                    // logDebug << "#seekp write[" << dstOff << "," << dstOff + accept - 1 << "]" << "srcOff " <<
                    // srcOff;
                    file->write(reinterpret_cast<const char*>(data + srcOff), accept);
                    wrote = true;
                } catch (...) {
                    wrote = false;
                    logWarn << "[write_bytes_clipped] fstream seek/write exception, dstOff=" << dstOff
                            << " accept=" << accept;
                }
            } else {
                // 兜底：临时打开
                std::fstream ofs(opts.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
                if (!ofs) {
                    // 文件不存在则创建
                    std::ofstream create(opts.OutputPath, std::ios::binary);
                    create.close();
                    ofs.open(opts.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
                }
                if (ofs) {
                    ofs.seekp(static_cast<std::streamoff>(dstOff), std::ios::beg);
                    ofs.write(reinterpret_cast<const char*>(data + srcOff), accept);
                    wrote = true;
                } else {
                    logWarn << "[write_bytes_clipped] open fallback file failed: " << opts.OutputPath;
                }
            }
        }
    }

    if (wrote) {
        agg.bytesWritten += accept;
        if (agg.firstWriteAbsBegin == SIZE_MAX)
            agg.firstWriteAbsBegin = wrBeg;
        agg.lastWriteAbsEnd = wrEnd;
    }
    // logDebug << "agg bytesWritten=" << accept << " accumulation =" << agg.bytesWritten;
}

} // namespace

std::string DownloadTask::Serialize() const
{
    std::ostringstream ss;
    ss << Id << "|" << Url << "|" << ContentHash << "|" << TotalSize << "|" << Downloaded << "|"
       << static_cast<int>(Status);
    return ss.str();
}

DownloadTask DownloadTask::Deserialize(const std::string& data)
{
    DownloadTask task;
    std::istringstream ss(data);
    std::string token;
    if (!std::getline(ss, token, '|'))
        return task;
    try {
        task.Id = static_cast<uint64_t>(std::stoull(token));
    } catch (...) {
        task.Id = 0;
    }
    std::getline(ss, task.Url, '|');
    std::getline(ss, task.ContentHash, '|');
    if (!std::getline(ss, token, '|'))
        return task;
    task.TotalSize = static_cast<size_t>(std::stoull(token));
    if (!std::getline(ss, token, '|'))
        return task;
    task.Downloaded = static_cast<size_t>(std::stoull(token));
    if (!std::getline(ss, token, '|'))
        return task;
    task.Status = static_cast<TaskStatus>(std::stoi(token));
    return task;
}

// ========== PersistenceHelper（sqlite 桩实现，便于后续替换） ==========
DownloadManager::PersistenceHelper::PersistenceHelper(const std::string& dbPath)
{
    (void)dbPath;
    mDb = nullptr;
}
DownloadManager::PersistenceHelper::~PersistenceHelper() {}

bool DownloadManager::PersistenceHelper::saveTask(const DownloadTask& task)
{
    (void)task;
    return true;
}
bool DownloadManager::PersistenceHelper::loadTasks(std::vector<DownloadTask>& tasks)
{
    (void)tasks;
    return true;
}
bool DownloadManager::PersistenceHelper::deleteTask(TaskId taskId)
{
    (void)taskId;
    return true;
}

bool DownloadManager::PersistenceHelper::saveSubTasks(TaskId taskId, const std::vector<SubTaskRec>& subtasks)
{
    (void)taskId;
    (void)subtasks;
    return true;
}

bool DownloadManager::PersistenceHelper::loadSubTasks(TaskId taskId, std::vector<SubTaskRec>& subtasks)
{
    (void)taskId;
    (void)subtasks;
    return true;
}

DownloadManager::DownloadManager(
    std::shared_ptr<util::HttpDownloader> http,
    std::shared_ptr<download::P2PDownloader> p2p)
    : BaseManager(nullptr)
{
    mDeps.http = std::move(http);
    mDeps.p2p = std::move(p2p);

    this->registerHandler(EventType::FunctionCall, &DownloadManager::handleFunctionCall);
}

DownloadManager::DownloadManager(const Dependencies& injected): BaseManager(nullptr), mDeps(injected)
{
    this->registerHandler(EventType::FunctionCall, &DownloadManager::handleFunctionCall);
}

DownloadManager::~DownloadManager()
{
    // resource cleanup is handled by smart pointers automatically;
}

void DownloadManager::Init() {}

void DownloadManager::watchdogHandleMayStalledSubtask(
    const ActiveSubTask& st,
    std::chrono::steady_clock::time_point now,
    const std::chrono::seconds& staleTimeout)
{
    bool paused = false, cancelled = false;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(st.parentTaskId);
        if (it != mTasks.end()) {
            paused = it->second.Paused.load();
            cancelled = it->second.Cancelled.load();
        }
    }

    if (paused || cancelled)
        return;

    if (now - st.lastTouched > staleTimeout) {
        const size_t got = static_cast<size_t>(st.actualGot);
        if (got >= st.length)
            return;

        const size_t missStart = st.offset + got;
        const size_t missEnd = st.offset + st.length - 1;

        // cancel stalled subtask and pending miss
        {
            AnnotatedMutex::Guard lk(mEventMutex);
            auto raw = st.downloader.get();
            mActiveByPtrHttp.erase(raw);
            auto& vec = mDlByTaskHttp[st.parentTaskId];
            vec.erase(
                std::remove_if(
                    vec.begin(),
                    vec.end(),
                    [raw](const std::shared_ptr<util::DownloaderTask>& p) { return p.get() == raw; }),
                vec.end());
            mPendingRanges[st.parentTaskId].push_front(Range{missStart, missEnd});
            mCancelledRawHttp.insert(raw);
            if (mDeps.http)
                mDeps.http->CancelTask(st.downloader);
            logWarn << "[watchdog] cancel stalled subtask idx=" << st.index << " miss=[" << missStart << "," << missEnd
                    << "]";
        }

        // 立即续排一个(TODO: 应该考虑并发数受限？)
        Range r;
        bool hasNext = false;
        {
            AnnotatedMutex::Guard lk(mEventMutex);

            auto& q = mPendingRanges[st.parentTaskId];
            if (!q.empty()) {
                r = q.front();
                q.pop_front();
                hasNext = true;
            }
        }
        if (hasNext) {
            std::string urlLocal;
            {
                AnnotatedMutex::Guard lk(mTasksMutex);
                auto itT = mTasks.find(st.parentTaskId);
                if (itT != mTasks.end())
                    urlLocal = itT->second.Url;
            }
            if (st.transport == ActiveSubTask::Transport::P2P) {
                handleP2pSubtaskStall(st, now);
            } else if (st.transport == ActiveSubTask::Transport::HTTP) {
                scheduleHttpChunk_(st.parentTaskId, urlLocal, r.start, r.end);
            }
        }
    }
}
void DownloadManager::handleHttpSubtaskStall(const ActiveSubTask& st, std::chrono::steady_clock::time_point now)
{
    void(st.parentTaskId);
}

void DownloadManager::handleP2pSubtaskStall(const ActiveSubTask& st, std::chrono::steady_clock::time_point now)
{
    // TODO:
    logWarn << "[watchdog] p2p subtask stall NOT IMPLEMENTED!!!!!!!!!!!" << std::endl;
    return;
}

// ========== BaseManager 线程主函数：事件主循环（poll、分发、驱动 FSM） ==========
void DownloadManager::run()
{
    DmLoopThreadCap::Guard loop_guard(loop_cap_);
    using namespace std::chrono_literals;

    auto lastReap = std::chrono::steady_clock::now();
    const auto reapInterval = 1s; // 看门狗巡检周期
    const auto staleTimeout = 8s; // N 秒无进展视为卡死

    while (true) {
        {
            std::unique_lock<std::mutex> lk(mEventMutex.native_handle());
            mCv.wait_for(lk, reapInterval, [&]() REQUIRES(mEventMutex) {
                return !mDlEventsHttp.empty() || !mDlEventsP2p.empty() || !mEvents.empty();
            });
        }

        // execute control cmd
        if (!mEvents.empty()) {
            this->waitEvent(0ms);
        }

        // downloader read ready events
        std::vector<std::shared_ptr<util::DownloaderTask>> httpBatch;
        std::vector<std::shared_ptr<util::DownloaderTask>> p2pBatch;
        {
            AnnotatedMutex::Guard g(mEventMutex);
            httpBatch.reserve(mDlEventsHttp.size());
            for (auto& kv : mDlEventsHttp)
                httpBatch.push_back(kv.second);
            mDlEventsHttp.clear();

            p2pBatch.reserve(mDlEventsP2p.size());
            for (auto& kv : mDlEventsP2p)
                p2pBatch.push_back(kv.second);
            mDlEventsP2p.clear();
        }

        for (auto& t : httpBatch) {
            try {
                processHttpEvent(t);
            } catch (...) {
            }
        }
        for (auto& t : p2pBatch) {
            try {
                processP2pEvent(t);
            } catch (...) {
            }
        }

        auto now = std::chrono::steady_clock::now();
        if (now - lastReap >= reapInterval) {
            lastReap = now;

            std::vector<ActiveSubTask> snapshot;
            {
                AnnotatedMutex::Guard lk(mEventMutex);
                snapshot.reserve(mActiveByPtrHttp.size());
                for (auto& kv : mActiveByPtrHttp)
                    snapshot.push_back(kv.second);
            }

            for (auto& st : snapshot) {
                if (st.isProbe || st.length == 0)
                    continue;
                watchdogHandleMayStalledSubtask(st, now, staleTimeout);
            }

            // 巡检后尝试 finalize
            {
                std::vector<TaskId> toCheck;
                {
                    AnnotatedMutex::Guard lk(mEventMutex);
                    toCheck.reserve(mDlByTaskHttp.size() + mDlByTaskP2p.size());
                    for (auto& kv : mDlByTaskHttp)
                        toCheck.push_back(kv.first);
                    for (auto& kv : mDlByTaskP2p)
                        toCheck.push_back(kv.first);
                }
                for (auto id : toCheck) {
                    applyFsm_(id, FinalizeCheck{id});
                }
            }
        }
    }
}

void DownloadManager::handleFunctionCall(std::shared_ptr<Event> evt)
{
    auto* a = dynamic_cast<ArgEvent<std::function<void()>>*>(evt.get());
    if (!a)
        return;
    auto fn = a->Arg();
    if (fn)
        fn();
}

void DownloadManager::coreNotifyCallbackHttp(std::shared_ptr<util::DownloaderTask> task, void* receiver)
{
    if (!task || !receiver)
        return;
    auto* self = static_cast<DownloadManager*>(receiver);
    {
        AnnotatedMutex::Guard g(self->mEventMutex);
        if (self->mCancelledRawHttp.count(task.get()))
            return;
        self->mDlEventsHttp[task.get()] = std::move(task);
    }
    self->mCv.notify_all();
}

void DownloadManager::coreNotifyCallbackP2p(std::shared_ptr<util::DownloaderTask> task, void* receiver)
{
    if (!task || !receiver)
        return;
    auto* self = static_cast<DownloadManager*>(receiver);
    {
        AnnotatedMutex::Guard g(self->mEventMutex);
        if (self->mCancelledRawP2p.count(task.get()))
            return;
        self->mDlEventsP2p[task.get()] = std::move(task);
    }
    self->mCv.notify_all();
}

// ========== 将 downloader 事件转为抽象事件并进入 FSM（HTTP）==========
void DownloadManager::processHttpEvent(std::shared_ptr<util::DownloaderTask> ev)
{
    auto* raw = ev.get();

    // cancelled task event, discard
    {
        AnnotatedMutex::Guard g(mEventMutex);
        if (mCancelledRawHttp.count(raw))
            return;
    }

    ActiveSubTask active;
    auto it = mActiveByPtrHttp.find(raw);
    if (it == mActiveByPtrHttp.end()) {
        return; // late event
    }
    active = it->second;

    const bool isEnd = ev->IsEnd();
    auto buffer = ev->Read();
    if (!buffer)
        return;

    // 计算本次事件对应的分片范围：
    // - 常规：使用 active.offset/length
    // - probe：这次读取了多少就以多少作为"临时范围"，让 FSM 去写与计算
    size_t segStart = active.offset;
    size_t segEnd = active.offset + (active.length ? (active.length - 1) : 0);

    size_t probeDataLen = 0;
    size_t probedContentLen = 0;
    if (active.isProbe) {
        auto tmp = buffer;
        while (tmp) {
            probeDataLen += tmp->Length();
            tmp = tmp->Next();
        }
        // probe 任务没有固定 length，用"这次读到的数据长度"作为临时范围
        segStart = active.offset;
        ;
        segEnd = active.offset + ((probeDataLen > 0) ? (probeDataLen - 1) : 0);

        if (auto ht = dynamic_cast<util::HttpDownloaderTask*>(ev.get())) {
            probedContentLen = ht->ContentLength();
        }
        logDebug << "probeDataLen: " << probeDataLen << ", probedContentLen: " << probedContentLen;
    }
    // enter FSM: data
    IOData dataEv{};
    dataEv.id = active.parentTaskId;
    dataEv.absOffset = segStart; // 这里作为"本次写入起点"的提示值；真正写入用 buffer->Offset 裁剪
    dataEv.data = buffer;
    dataEv.segStart = segStart;
    dataEv.segEnd = segEnd;
    dataEv.isProbe = active.isProbe;
    dataEv.probeLenSnapshot = probeDataLen;
    dataEv.contentLenHint = probedContentLen;
    applyFsm_(dataEv);
    uint64_t got = 0;
    {
        AnnotatedMutex::Guard lk(mEventMutex);
        auto it = mActiveByPtrHttp.find(raw);
        if (it != mActiveByPtrHttp.end()) {
            it->second.lastTouched = std::chrono::steady_clock::now();
            got = it->second.actualGot; // 使用前面 bump 后的真实写入量
        }
    }
    // logInfo << "##########" << "data buf [ "<< dataEv.data->Offset() << ","  << dataEv.data->Offset() + got - 1<<
    // " ] << seg start end is[" << segStart << ", " << segEnd << "]";

    // 结束事件（下载器声明结束，或普通分片按长度读满）
    const bool endByLength = (!active.isProbe && active.length > 0 && got >= active.length);
    if (isEnd || endByLength) {
        size_t contentLenIfProbe = 0;
        if (active.isProbe) {
            if (auto ht = dynamic_cast<util::HttpDownloaderTask*>(raw)) {
                contentLenIfProbe = ht->ContentLength();
            }
        }
        IOEnd endEv{};
        endEv.id = active.parentTaskId;
        endEv.ok = true;
        endEv.actuallyGot = static_cast<size_t>(got);
        endEv.segStart = segStart;
        endEv.segEnd = segEnd;
        endEv.isProbe = active.isProbe;
        endEv.contentLen = contentLenIfProbe;
        applyFsm_(endEv);
    }
}

// ========== 将 downloader 事件转为抽象事件并进入 FSM（P2P）==========
void DownloadManager::processP2pEvent(std::shared_ptr<util::DownloaderTask> ev)
{
    auto* raw = ev.get();
    {
        AnnotatedMutex::Guard lk(mEventMutex);
        if (mCancelledRawP2p.count(raw)) {
            logInfo << "p2p event cancelled";
            return;
        }
    }

    ActiveSubTask active;
    auto it = mActiveByPtrP2p.find(raw);
    if (it == mActiveByPtrP2p.end()) {
        return;
    }
    active = it->second;

    const bool isEnd = ev->IsEnd();
    auto buffer = ev->Read();
    if (!buffer)
        return;

    // 统计链上总长度（给 end 用），P2P 的 seg 区间来自 ActiveSubTask
    size_t totalLen = 0;
    {
        auto tmp = buffer;
        while (tmp) {
            totalLen += tmp->Length();
            tmp = tmp->Next();
        }
    }

    const size_t segBeg = active.offset;
    const size_t segEnd = active.offset + (active.length ? (active.length - 1) : 0);

    IOData dataEv{};
    dataEv.id = active.parentTaskId;
    dataEv.absOffset = segBeg;
    dataEv.data = buffer;
    dataEv.segStart = segBeg;
    dataEv.segEnd = segEnd;
    dataEv.isProbe = false;
    dataEv.peerId = ""; // 如需，可在 ActiveSubTask 增加 peerId 再填
    applyFsm_(dataEv);

    if (isEnd) {
        IOEnd endEv{};
        endEv.id = active.parentTaskId;
        endEv.ok = true;
        endEv.actuallyGot = totalLen;
        endEv.segStart = segBeg;
        endEv.segEnd = segEnd;
        endEv.isProbe = false;
        applyFsm_(endEv);
    }
}

void DownloadManager::transitionLocked_(TaskId id, TaskStatus to)
{
    auto it = mTasks.find(id);
    if (it == mTasks.end())
        return;
    const auto from = it->second.Status;
    if (from == to)
        return;
    it->second.Status = to;
    publish_(ETaskStatusChanged{id, from, to});
}

bool DownloadManager::canFinalize_(TaskId id)
{
    bool hasRunningHttp = false, hasRunningP2p = false, hasPending = false, hasP2pQueryInflight = false;

    auto itH = mDlByTaskHttp.find(id);
    hasRunningHttp = (itH != mDlByTaskHttp.end() && !itH->second.empty());
    auto itP = mDlByTaskP2p.find(id);
    hasRunningP2p = (itP != mDlByTaskP2p.end() && !itP->second.empty());
    auto itQ = mPendingRanges.find(id);
    hasPending = (itQ != mPendingRanges.end() && !itQ->second.empty());

    auto it = p2pStates_.find(id);
    if (it != p2pStates_.end()) {
        hasP2pQueryInflight =
            std::any_of(it->second.begin(), it->second.end(), [](const auto& kv) { return kv.second.queryInFlight; });
    }
    return !(hasRunningHttp || hasRunningP2p || hasPending || hasP2pQueryInflight);
}

void DownloadManager::applyFsm_(TaskId id, const FinalizeCheck&)
{
    if (!canFinalize_(id))
        return;
    logDebug << "###############Finalize task applyFsm " << id;
    // transit task status
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto itT = mTasks.find(id);
        if (itT == mTasks.end())
            return;
        auto& t = itT->second;
        if (t.Cancelled) {
            transitionLocked_(id, TaskStatus::Cancelled);
        } else {
            if (t.TotalSize > 0 && t.Downloaded < t.TotalSize) {
                logWarn << "[🐛]Task Downloaded Force Aligned," << id
                        << "unexpected downloaded less than expected: " << t.Downloaded << " < " << t.TotalSize;
                t.Downloaded = t.TotalSize;
            }
            transitionLocked_(id, TaskStatus::Completed);
        }
    }

    // clean up run-time resources
    mDlByTaskHttp.erase(id);
    mDlByTaskP2p.erase(id);
    mPendingRanges.erase(id);
    mParentFiles.erase(id);
}

void DownloadManager::applyFsm_(const IOData& ev)
{
    // ========== A) 若是 probe 且已经拿到 Content-Length，则立刻切分并取消 probe ==========
    if (ev.isProbe && ev.contentLenHint > 0) {
        FileDownloadOptions opts;
        bool needSplit = false;
        size_t totalLen = 0;
        size_t baseOffset = 0;

        {
            AnnotatedMutex::Guard lk(mTasksMutex);
            auto it = mTasks.find(ev.id);
            if (it != mTasks.end() && it->second.TotalSize == 0) {
                auto itOpt = mTaskOptions.find(ev.id);
                if (itOpt != mTaskOptions.end())
                    opts = itOpt->second;

                const size_t clen = ev.contentLenHint;
                if (opts.HasRange) {
                    baseOffset = opts.RangeStart;
                    if (opts.RangeEnd != SIZE_MAX) {
                        totalLen = (opts.RangeEnd >= opts.RangeStart) ? (opts.RangeEnd - opts.RangeStart + 1) : 0;
                    } else {
                        totalLen = (baseOffset >= clen) ? 0 : (clen - baseOffset);
                    }
                } else {
                    baseOffset = 0;
                    totalLen = clen;
                }

                if (totalLen > 0) {
                    it->second.TotalSize = totalLen;
                    it->second.Status = TaskStatus::Running;
                    needSplit = true;
                } else {
                    it->second.Status = TaskStatus::Failed;
                }
            }
        }

        if (needSplit) {
            // NOTE: the internal splitTask will locate and cancel running probe subtasks
            splitTask(ev.id, ev.probeLenSnapshot, totalLen, baseOffset);
            // no return here to keep probe data
        }
    }

    // ========== B) 通用：将 buffer 链按 [segStart, segEnd] clip 并写入 ==========
    FileDownloadOptions opts;
    std::shared_ptr<std::fstream> file;
    {
        AnnotatedMutex::Guard l(mTasksMutex);
        auto itOpt = mTaskOptions.find(ev.id);
        if (itOpt != mTaskOptions.end())
            opts = itOpt->second;
    }

    auto itF = mParentFiles.find(ev.id);
    if (itF != mParentFiles.end())
        file = itF->second;

    DrainResult agg{};
    auto b = ev.data;
    while (b) {
        const size_t len = b->Length();
        const size_t off = b->Offset(); // 资源绝对偏移
        // logDebug << "[P2P][Write] task id=" << ev.id << " range " << off << "-" << (off + len - 1) << std::endl;
        write_bytes_clipped(
            reinterpret_cast<const uint8_t*>(b->Data()),
            len,
            off,
            ev.segStart,
            ev.segEnd,
            opts,
            file,
            ev.id,
            mDeps,
            agg);
        b = b->Next();
    }

    // 进度 + buffer-ready
    if (agg.bytesWritten > 0) {
        updateTaskProgress(ev.id, agg.bytesWritten);
        const size_t readyBeg = (agg.firstWriteAbsBegin == SIZE_MAX) ? ev.segStart : agg.firstWriteAbsBegin;
        const size_t readyEnd = agg.lastWriteAbsEnd;
        if (readyBeg <= readyEnd)
            notifyBufferReady(ev.id, readyBeg, readyEnd);
    }

    // 心跳/actualGot（尽量定位对应 subtask）
    // TODO: 优化
    auto bump = [&](auto& activeByPtrMap, auto& dlByTaskMap) {
        // taskID -> util::DownloadTask -> ActiveSubTask
        auto itTask = dlByTaskMap.find(ev.id);
        if (itTask == dlByTaskMap.end())
            return false;
        for (auto& dlTask : itTask->second) {
            auto raw = dlTask.get();
            auto itActiveSub = activeByPtrMap.find(raw);
            if (itActiveSub == activeByPtrMap.end())
                continue;
            auto& subT = itActiveSub->second;
            const size_t stEnd = subT.length ? (subT.offset + subT.length - 1) : subT.offset;
            const bool matched = subT.isProbe ? ev.isProbe : (subT.offset == ev.segStart && stEnd == ev.segEnd);
            if (matched) {
                subT.actualGot += agg.bytesWritten;
                subT.lastTouched = std::chrono::steady_clock::now();
                return true;
            }
        }
        return false;
    };
    if (agg.bytesWritten > 0) {
        if (!bump(mActiveByPtrHttp, mDlByTaskHttp))
            bump(mActiveByPtrP2p, mDlByTaskP2p);
    }

    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(ev.id);
        if (it != mTasks.end()) {
            publish_(ETaskProgress{ev.id, it->second.Downloaded, it->second.TotalSize});
        }
    }
    applyFsm_(ev.id, FinalizeCheck{ev.id});
}

void DownloadManager::applyFsm_(const IOEnd& ev)
{
    logInfo << "任务 " << ev.id << " IOEnd" << ev.segStart << "-" << ev.segEnd << " actual got " << ev.actuallyGot;
    if (ev.segEnd - ev.segStart + 1 > ev.actuallyGot) {
        logWarn << "IOEnd 任务得到比预期少的数据: less" << ev.segEnd - ev.segStart + 1 - ev.actuallyGot << "Bytes "
                << ev.id << " IOEnd" << ev.segStart << "-" << ev.segEnd << " actual got " << ev.actuallyGot;
    } else if (ev.segEnd - ev.segStart + 1 < ev.actuallyGot) {
        logWarn << "IOEnd 任务得到比预多的数据: more" << ev.actuallyGot - (ev.segEnd - ev.segStart + 1) << "Bytes "
                << ev.id << " IOEnd" << ev.segStart << "-" << ev.segEnd << " actual got " << ev.actuallyGot;
    }
    // 1) 将对应 subtask 从活跃集合中移除
    auto findAndRemoveByRange = [&](auto& activeByPtrMap, auto& dlByTaskMap) -> bool {
        // taskid --> util::DownloadTask -- > ActiveSubTask
        auto itV = dlByTaskMap.find(ev.id);
        if (itV == dlByTaskMap.end())
            return false;
        for (auto it = itV->second.begin(); it != itV->second.end();) {
            auto raw = it->get();
            auto itA = activeByPtrMap.find(raw);
            if (itA == activeByPtrMap.end()) {
                ++it;
                continue;
            }
            const auto& st = itA->second;
            const size_t stEnd = st.length ? (st.offset + st.length - 1) : st.offset;
            const bool matched = st.isProbe ? ev.isProbe : (st.offset == ev.segStart && stEnd == ev.segEnd);
            if (matched) {
                activeByPtrMap.erase(raw);
                it = itV->second.erase(it);
                return true;
            } else {
                ++it;
            }
        }
        return false;
    };

    if (!findAndRemoveByRange(mActiveByPtrHttp, mDlByTaskHttp)) {
        findAndRemoveByRange(mActiveByPtrP2p, mDlByTaskP2p);
    }

    if (ev.isProbe) {
        return;
    }

    // 3) 非 probe：处理短读（回填到 pending）
    const size_t expected = (ev.segEnd >= ev.segStart) ? (ev.segEnd - ev.segStart + 1) : 0;
    if (ev.ok && ev.actuallyGot < expected) {
        const size_t missStart = ev.segStart + ev.actuallyGot;
        const size_t missEnd = ev.segEnd;
        AnnotatedMutex::Guard lk(mEventMutex);
        mPendingRanges[ev.id].push_front(Range{missStart, missEnd});
        logWarn << "missed range: [" << missStart << ", " << missEnd << ")" << " taskId=" << ev.id
                << "actual got=" << ev.actuallyGot;
    }

    // 4) 按策略续排一个 pending
    bool paused = false, cancelled = false;
    DownloadStrategy strategy = DownloadStrategy::HTTP_ONLY;
    std::string urlLocal;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(ev.id);
        if (it != mTasks.end()) {
            paused = it->second.Paused.load();
            cancelled = it->second.Cancelled.load();
            urlLocal = it->second.Url;
        }
        auto itOpt = mTaskOptions.find(ev.id);
        if (itOpt != mTaskOptions.end())
            strategy = itOpt->second.Strategy;
    }

    if (!paused && !cancelled) {
        Range r;
        bool hasNext = false;
        {
            AnnotatedMutex::Guard lk(mEventMutex);
            auto& q = mPendingRanges[ev.id];
            if (!q.empty()) {
                r = q.front();
                q.pop_front();
                hasNext = true;
            }
        }
        if (hasNext) {
            if (strategy == DownloadStrategy::HTTP_ONLY) {
                logInfo << "续排一个 HTTP 分片 taskId=" << ev.id << " range=" << r.start << "-" << r.end;
                scheduleHttpChunk_(ev.id, urlLocal, r.start, r.end);
            } else {
                // P2P_ONLY / HYBRID：优先 P2P；HYBRID 失败则回退 HTTP
                const bool ok = p2pQueryPeersAsync_(ev.id, r.start, r.end);
                if (!ok && strategy == DownloadStrategy::HYBRID) {
                    scheduleHttpChunk_(ev.id, urlLocal, r.start, r.end);
                }
            }
        }
    }

    applyFsm_(ev.id, FinalizeCheck{ev.id});
}

void DownloadManager::applyFsm_(TaskId id, const TaskFailed& ev)
{
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(id);
        if (it != mTasks.end()) {
            transitionLocked_(id, TaskStatus::Failed);
            it->second.Status = TaskStatus::Failed;
        }
    }

    mDlByTaskHttp.erase(id);
    p2pStates_.erase(id);
    mPendingRanges.erase(id);

    logError << "[fsm] Task " << id << " failed, code=" << ev.errorCode << " msg=" << ev.message;
    applyFsm_(id, FinalizeCheck{id});
}

// ========== FSM：计划就绪（P2P peers 查询结果）==========
void DownloadManager::applyFsm_(TaskId id, const PlanReady& ev)
{
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(id);
        if (it == mTasks.end()) {
            // task may be cancelled/deleted, nothing to do
        } else {
            // only use hint when TotalSize is not set
            if (it->second.TotalSize == 0 && ev.totalSizeHint > 0) {
                it->second.TotalSize = ev.totalSizeHint;
            }
            // Pending -> Running
            if (it->second.Status == TaskStatus::Pending) {
                transitionLocked_(id, TaskStatus::Running);
            }
            auto& st = p2pStates_[id][ev.startHint];
            st.queryInFlight = false;
            st.queryDone = true;
            st.lastQueryErr = 0;
            st.offset = ev.startHint;
            st.plan = ev.chunks;
        }
    }

    scheduleP2PChunks_(id, ev.chunks, ev.startHint, ev.endHint);
    applyFsm_(id, FinalizeCheck{id});
    // TODO: may publish planReadyEvent here
}

// ========== FSM：StallFound（看门狗通知）==========
void DownloadManager::applyFsm_(TaskId id, const StallFound& ev)
{
    mPendingRanges[id].push_front(Range{ev.segStart, ev.segEnd});
    // 交由 run() 循环里的 maybeFinalize / 正常续排逻辑去处理
}

void DownloadManager::applyFsm_(TaskId /*id*/, const CmdAdd& cmd)
{
    // 创建 Task + 预分配 + 启动
    TaskId id = genTaskId();
    DownloadTask task;
    task.Id = id;
    task.Url = cmd.url;
    task.ContentHash = cmd.hash;
    task.TotalSize = 0;
    task.Downloaded = 0;
    task.StartTime = std::chrono::system_clock::now();
    task.LastUpdate = task.StartTime;
    task.Status = TaskStatus::Pending; // the initial state initialization, not changed by transitionLocked_() is ok
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        mTasks[id] = task;
        mTaskOptions[id] = cmd.opt;
    }
    mLastCreatedTaskId.store(id, std::memory_order_release);

    // 打开父任务共享文件（随机写）或通过 IFileStore 预分配
    if (!cmd.opt.OutputPath.empty() && cmd.opt.ChunkSize > 0) {
        ensurePreallocate_(
            id, cmd.opt.HasRange && cmd.opt.RangeEnd != SIZE_MAX ? (cmd.opt.RangeEnd - cmd.opt.RangeStart + 1) : 0);
    }

    if (cmd.opt.Strategy == DownloadStrategy::HTTP_ONLY) {
        if (cmd.opt.HasRange && cmd.opt.RangeEnd != SIZE_MAX) {
            const size_t start = cmd.opt.RangeStart;
            const size_t len = (cmd.opt.RangeEnd >= start) ? (cmd.opt.RangeEnd - start + 1) : 0;
            {
                AnnotatedMutex::Guard lk(mTasksMutex);
                auto& t = mTasks[id];
                t.TotalSize = len;
                transitionLocked_(id, TaskStatus::Running);
            }
            splitTask(id, 0, len, start);
        } else {
            scheduleHttpProbe_(id, cmd.url);
            {
                AnnotatedMutex::Guard lk(mTasksMutex);
                auto& t = mTasks[id];
                t.Status = TaskStatus::Running;
                transitionLocked_(id, TaskStatus::Running);
            }
        }
    } else if (cmd.opt.Strategy == DownloadStrategy::P2P_ONLY) {
        // 若指定了范围，TotalSize 可先按范围算；否则等 peers 响应里带 size
        if (cmd.opt.HasRange && cmd.opt.RangeEnd != SIZE_MAX) {
            const size_t len =
                (cmd.opt.RangeEnd >= cmd.opt.RangeStart) ? (cmd.opt.RangeEnd - cmd.opt.RangeStart + 1) : 0;
            AnnotatedMutex::Guard lk(mTasksMutex);
            auto& t = mTasks[id];
            t.TotalSize = len;
            t.Status = TaskStatus::Running;
            transitionLocked_(id, TaskStatus::Running);
        }
        bool ok = p2pQueryPeersAsync_(
            id, cmd.opt.HasRange ? cmd.opt.RangeStart : 0, cmd.opt.HasRange ? cmd.opt.RangeEnd : SIZE_MAX);
        if (!ok) {
            AnnotatedMutex::Guard lk(mTasksMutex);
            mTasks[id].Status = TaskStatus::Failed;
            transitionLocked_(id, TaskStatus::Failed);
        }
    } else { // HYBRID：先走 P2P，失败/不足时回落 HTTP（此处简单启动 P2P）
        AnnotatedMutex::Guard lk(mTasksMutex);
        mTasks[id].Status = TaskStatus::Running;
        transitionLocked_(id, TaskStatus::Running);
        bool ok = p2pQueryPeersAsync_(
            id, cmd.opt.HasRange ? cmd.opt.RangeStart : 0, cmd.opt.HasRange ? cmd.opt.RangeEnd : SIZE_MAX);
        if (!ok) {
            // 立即回退 HTTP-探测
            scheduleHttpProbe_(id, cmd.url);
        }
    }
}

void DownloadManager::applyFsm_(TaskId id, const CmdPause&)
{
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(id);
        if (it == mTasks.end())
            return;
        it->second.Paused = true;
        it->second.Status = TaskStatus::Paused;
    }

    auto itH = mDlByTaskHttp.find(id);
    if (itH != mDlByTaskHttp.end() && mDeps.http) {
        for (auto& sp : itH->second)
            if (sp)
                mDeps.http->PauseTask(sp);
    }
    auto itP = mDlByTaskP2p.find(id);
    if (itP != mDlByTaskP2p.end() && mDeps.p2p) {
        for (auto& sp : itP->second)
            if (sp)
                mDeps.p2p->PauseTask(sp);
    }
}

void DownloadManager::applyFsm_(TaskId id, const CmdResume&)
{
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(id);
        if (it == mTasks.end())
            return;
        it->second.Paused = false;
        it->second.Status = TaskStatus::Running;
    }

    auto itH = mDlByTaskHttp.find(id);
    if (itH != mDlByTaskHttp.end() && mDeps.http) {
        for (auto& sp : itH->second) {
            if (sp && !sp->IsEnd())
                mDeps.http->ResumeTask(sp);
        }
    }
    auto itP = mDlByTaskP2p.find(id);
    if (itP != mDlByTaskP2p.end() && mDeps.p2p) {
        for (auto& sp : itP->second) {
            if (sp && !sp->IsEnd())
                mDeps.p2p->ResumeTask(sp);
        }
    }
}

void DownloadManager::applyFsm_(TaskId id, const CmdCancel&)
{
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(id);
        if (it == mTasks.end())
            return;
        it->second.Cancelled = true;
        it->second.Status = TaskStatus::Cancelled;
    }
    cancelAllSubs_(id, true, true);

    mPendingRanges[id].clear();
}

// ========== HTTP 调度 ==========
void DownloadManager::ensurePreallocate_(TaskId id, size_t totalSize)
{
    FileDownloadOptions opts;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto itOpt = mTaskOptions.find(id);
        if (itOpt != mTaskOptions.end())
            opts = itOpt->second;
    }

    if (mDeps.files) {
        // 交给外部 IFileStore，自行保证幂等与非截断
        mDeps.files->Preallocate(id, opts.OutputPath, totalSize);
        return;
    }

    if (opts.OutputPath.empty() || totalSize == 0) {
        return;
    }

    // 存在则打开，不存在则创建，然后扩到需要的尺寸
    std::shared_ptr<std::fstream> fs =
        std::make_shared<std::fstream>(opts.OutputPath, std::ios::in | std::ios::out | std::ios::binary);

    if (!fs->is_open()) {
        // 不存在则创建，再以 in|out 重新打开
        {
            std::ofstream create(opts.OutputPath, std::ios::binary);
        }
        fs->open(opts.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
    }
    if (!fs->is_open()) {
        logError << "ensurePreallocate_: open file failed: " << opts.OutputPath;
        return;
    }

    try {
        // 读取当前大小（不改变写指针）
        fs->seekp(0, std::ios::end);
        std::streamoff cur = fs->tellp();
        if (cur < 0)
            cur = 0;

        // 只在当前小于目标时扩容到 totalSize
        if (static_cast<size_t>(cur) < totalSize) {
            fs->seekp(static_cast<std::streamoff>(totalSize - 1), std::ios::beg);
            char z = 0;
            fs->write(&z, 1);
            fs->flush();
        }

        // 缓存句柄，后续写入统一走同一个 fd，避免再走"临时开关文件"的兜底逻辑
        {
            AnnotatedMutex::Guard lk(mEventMutex);
            mParentFiles[id] = fs;
        }
    } catch (...) {
        logError << "ensurePreallocate_: exception while preallocating";
    }
}

void DownloadManager::scheduleHttpProbe_(TaskId id, const std::string& url)
{
    if (!mDeps.http) {
        logError << "HTTP engine not injected";
        return;
    }
    FileDownloadOptions opts;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto itOpt = mTaskOptions.find(id);
        if (itOpt != mTaskOptions.end())
            opts = itOpt->second;
    }
    const size_t probeStart = (opts.HasRange ? opts.RangeStart : 0);

    dcdn::util::HttpDownloaderTaskOption opt;
    opt.Request = std::make_shared<dcdn::util::HttpRequest>(url);
    opt.Start = probeStart; // 探测
    opt.Notify = &DownloadManager::coreNotifyCallbackHttp;
    opt.Receiver = this;

    auto probe = mDeps.http->CreateTask(&opt);
    if (!probe) {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto itT = mTasks.find(id);
        if (itT != mTasks.end())
            itT->second.Status = TaskStatus::Failed;
        return;
    }

    {
        AnnotatedMutex::Guard lk(mEventMutex);
        ActiveSubTask a;
        a.parentTaskId = id;
        a.downloader = probe;
        a.offset = probeStart;
        a.length = 0;
        a.index = 0;
        a.isProbe = true;
        a.lastTouched = std::chrono::steady_clock::now();
        auto itF = mParentFiles.find(id);
        if (itF != mParentFiles.end())
            a.file = itF->second;
        mActiveByPtrHttp[probe.get()] = std::move(a);
        mDlByTaskHttp[id].push_back(probe);
    }
    mDeps.http->AddTask(probe);
}

void DownloadManager::scheduleHttpChunk_(TaskId id, const std::string& url, size_t start, size_t end)
{
    if (!mDeps.http) {
        logError << "HTTP engine not injected";
        return;
    }

    dcdn::util::HttpDownloaderTaskOption opt;
    opt.Request = std::make_shared<dcdn::util::HttpRequest>(url);
    opt.Start = start;
    opt.End = end;
    opt.Notify = &DownloadManager::coreNotifyCallbackHttp;
    opt.Receiver = this;
    logInfo << "创建任务 start " << start << " end " << end << std::endl;
    auto sub = mDeps.http->CreateTask(&opt);
    if (!sub) {
        logWarn << "CreateTask failed range " << start << "-" << end;
        return;
    }

    std::shared_ptr<std::fstream> parentFile;
    size_t idx = 0;

    auto itF = mParentFiles.find(id);
    if (itF != mParentFiles.end())
        parentFile = itF->second;
    idx = mNextRangeIdx[id]++;

    ActiveSubTask st;
    st.parentTaskId = id;
    st.offset = start;
    st.length = end - start + 1;
    st.index = static_cast<int>(idx);
    st.downloader = sub;
    st.file = parentFile;
    st.isProbe = false;
    st.lastTouched = std::chrono::steady_clock::now();

    mActiveByPtrHttp[sub.get()] = std::move(st);
    mDlByTaskHttp[id].push_back(sub);

    mDeps.http->AddTask(sub);
}

// ========== HTTP-only：切分 ==========
void DownloadManager::splitTask(TaskId id, size_t probedDataLen, size_t totalSize, size_t baseOffset)
{
    FileDownloadOptions opts;
    std::string url;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(id);
        if (it == mTasks.end())
            return;
        url = it->second.Url;
        auto itOpt = mTaskOptions.find(id);
        if (itOpt != mTaskOptions.end())
            opts = itOpt->second;
    }

    ensurePreallocate_(id, totalSize);

    // 清理 probe（若有）
    std::shared_ptr<util::DownloaderTask> probeToCancel;
    size_t already = 0;
    {
        auto itD = mDlByTaskHttp.find(id);
        if (itD != mDlByTaskHttp.end()) {
            for (auto& sp : itD->second) {
                if (!sp)
                    continue;
                auto raw = sp.get();
                auto itActive = mActiveByPtrHttp.find(raw);
                if (itActive != mActiveByPtrHttp.end() && itActive->second.isProbe) {
                    probeToCancel = sp;
                    break;
                }
            }
        }
    }
    if (probeToCancel && mDeps.http) {
        already = probedDataLen;
        mDeps.http->CancelTask(probeToCancel);
        AnnotatedMutex::Guard lk(mEventMutex);
        mActiveByPtrHttp.erase(probeToCancel.get());
        auto& vec = mDlByTaskHttp[id];
        vec.erase(std::remove(vec.begin(), vec.end(), probeToCancel), vec.end());
        mCancelledRawHttp.insert(probeToCancel.get());
        mDlEventsHttp.erase(probeToCancel.get());
    }

    const size_t absBegin = baseOffset;
    const size_t absEnd = baseOffset + totalSize - 1;

    size_t rangeStart = absBegin + already;
    if (rangeStart > absEnd) {
        applyFsm_(id, FinalizeCheck{id});
        return;
    }

    if (already > 0) {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto itT = mTasks.find(id);
        if (itT != mTasks.end()) {
            itT->second.LastUpdate = std::chrono::system_clock::now();
            calculateSpeedLocked(itT->second, itT->second.LastUpdate);
        }
    }

    auto chunkHitByConcurrent = opts.MaxConcurrent > 0 ? totalSize / opts.MaxConcurrent : 0;
    size_t chunk = (opts.ChunkSize ? opts.ChunkSize : (1u << 20));
    chunk = std::max(chunk, chunkHitByConcurrent);
    std::vector<Range> ranges;
    for (size_t pos = rangeStart; pos <= absEnd;) {
        size_t rEnd = std::min(pos + chunk - 1, absEnd);
        ranges.push_back({pos, rEnd});
        logInfo << "range: [" << pos << ", " << rEnd << "]" << std::endl;
        if (rEnd == absEnd)
            break;
        pos = rEnd + 1;
    }

    if (ranges.empty()) {
        applyFsm_(id, FinalizeCheck{id});
        return;
    }

    const size_t canLaunch = std::min(ranges.size(), mMaxConcurrent);
    std::shared_ptr<std::fstream> parentFile;
    {
        AnnotatedMutex::Guard lk(mEventMutex);
        auto itF = mParentFiles.find(id);
        if (itF != mParentFiles.end())
            parentFile = itF->second;
        mNextRangeIdx[id] = 0;
    }

    bool paused = false, cancelled = false;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto itT = mTasks.find(id);
        if (itT != mTasks.end()) {
            paused = itT->second.Paused.load();
            cancelled = itT->second.Cancelled.load();
        }
    }
    if (paused || cancelled || canLaunch == 0) {
        AnnotatedMutex::Guard lk(mEventMutex);
        auto& q = mPendingRanges[id];
        for (auto& r : ranges)
            q.push_back(r);
        applyFsm_(id, FinalizeCheck{id});
        return;
    }

    for (size_t i = 0; i < canLaunch; ++i) {
        auto r = ranges[i];
        scheduleHttpChunk_(id, url, r.start, r.end);
    }

    if (ranges.size() > canLaunch) {
        AnnotatedMutex::Guard lk(mEventMutex);
        auto& q = mPendingRanges[id];
        for (size_t i = canLaunch; i < ranges.size(); ++i)
            q.push_back(ranges[i]);
    }
    applyFsm_(id, FinalizeCheck{id});
}

// ========== P2P peers 异步查询 ==========
static inline json buildQueryPeersRequest(
    const std::string& ip,
    const std::string& url,
    const std::string& hash,
    const std::string& start,
    const std::string& scenario,
    bool showBlocksHash)
{
    json j;
    j["ip"] = ip;
    j["url"] = url;
    j["hash"] = hash;
    j["start"] = start;
    j["scenario"] = scenario;
    j["showBlocksHash"] = showBlocksHash;
    return j;
}

bool DownloadManager::p2pQueryPeersAsync_(TaskId id, size_t start, size_t end)
{
    DownloadTask t;
    FileDownloadOptions opt;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(id);
        if (it == mTasks.end())
            return false;
        t = it->second;
        auto itOpt = mTaskOptions.find(id);
        if (itOpt != mTaskOptions.end())
            opt = itOpt->second;
    }

    auto arg =
        buildQueryPeersRequest("127.0.0.1" /*可改为实际IP*/, t.Url, t.ContentHash, std::to_string(start), "", true);
    void* reqId = nullptr;

    auto succ = [this, id, start, end](nlohmann::json& res) {
        this->PostEvent(
            std::make_shared<ArgEvent<std::function<void()>>>(
                EventType::FunctionCall, [this, id, start, end, res = std::move(res)]() REQUIRES(dm_thread()) {
                    this->onP2PPeerQuerySuccess_(id, std::move(res), start, end);
                }));
    };
    // auto fail = [this, id, start](int code) REQUIRES(dm_thread()) { this->onP2PPeerQueryFail_(id, code, start); };
    auto fail = [this, id, start](int code) {
        this->PostEvent(
            std::make_shared<ArgEvent<std::function<void()>>>(
                EventType::FunctionCall,
                [this, id, start, code]() REQUIRES(dm_thread()) { this->onP2PPeerQueryFail_(id, code, start); }));
    };

    auto mm = MainManager::Singlet();
    if (mm == nullptr) {
        logError << "[P2P] MainManager::Singlet() is null";
        return false;
    }
    int rc = mm->AsyncApiPostWithToken(&reqId, kApiPeersByHashOrUrl, arg, this, succ, fail);
    if (rc != 1) {
        logWarn << "[P2P] Async query failed rc=" << rc << " taskId=" << id;
        return false;
    }

    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        p2pStates_[id][start].queryReqId = reqId;
        p2pStates_[id][start].queryInFlight = true;
        p2pStates_[id][start].offset = start;
        p2pStates_[id][start].lastQueryErr = 0;
    }
    return true;
}

void DownloadManager::onP2PPeerQueryFail_(TaskId id, int errCode, size_t start)
{
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto& seg = p2pStates_[id][start];
        seg.queryInFlight = false;
        seg.queryDone = true;
        seg.lastQueryErr = errCode;
    }

    bool strategyP2pOnly = false;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTaskOptions.find(id);
        strategyP2pOnly = (it != mTaskOptions.end() && it->second.Strategy == DownloadStrategy::P2P_ONLY);
    }
    if (strategyP2pOnly) {
        applyFsm_(id, TaskFailed{errCode, "P2P peer query failed"});
    } else {
        // TODO: implement: HYBRID or http：回退到 HTTP 探测
        logError << "[P2P] onP2PPeerQueryFail_ taskId=" << id << " start=" << start << " errCode=" << errCode;
        applyFsm_(id, TaskFailed{errCode, "P2P peer query failed"});
    }
    applyFsm_(id, FinalizeCheck{id});
}

void DownloadManager::onP2PPeerQuerySuccess_(TaskId id, nlohmann::json res, size_t start, size_t end)
{
    logInfo << "[P2P] onP2PPeerQuerySuccess_ taskId=" << id << " start=" << start << " end=" << end
            << " response=" << res.dump();
    std::vector<PeerChunk> plan;
    size_t totalSizeFromApi = 0;

    try {
        if (res.contains("size")) {
            totalSizeFromApi = std::stoull(res.value("size", "0"));
        }
        if (res.contains("peers") && res["peers"].is_array()) {
            for (auto& p : res["peers"]) {
                PeerChunk pc;
                pc.peerId = p.value("peerId", "");
                pc.url = p.value("url", "");
                pc.hash = p.value("hash", "");
                pc.iceUfrag = p.value("iceUfrag", "");
                pc.icePwd = p.value("icePwd", "");
                pc.remoteSdp = p.value("connMeta", "");
                pc.start = std::stoull(p.value("start", "0"));
                assert(std::stoull(p.value("end", "0")) > 0);
                pc.end = std::stoull(p.value("end", "0")) - 1; // server returns [start, end)
                plan.emplace_back(std::move(pc));
            }
        }
    } catch (const std::exception& ex) {
        logWarn << "[P2P] parse response error: " << ex.what() << " taskId=" << id;
        onP2PPeerQueryFail_(id, -2, start);
        return;
    }

    PlanReady ev;
    ev.chunks = std::move(plan);
    ev.startHint = start;
    ev.endHint = end;
    ev.totalSizeHint = totalSizeFromApi;

    applyFsm_(id, std::move(ev));
    applyFsm_(id, FinalizeCheck{id});
}

void DownloadManager::scheduleP2PChunks_(TaskId id, const std::vector<PeerChunk>& plan, size_t start, size_t end)
{
    if (plan.empty()) {
        logError << "scheduleP2PChunks_: plan empty";
        return;
    }

#ifdef DEBUG_LOCAL_P2P
    std::vector<PeerChunk> planO;
    planO.push_back({.peerId = "peer1", .start = 0, .end = (1 << 21) - 1});
    std::cout << "INPUT connMeta " << std::endl;
    std::string remote_sdp, line;
    while (std::getline(std::cin, line) && !line.empty()) {
        remote_sdp += line + "\r\n";
    }
    if (remote_sdp.size() >= 2) {
        remote_sdp.pop_back();
        remote_sdp.pop_back();
    }
    planO.back().remoteSdp = remote_sdp;
#else
    std::vector<PeerChunk> planO = plan;
#endif

    DownloadTask t;
    FileDownloadOptions opts;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto itT = mTasks.find(id);
        if (itT == mTasks.end())
            return;
        t = itT->second;
        auto itOpt = mTaskOptions.find(id);
        if (itOpt != mTaskOptions.end())
            opts = itOpt->second;
    }
    if (t.Paused || t.Cancelled)
        return;

    size_t runningP2p = 0;
    {
        AnnotatedMutex::Guard lk(mEventMutex);
        auto it = mDlByTaskP2p.find(id);
        if (it != mDlByTaskP2p.end())
            runningP2p = it->second.size();
        if (mNextRangeIdx.find(id) == mNextRangeIdx.end())
            mNextRangeIdx[id] = 0;
    }

    // 简化策略：取能覆盖 [start,end] 的最长 peer chunk，clip 到 [start,end]
    PeerChunk chosen = planO.front();
    for (auto& pc : planO)
        if (pc.end > chosen.end)
            chosen = pc;

    PeerChunk clipped = chosen;
    clipped.start = start;
    if (end != SIZE_MAX)
        clipped.end = std::min(chosen.end, end);

    if (clipped.start > start) {
        logWarn << "[P2P] clipped.start > start, unexpected";
    } else if (end != SIZE_MAX && clipped.end < end) {
        size_t missStart = clipped.end + 1;
        size_t missEnd = end;
        AnnotatedMutex::Guard lk(mEventMutex);
        mPendingRanges[id].push_front(Range{missStart, missEnd});
    }

    const size_t room = (mMaxConcurrent > runningP2p) ? (mMaxConcurrent - runningP2p) : 0;
    if (room == 0) {
        AnnotatedMutex::Guard lk(mEventMutex);
        // 简化：直接入 pending，等待后续续排
        mPendingRanges[id].push_back(Range{clipped.start, clipped.end});
        return;
    }

    if (!startOneP2PChunk_(id, clipped)) {
        logError << "[P2P] startOneP2PChunk_ failed " << clipped.start << "-" << clipped.end;
    }

    applyFsm_(id, FinalizeCheck{id});
}

bool DownloadManager::startOneP2PChunk_(TaskId id, const PeerChunk& pc)
{
    if (!mDeps.p2p) {
        logError << "P2P engine not injected";
        return false;
    }

    // no launch if paused/cancelled
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(id);
        if (it == mTasks.end())
            return false;
        if (it->second.Paused || it->second.Cancelled)
            return false;
    }

    std::shared_ptr<std::fstream> parentFile;
    {
        AnnotatedMutex::Guard lk(mEventMutex);
        auto itF = mParentFiles.find(id);
        if (itF != mParentFiles.end())
            parentFile = itF->second;
    }

    download::P2PDownloaderTaskOption opt;
    opt.PeerID = pc.peerId;
    opt.PeerSdp = pc.remoteSdp;
#ifdef DEBUG_LOCAL_P2P
    opt.IceUfrag = "kul9";
    opt.IcePwd = "HArM7vdA12b4f+NrSE1hMu";
    opt.FileHash = "cd4a7faf4ed9cd3486cb08ff1dcfd040";
#else
    opt.IceUfrag = pc.iceUfrag;
    opt.IcePwd = pc.icePwd;
    opt.FileHash = pc.hash;
#endif
    opt.Start = pc.start;
    opt.End = pc.end + 1; // P2PDownloaderTaskOption expects [start, end) , so we need to add 1
    opt.Notify = &DownloadManager::coreNotifyCallbackP2p;
    opt.Receiver = this;
    logDebug << "[P2P] CreateTask " << pc.start << "-" << pc.end << "iceUfrag = " << opt.IceUfrag
             << " icePwd = " << opt.IcePwd << " hash = " << opt.FileHash << "remote sdp = " << pc.remoteSdp;
    auto sub = mDeps.p2p->CreateTask(&opt);
    if (!sub) {
        logWarn << "[P2P] CreateTask failed " << pc.start << "-" << pc.end;
        return false;
    }

    size_t idx = 0;
    {
        AnnotatedMutex::Guard lk(mEventMutex);
        idx = mNextRangeIdx[id]++;
        ActiveSubTask st;
        st.parentTaskId = id;
        st.offset = pc.start;
        st.length = pc.end - pc.start + 1;
        st.index = static_cast<int>(idx);
        st.downloader = sub;
        st.file = parentFile;
        st.isProbe = false;
        st.transport = ActiveSubTask::Transport::P2P;
        st.lastTouched = std::chrono::steady_clock::now();

        mActiveByPtrP2p[sub.get()] = std::move(st);
        mDlByTaskP2p[id].push_back(sub);
    }

    mDeps.p2p->AddTask(sub);
    return true;
}

// ========== 任务管理（线程安全：投递到事件循环）==========
TaskId DownloadManager::AddDownloadTask(
    const std::string& url,
    const std::string& contentHash,
    const FileDownloadOptions& options)
{
    auto prom = std::make_shared<std::promise<TaskId>>();
    auto fut = prom->get_future();

    this->PostEvent(
        std::make_shared<ArgEvent<std::function<void()>>>(
            EventType::FunctionCall, [this, prom, url, contentHash, options = options]() REQUIRES(dm_thread()) {
                // FSM 创建任务并启动流程
                applyFsm_(0, CmdAdd{url, contentHash, options});
                // FSM 写入的成员取回 taskId
                TaskId idRet = mLastCreatedTaskId.load(std::memory_order_acquire);
                prom->set_value(idRet);

                if (options.taskStateChangeEventCallback) {
                    Subscribe([idRet, options](const DMEvent& ev) {
                        if (ev.id == idRet) {
                            options.taskStateChangeEventCallback(ev);
                        }
                    });
                }
            }));

    return fut.get();
}

bool DownloadManager::CancelDownloadTask(TaskId taskId)
{
    auto prom = std::make_shared<std::promise<bool>>();
    auto fut = prom->get_future();
    this->PostEvent(
        std::make_shared<ArgEvent<std::function<void()>>>(
            EventType::FunctionCall, [this, prom, taskId]() REQUIRES(dm_thread()) {
                applyFsm_(taskId, CmdCancel{taskId});
                prom->set_value(true);
            }));
    return fut.get();
}

bool DownloadManager::PauseDownloadTask(TaskId taskId)
{
    auto prom = std::make_shared<std::promise<bool>>();
    auto fut = prom->get_future();
    this->PostEvent(
        std::make_shared<ArgEvent<std::function<void()>>>(
            EventType::FunctionCall, [this, prom, taskId]() REQUIRES(dm_thread()) {
                applyFsm_(taskId, CmdPause{taskId});
                prom->set_value(true);
            }));
    return fut.get();
}

bool DownloadManager::ResumeDownloadTask(TaskId taskId)
{
    auto prom = std::make_shared<std::promise<bool>>();
    auto fut = prom->get_future();
    this->PostEvent(
        std::make_shared<ArgEvent<std::function<void()>>>(
            EventType::FunctionCall, [this, prom, taskId]() REQUIRES(dm_thread()) {
                applyFsm_(taskId, CmdResume{taskId});
                prom->set_value(true);
            }));
    return fut.get();
}

// ========== 状态查询 ==========
DownloadTask DownloadManager::GetTaskStatus(TaskId taskId) const
{
    AnnotatedMutex::Guard lk(mTasksMutex);
    auto it = mTasks.find(taskId);
    if (it != mTasks.end())
        return it->second;
    return {};
}

std::vector<DownloadTask> DownloadManager::GetAllTasks() const
{
    AnnotatedMutex::Guard lk(mTasksMutex);
    std::vector<DownloadTask> out;
    out.reserve(mTasks.size());
    for (auto& kv : mTasks)
        out.push_back(kv.second);
    return out;
}

double DownloadManager::GetOverallSpeed() const
{
    AnnotatedMutex::Guard lk(mTasksMutex);
    double sum = 0.0;
    for (auto& kv : mTasks)
        sum += kv.second.Speed;
    return sum;
}

// ========== 回调注册 ==========
void DownloadManager::SetBufferReadyCallback(TaskId taskId, BufferReadyCallback callback)
{
    AnnotatedMutex::Guard lk(mTasksMutex);
    mBufferCallbacks[taskId] = std::move(callback);
}

void DownloadManager::RemoveBufferReadyCallback(TaskId taskId)
{
    AnnotatedMutex::Guard lk(mTasksMutex);
    mBufferCallbacks.erase(taskId);
}

std::vector<std::pair<size_t, size_t>> DownloadManager::GetAvailableRanges(TaskId taskId) const
{
    AnnotatedMutex::Guard lk(mTasksMutex);
    auto it = mTasks.find(taskId);
    if (it != mTasks.end())
        return it->second.CompletedRanges;
    return {};
}

// ========== 带宽比配置（预留）==========
void DownloadManager::SetHttpBandwidthRatio(float ratio)
{
    mHttpBandwidthRatio = ratio;
}
void DownloadManager::SetP2pBandwidthRatio(float ratio)
{
    mP2pBandwidthRatio = ratio;
}

DownloadManager::SubId DownloadManager::Subscribe(Handler h)
{
    AnnotatedMutex::Guard lk(mSubMutex);
    auto id = mNextSubId++;
    mSubscribers.emplace(id, std::move(h));
    return id;
}
void DownloadManager::Unsubscribe(SubId id)
{
    AnnotatedMutex::Guard lk(mSubMutex);
    mSubscribers.erase(id);
}

// ========== 进度 / 速度 / BufferReady ==========
void DownloadManager::updateTaskProgress(TaskId id, size_t downloaded)
{
    auto now = std::chrono::system_clock::now();
    bool reachedEnd = false;

    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mTasks.find(id);
        if (it == mTasks.end())
            return;

        it->second.Downloaded += downloaded;
        if (it->second.TotalSize > 0 && it->second.Downloaded >= it->second.TotalSize) {
            it->second.Downloaded = it->second.TotalSize;
            reachedEnd = true;
            if (it->second.Downloaded > it->second.TotalSize) {
                logWarn << "Task " << id << " downloaded more than expected: " << it->second.Downloaded << " > "
                        << it->second.TotalSize;
            }
        }
        it->second.LastUpdate = now;
        calculateSpeedLocked(it->second, now);
    }

    if (reachedEnd)
        applyFsm_(id, FinalizeCheck{id});
}

void DownloadManager::calculateSpeedLocked(DownloadTask& t, std::chrono::system_clock::time_point now)
{
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now - t.StartTime).count();
    t.Speed = (seconds > 0) ? (static_cast<double>(t.Downloaded) / static_cast<double>(seconds)) : 0.0;
}

void DownloadManager::notifyBufferReady(const TaskId& id, size_t start, size_t end)
{
    BufferReadyCallback cb;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto it = mBufferCallbacks.find(id);
        if (it != mBufferCallbacks.end())
            cb = it->second;
    }
    if (cb)
        cb(id, start, end);

    EBufferReady ev{id, start, end};
    publish_(ev);
}

void DownloadManager::cancelAllSubs_(TaskId id, bool http, bool p2p)
{
    AnnotatedMutex::Guard lk(mEventMutex);
    if (http && mDeps.http) {
        auto itVec = mDlByTaskHttp.find(id);
        if (itVec != mDlByTaskHttp.end()) {
            for (auto& sp : itVec->second) {
                if (!sp)
                    continue;
                mDeps.http->CancelTask(sp);
                mCancelledRawHttp.insert(sp.get());
                mActiveByPtrHttp.erase(sp.get());
            }
            itVec->second.clear();
        }
        for (auto it2 = mDlEventsHttp.begin(); it2 != mDlEventsHttp.end();) {
            if (mCancelledRawHttp.count(it2->first))
                it2 = mDlEventsHttp.erase(it2);
            else
                ++it2;
        }
    }
    if (p2p && mDeps.p2p) {
        auto itVec = mDlByTaskP2p.find(id);
        if (itVec != mDlByTaskP2p.end()) {
            for (auto& sp : itVec->second) {
                if (!sp)
                    continue;
                mDeps.p2p->CancelTask(sp);
                mCancelledRawP2p.insert(sp.get());
                mActiveByPtrP2p.erase(sp.get());
            }
            itVec->second.clear();
        }
        for (auto it2 = mDlEventsP2p.begin(); it2 != mDlEventsP2p.end();) {
            if (mCancelledRawP2p.count(it2->first))
                it2 = mDlEventsP2p.erase(it2);
            else
                ++it2;
        }
    }

    // delete output file ( if file path specified and no IFileStore injected )
    FileDownloadOptions opts;
    {
        AnnotatedMutex::Guard lk(mTasksMutex);
        auto itOpt = mTaskOptions.find(id);
        if (itOpt != mTaskOptions.end())
            opts = itOpt->second;
    }
    if (!opts.OutputPath.empty() && !mDeps.files) {
        mParentFiles.erase(id);
        std::remove(opts.OutputPath.c_str());
    } else if (mDeps.files) {
        mDeps.files->Close(id);
    }
}

void DownloadManager::publish_(const DMEvent& ev)
{
    // inner subscriber
    {
        AnnotatedMutex::Guard g(mSubMutex);
        for (auto& kv : mSubscribers) {
            try {
                kv.second(ev);
            } catch (...) {
            }
        }
    }

    // the external event bus
    if (mDeps.bus) {
        // IEventBus 可以封装一个 virtual publishImpl，这里假设 bus->Publish<E>(ev) 实际可用
        // 由于接口是抽象 publishImpl(const EventBase&)，在设计里让 DMEvent : EventBase
        // 这里简化为：由外部 IEventBus 的实现接收 DMEvent&（保持兼容）
        // 注意：如需严格类型，在 IEventBus 定义模板 Publish，或在此 dynamic_cast。
        // struct BusShim: public IEventBus
        // {
        //     void publishImpl(const DMEvent&) override {}
        // };
        // 无操作；留给具体项目的 IEventBus 实现去接
    }
}

void DownloadManager::SetStrategy(DownloadStrategy strategy)
{
    mStrategy = strategy;
}

void DownloadManager::SetMaxConcurrentDownloads(size_t max)
{
    mMaxConcurrent = max;
}

void DownloadManager::SetPersistPath(const std::string& path)
{
    mPersistPath = path;
    mDbHelper = std::make_unique<PersistenceHelper>(path);
}
