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
using namespace dcdn;
using nlohmann::json;

// TODO: read from config ?
static constexpr const char* kApiPeersByHashOrUrl = "/api/v1/query_peers_by_file";

namespace {
static uint64_t genTaskId()
{
    static std::atomic<uint64_t> cnt{0};
    static const uint64_t base = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    return base + cnt++;
}
} // namespace

struct Range
{
    size_t start;
    size_t end;
};

struct ActiveSubTask
{
    uint64_t parentTaskId = 0;
    size_t offset = 0; // 分片起始绝对偏移
    size_t length = 0; // 分片长度
    int index = 0; // 分片序号
    std::shared_ptr<dcdn::util::DownloaderTask> downloader;
    std::shared_ptr<std::fstream> file;
    bool isProbe = false;
    uint64_t actualGot = 0;

    // 看门狗：最后一次有进展（读取到数据）的时间
    std::chrono::steady_clock::time_point lastTouched = std::chrono::steady_clock::now();
};

// ============ DownloadTask 序列化 ============
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

// ====== P2P 查询：query_peers_by_file 的请求/响应解析 ======
static inline json BuildQueryPeersRequest(
    const std::string& ip,
    const std::string& url,
    const std::string& hash,
    const std::string& start,
    const std::string& scenario,
    bool showBlocksHash)
{
    json j;
    j["ip"] = ip; // string
    j["url"] = url; // string
    j["hash"] = hash; // string (文件 hash)
    j["start"] = start; // string (起始偏移，字符串表示)
    j["scenario"] = scenario; // string
    j["showBlocksHash"] = showBlocksHash; // bool
    logDebug << "BuildQueryPeersRequest: " << j.dump();
    return j;
}

// ============ PersistenceHelper ============
DownloadManager::PersistenceHelper::PersistenceHelper(const std::string& dbPath)
{
    mDb = nullptr;
    (void)dbPath;
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
bool DownloadManager::PersistenceHelper::deleteTask(uint64_t taskId)
{
    (void)taskId;
    return true;
}
bool DownloadManager::PersistenceHelper::saveSubTasks(uint64_t taskId, const std::vector<SubTask>& subtasks)
{
    (void)taskId;
    (void)subtasks;
    return true;
}
bool DownloadManager::PersistenceHelper::loadSubTasks(uint64_t taskId, std::vector<SubTask>& subtasks)
{
    (void)taskId;
    (void)subtasks;
    return true;
}

// ============ DownloadManager 构造 / 析构 / 线程 ============
DownloadManager::DownloadManager(
    std::shared_ptr<util::HttpDownloader> http,
    std::shared_ptr<download::P2PDownloader> p2p)
    : BaseManager(nullptr), mHttpDownloader(http), mP2pDownloader(p2p)
{
    this->registerHandler(EventType::FunctionCall, &DownloadManager::handleFunctionCall);
}

void DownloadManager::Init() {}

DownloadManager::~DownloadManager()
{
    if (mHttpDownloader) {
        mHttpDownloader.reset();
    }
}

// function call + downloader data events + watchdog
void DownloadManager::run()
{
    using namespace std::chrono_literals;

    auto lastReap = std::chrono::steady_clock::now();
    const auto reapInterval = 1s; // 看门狗巡检周期
    const auto staleTimeout = 8s; // 分片 N 秒无任何进展视为卡死

    while (true) {
        {
            std::unique_lock<std::mutex> lk(mMtx);
            mCv.wait_for(lk, reapInterval, [&] { return !mDlEventsHttp.empty() || !mEvents.empty(); });
        }

        // process a control cmd
        if (!mEvents.empty()) {
            this->waitEvent(0ms);
        }

        // pop downloader read events
        std::vector<std::shared_ptr<util::DownloaderTask>> httpBatch;
        std::vector<std::shared_ptr<util::DownloaderTask>> p2pBatch;
        {
            std::lock_guard<std::mutex> g(mMtx);
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

        // —— 看门狗：回收卡死的分片
        auto now = std::chrono::steady_clock::now();
        if (now - lastReap >= reapInterval) {
            lastReap = now;

            // 拷贝 snapshot，避免持锁太久
            std::vector<ActiveSubTask> snapshot;
            {
                std::lock_guard<std::mutex> l(mMtx);
                snapshot.reserve(mActiveByPtrHttp.size());
                for (auto& kv : mActiveByPtrHttp)
                    snapshot.push_back(kv.second);
            }

            for (auto& st : snapshot) {
                if (st.isProbe || st.length == 0)
                    continue;
                // TODO: 实现p2p模式的watchdog
                if (st.transport == ActiveSubTask::Transport::P2P) {
                    logInfo << "[P2P]watchdog skip in p2p mode (un-implemented)" << std::endl;
                    continue;
                }

                // 若任务被暂停/取消，跳过（不误判）
                bool paused = false, cancelled = false;
                {
                    std::lock_guard<std::mutex> lk(mTasksMutex);
                    auto it = mTasks.find(st.parentTaskId);
                    if (it != mTasks.end()) {
                        paused = it->second.Paused.load();
                        cancelled = it->second.Cancelled.load();
                    }
                }
                if (paused || cancelled)
                    continue;

                if (now - st.lastTouched > staleTimeout) {
                    const size_t got = static_cast<size_t>(st.actualGot);
                    if (got >= st.length)
                        continue;

                    const size_t missStart = st.offset + got;
                    const size_t missEnd = st.offset + st.length - 1;
                    logWarn << "看门狗扫描：子任务 " << st.index << " 无任何进展，miss[" << missStart << ", " << missEnd
                            << "]";
                    // 回填 + 续排（注意：续排前再次读 paused/cancelled）
                    {
                        std::lock_guard<std::mutex> l(mMtx);
                        auto raw = st.downloader.get();

                        mActiveByPtrHttp.erase(raw);
                        auto& vec = mDlByTaskHttp[st.parentTaskId];
                        vec.erase(
                            std::remove_if(
                                vec.begin(),
                                vec.end(),
                                [raw](const std::shared_ptr<dcdn::util::DownloaderTask>& p) { return p.get() == raw; }),
                            vec.end());

                        mPendingRanges[st.parentTaskId].push_front(Range{missStart, missEnd});
                        mCancelledRawHttp.insert(raw);
                        mHttpDownloader->CancelTask(st.downloader);
                        logWarn << "看门狗扫描：无进展子任务 " << st.index << "被取消";
                    }

                    // 再次检查暂停/取消
                    bool paused2 = false, cancelled2 = false;
                    {
                        std::lock_guard<std::mutex> lk(mTasksMutex);
                        auto it = mTasks.find(st.parentTaskId);
                        if (it != mTasks.end()) {
                            paused2 = it->second.Paused.load();
                            cancelled2 = it->second.Cancelled.load();
                        }
                    }
                    if (paused2 || cancelled2) {
                        logWarn << "[watchdog] 任务处于暂停/取消，缺口已回填，不续排" << " st.index " << st.index;
                        continue;
                    }

                    // 立即续排一个
                    Range r;
                    bool hasNext = false;
                    {
                        std::lock_guard<std::mutex> l(mMtx);
                        auto& q = mPendingRanges[st.parentTaskId];
                        if (!q.empty()) {
                            r = q.front();
                            q.pop_front();
                            hasNext = true;
                        }
                    }
                    if (hasNext) {
                        std::string urlLocal;
                        FileDownloadOptions opts2;
                        {
                            std::lock_guard<std::mutex> lk(mTasksMutex);
                            auto itT = mTasks.find(st.parentTaskId);
                            if (itT != mTasks.end())
                                urlLocal = itT->second.Url;
                            auto itOpt = mTaskOptions.find(st.parentTaskId);
                            if (itOpt != mTaskOptions.end())
                                opts2 = itOpt->second;
                        }

                        try {
                            dcdn::util::HttpDownloaderTaskOption opt;
                            opt.Request = std::make_shared<dcdn::util::HttpRequest>(urlLocal);
                            opt.Start = r.start;
                            opt.End = r.end;
                            opt.Notify = &DownloadManager::coreNotifyCallbackHttp;
                            opt.Receiver = this;

                            auto sub = mHttpDownloader->CreateTask(&opt);
                            if (sub) {
                                std::shared_ptr<std::fstream> f;
                                {
                                    std::lock_guard<std::mutex> l(mMtx);
                                    auto itF = mParentFiles.find(st.parentTaskId);
                                    if (itF != mParentFiles.end())
                                        f = itF->second;

                                    ActiveSubTask st2;
                                    st2.parentTaskId = st.parentTaskId;
                                    st2.offset = r.start;
                                    st2.length = r.end - r.start + 1;
                                    st2.index = static_cast<int>(mNextRangeIdx[st.parentTaskId]++);
                                    st2.downloader = sub;
                                    st2.file = f;
                                    st2.isProbe = false;
                                    st2.lastTouched = std::chrono::steady_clock::now();

                                    mActiveByPtrHttp[sub.get()] = std::move(st2);
                                    mDlByTaskHttp[st.parentTaskId].push_back(sub);
                                }
                                mHttpDownloader->AddTask(sub);
                                logWarn << "[watchdog] 子任务无进展，回填续排原来index任务：" << st.index
                                        << ", 回填range: " << r.start << "-" << r.end;
                            } else {
                                logWarn << "[watchdog] CreateTask 失败，稍后重试 range: " << r.start << "-" << r.end;
                                std::lock_guard<std::mutex> l(mMtx);
                                mPendingRanges[st.parentTaskId].push_front(r);
                            }
                        } catch (...) {
                            std::lock_guard<std::mutex> l(mMtx);
                            mPendingRanges[st.parentTaskId].push_front(r);
                        }
                    }
                }
            }

            // 巡检后看能否 finalize
            {
                std::vector<uint64_t> toCheck;
                {
                    std::lock_guard<std::mutex> l(mMtx);
                    toCheck.reserve(mDlByTaskHttp.size());
                    for (auto& kv : mDlByTaskHttp)
                        toCheck.push_back(kv.first);
                }
                for (auto id : toCheck) {
                    maybeFinalizeTask(id);
                }
            }
        }
    }
}

// FunctionCall 事件：把 std::function<void()> 执行掉
void DownloadManager::handleFunctionCall(std::shared_ptr<Event> evt)
{
    auto* a = dynamic_cast<ArgEvent<std::function<void()>>*>(evt.get());
    if (!a)
        return;
    auto fn = a->Arg();
    if (fn)
        fn();
}

void DownloadManager::coreNotifyCallbackHttp(std::shared_ptr<dcdn::util::DownloaderTask> task, void* receiver)
{
    if (!task || !receiver)
        return;
    auto* self = static_cast<DownloadManager*>(receiver);
    {
        std::lock_guard<std::mutex> g(self->mMtx);
        if (self->mCancelledRawHttp.count(task.get()))
            return; // 丢弃取消后的迟到回调
        self->mDlEventsHttp[task.get()] = std::move(task); // 去重：同一个 raw 只保留一个
    }
    self->mCv.notify_all();
}

void DownloadManager::coreNotifyCallbackP2p(std::shared_ptr<dcdn::util::DownloaderTask> task, void* receiver)
{
    if (!task || !receiver)
        return;
    auto* self = static_cast<DownloadManager*>(receiver);
    {
        std::lock_guard<std::mutex> g(self->mMtx);
        if (self->mCancelledRawP2p.count(task.get()))
            return;
        self->mDlEventsP2p[task.get()] = std::move(task);
    }
    self->mCv.notify_all();
}

// 实际处理一个 downloader 事件（读取 -> 写入 -> 进度/续排/完成判定）
void DownloadManager::processHttpEvent(std::shared_ptr<dcdn::util::DownloaderTask> ev)
{
    auto* raw = ev.get();

    // 任务取消后的迟到通知直接丢弃
    {
        std::lock_guard<std::mutex> g(mMtx);
        if (mCancelledRawHttp.count(raw))
            return;
    }

    // 找到 ActiveSubTask
    ActiveSubTask active;
    {
        std::lock_guard<std::mutex> g(mMtx);
        auto it = mActiveByPtrHttp.find(raw);
        if (it == mActiveByPtrHttp.end()) {
            // 可能是迟到事件，直接早退
            return;
        }
        active = it->second;
    }

    // 如果是 probe：尝试拿到 Content-Length 后 split
    if (active.isProbe && active.parentTaskId != 0) {
        bool needSplit = false;
        size_t clen = 0;
        size_t baseOffset = 0;
        size_t totalLen = 0;
        FileDownloadOptions opts;

        {
            std::lock_guard<std::mutex> lk(mTasksMutex);
            auto it = mTasks.find(active.parentTaskId);
            if (it != mTasks.end() && it->second.TotalSize == 0) {
                if (auto ht = dynamic_cast<dcdn::util::HttpDownloaderTask*>(raw)) {
                    clen = ht->ContentLength();
                    auto itOpt = mTaskOptions.find(active.parentTaskId);
                    if (itOpt != mTaskOptions.end())
                        opts = itOpt->second;

                    if (clen > 0) {
                        if (opts.HasRange) {
                            baseOffset = opts.RangeStart;
                            if (opts.RangeEnd != SIZE_MAX) {
                                totalLen =
                                    (opts.RangeEnd >= opts.RangeStart) ? (opts.RangeEnd - opts.RangeStart + 1) : 0;
                            } else {
                                totalLen = (baseOffset >= clen) ? 0 : (clen - baseOffset);
                            }
                        } else {
                            baseOffset = 0;
                            totalLen = clen;
                        }

                        if (totalLen > 0) {
                            it->second.TotalSize = totalLen;
                            needSplit = true;
                        } else {
                            it->second.Status = TaskStatus::Failed;
                        }
                    }
                }
            }
        }
        if (needSplit) {
            splitTask(active.parentTaskId, totalLen, baseOffset);
        }
    }

    // 读取并写入
    auto buffer = ev->Read();
    size_t readSum = 0;

    FileDownloadOptions opts;
    std::shared_ptr<std::fstream> file;
    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto itOpt = mTaskOptions.find(active.parentTaskId);
        if (itOpt != mTaskOptions.end())
            opts = itOpt->second;
    }
    {
        std::lock_guard<std::mutex> l(mMtx);
        auto itF = mParentFiles.find(active.parentTaskId);
        if (itF != mParentFiles.end())
            file = itF->second;
    }

    // 我这段允许写入的绝对区间
    const size_t segBeg = active.offset;
    const size_t segEnd = active.offset + active.length - 1;

    while (buffer) {
        size_t len = buffer->Length();
        size_t off = buffer->Offset(); // 假定为“资源内绝对偏移”
        size_t bufBeg = off;
        size_t bufEnd = off + (len ? (len - 1) : 0);

        // 与我负责的区间做交集
        size_t wrBeg = (bufBeg > segBeg) ? bufBeg : segBeg;
        size_t wrEnd = (bufEnd < segEnd) ? bufEnd : segEnd;

        if (wrBeg <= wrEnd) {
            size_t accept = wrEnd - wrBeg + 1;
            size_t srcOff = wrBeg - bufBeg; // 从 buffer 内的这个位置开始拷
            size_t dstOff;

            if (opts.HasRange && opts.WriteRangeToSeparateFile) {
                // 写相对文件：把“绝对位置”映射到用户 Range 的相对
                dstOff = (wrBeg >= opts.RangeStart) ? (wrBeg - opts.RangeStart) : 0;
            } else {
                // 共享大文件：按绝对偏移写
                dstOff = wrBeg;
            }

            if (file && file->good()) {
                try {
                    file->seekp(static_cast<std::streamoff>(dstOff), std::ios::beg);
                } catch (...) {
                }
                file->write(reinterpret_cast<const char*>(buffer->Data()) + srcOff, accept);
                // 不必每块都 flush，性能更好；如需稳妥可保留 flush()
                // file->flush();
            } else if (opts.OutputStream) {
                opts.OutputStream->write(reinterpret_cast<const char*>(buffer->Data()) + srcOff, accept);
                // opts.OutputStream->flush();
            } else if (!opts.OutputPath.empty()) {
                std::fstream ofs(opts.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
                if (!ofs) {
                    std::ofstream create(opts.OutputPath, std::ios::binary);
                    create.close();
                    ofs.open(opts.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
                }
                if (ofs) {
                    ofs.seekp(static_cast<std::streamoff>(dstOff), std::ios::beg);
                    ofs.write(reinterpret_cast<const char*>(buffer->Data()) + srcOff, accept);
                }
            }

            // 只累计“真正写入”的字节数
            readSum += accept;

            // 如果你需要回调可用区间，建议也用裁剪后的范围
            if (active.parentTaskId != 0) {
                notifyBufferReady(active.parentTaskId, wrBeg, wrEnd);
            }
        }

        buffer = buffer->Next();
    }

    // 记录分片实际读到的量 + 心跳
    if (readSum > 0) {
        std::lock_guard<std::mutex> l(mMtx);
        auto it = mActiveByPtrHttp.find(raw);
        if (it != mActiveByPtrHttp.end()) {
            it->second.actualGot += readSum;
            it->second.lastTouched = std::chrono::steady_clock::now();
        }
    }
    if (readSum > 0 && active.parentTaskId != 0) {
        updateTaskProgress(active.parentTaskId, readSum);
        notifyBufferReady(active.parentTaskId, active.offset, active.offset + readSum);
    }

    // 结束/短读判定
    bool isEnd = ev->IsEnd();
    uint64_t got = 0;
    {
        std::lock_guard<std::mutex> l(mMtx);
        auto it = mActiveByPtrHttp.find(raw);
        if (it != mActiveByPtrHttp.end())
            got = it->second.actualGot;
    }
    bool endByLength = (!active.isProbe && active.length > 0 && got >= active.length);
    bool shortRead = (!active.isProbe && active.length > 0 && isEnd && got < active.length);

    if ((isEnd || endByLength) && active.parentTaskId != 0) {
        if (shortRead) {
            const size_t missStart = active.offset + got;
            const size_t missEnd = active.offset + active.length - 1;
            {
                std::lock_guard<std::mutex> l(mMtx);
                mPendingRanges[active.parentTaskId].push_front(Range{missStart, missEnd});
                // 清理映射
                mActiveByPtrHttp.erase(raw);
                auto& vec = mDlByTaskHttp[active.parentTaskId];
                vec.erase(
                    std::remove_if(
                        vec.begin(),
                        vec.end(),
                        [raw](const std::shared_ptr<dcdn::util::DownloaderTask>& p) { return p.get() == raw; }),
                    vec.end());
            }
            logWarn << "子任务 " << active.index << " 短读, got=" << got << " < need=" << active.length
                    << ", 回填缺口: [" << (active.offset + got) << ", " << (active.offset + active.length - 1) << "]";
        } else {
            uint64_t actualGot = got;
            {
                std::lock_guard<std::mutex> l(mMtx);
                mActiveByPtrHttp.erase(raw);
                auto& vec = mDlByTaskHttp[active.parentTaskId];
                vec.erase(
                    std::remove_if(
                        vec.begin(),
                        vec.end(),
                        [raw](const std::shared_ptr<dcdn::util::DownloaderTask>& p) { return p.get() == raw; }),
                    vec.end());
            }
            if (!active.isProbe && active.length > 0) {
                logInfo << "子任务 " << active.index << " 结束, start: " << active.offset
                        << ", end: " << (active.offset + active.length - 1) << ", actually got: " << actualGot
                        << ", expect length: " << active.length << (endByLength && !isEnd ? " (size-guard)" : "");
            } else {
                logInfo << "子任务 " << active.index << " 结束 (probe)";
            }
        }

        // —— 续排一个 pending（先看是否暂停/取消）
        bool paused = false, cancelled = false;
        {
            std::lock_guard<std::mutex> lk(mTasksMutex);
            auto it = mTasks.find(active.parentTaskId);
            if (it != mTasks.end()) {
                paused = it->second.Paused.load();
                cancelled = it->second.Cancelled.load();
            }
        }
        if (!paused && !cancelled) {
            Range r;
            bool hasNext = false;
            size_t idxNext = 0;
            {
                std::lock_guard<std::mutex> l(mMtx);
                auto& q = mPendingRanges[active.parentTaskId];
                if (!q.empty()) {
                    r = q.front();
                    q.pop_front();
                    idxNext = mNextRangeIdx[active.parentTaskId]++;
                    hasNext = true;
                }
            }
            if (hasNext) {
                std::string urlLocal;
                FileDownloadOptions opts2;
                {
                    std::lock_guard<std::mutex> lk(mTasksMutex);
                    auto itT = mTasks.find(active.parentTaskId);
                    if (itT != mTasks.end())
                        urlLocal = itT->second.Url;
                    auto itOpt = mTaskOptions.find(active.parentTaskId);
                    if (itOpt != mTaskOptions.end())
                        opts2 = itOpt->second;
                }

                try {
                    dcdn::util::HttpDownloaderTaskOption opt;
                    opt.Request = std::make_shared<dcdn::util::HttpRequest>(urlLocal);
                    opt.Start = r.start;
                    opt.End = r.end;
                    opt.Notify = &DownloadManager::coreNotifyCallbackHttp;
                    opt.Receiver = this;

                    auto sub = mHttpDownloader->CreateTask(&opt);
                    if (sub) {
                        std::shared_ptr<std::fstream> f;
                        {
                            std::lock_guard<std::mutex> l(mMtx);
                            auto itF = mParentFiles.find(active.parentTaskId);
                            if (itF != mParentFiles.end())
                                f = itF->second;

                            ActiveSubTask st;
                            st.parentTaskId = active.parentTaskId;
                            st.offset = r.start;
                            st.length = r.end - r.start + 1;
                            st.index = static_cast<int>(idxNext);
                            st.downloader = sub;
                            st.file = f;
                            st.isProbe = false;
                            st.lastTouched = std::chrono::steady_clock::now();

                            mActiveByPtrHttp[sub.get()] = std::move(st);
                            mDlByTaskHttp[active.parentTaskId].push_back(sub);
                        }
                        mHttpDownloader->AddTask(sub);
                        logInfo << "子任务 " << idxNext << " 续排, start: " << r.start << ", end: " << r.end;
                    } else {
                        logWarn << "ERROR 创建子任务失败";
                        std::lock_guard<std::mutex> l(mMtx);
                        mPendingRanges[active.parentTaskId].push_front(r);
                    }
                } catch (...) {
                    std::lock_guard<std::mutex> l(mMtx);
                    mPendingRanges[active.parentTaskId].push_front(r);
                }
            }
        }

        // 完成判定：无运行子任务且无 pending
        bool hasRunning = false, hasPending = false;
        {
            std::lock_guard<std::mutex> l(mMtx);
            auto itD = mDlByTaskHttp.find(active.parentTaskId);
            hasRunning = (itD != mDlByTaskHttp.end() && !itD->second.empty());
            auto itP = mPendingRanges.find(active.parentTaskId);
            hasPending = (itP != mPendingRanges.end() && !itP->second.empty());
        }
        if (!hasRunning && !hasPending) {
            maybeFinalizeTask(active.parentTaskId);
        }
    }

    // —— 处理完一个 downloader 事件的最后，补一次 finalize 判定（幂等）
    if (active.parentTaskId != 0) {
        maybeFinalizeTask(active.parentTaskId);
    }
}

// TODO: some duplicate with processHttpEvent
void DownloadManager::processP2pEvent(std::shared_ptr<util::DownloaderTask> ev)
{
    auto* raw = ev.get();

    {
        std::lock_guard<std::mutex> g(mMtx);
        if (mCancelledRawP2p.count(raw))
            return;
    }

    ActiveSubTask active;
    {
        std::lock_guard<std::mutex> g(mMtx);
        auto it = mActiveByPtrP2p.find(raw);
        if (it == mActiveByPtrP2p.end()) {
            // 可能是迟到事件，直接早退
            return;
        }
        active = it->second;
    }

    auto buffer = ev->Read();
    size_t readSum = 0;

    FileDownloadOptions opts;
    std::shared_ptr<std::fstream> file;
    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto itOpt = mTaskOptions.find(active.parentTaskId);
        if (itOpt != mTaskOptions.end())
            opts = itOpt->second;
    }
    {
        std::lock_guard<std::mutex> l(mMtx);
        auto itF = mParentFiles.find(active.parentTaskId);
        if (itF != mParentFiles.end())
            file = itF->second;
    }

    // 我这段允许写入的绝对区间
    const size_t segBeg = active.offset;
    const size_t segEnd = active.offset + active.length - 1;

    while (buffer) {
        size_t len = buffer->Length();
        size_t off = buffer->Offset(); // 假定为"资源内绝对偏移"
        size_t bufBeg = off;
        size_t bufEnd = off + (len ? (len - 1) : 0);

        // 与我负责的区间做交集
        size_t wrBeg = (bufBeg > segBeg) ? bufBeg : segBeg;
        size_t wrEnd = (bufEnd < segEnd) ? bufEnd : segEnd;

        if (wrBeg <= wrEnd) {
            size_t accept = wrEnd - wrBeg + 1;
            size_t srcOff = wrBeg - bufBeg; // 从 buffer 内的这个位置开始拷
            size_t dstOff;

            if (opts.HasRange && opts.WriteRangeToSeparateFile) {
                // 写相对文件：把"绝对位置"映射到用户 Range 的相对
                dstOff = (wrBeg >= opts.RangeStart) ? (wrBeg - opts.RangeStart) : 0;
            } else {
                // 共享大文件：按绝对偏移写
                dstOff = wrBeg;
            }

            if (file && file->good()) {
                try {
                    file->seekp(static_cast<std::streamoff>(dstOff), std::ios::beg);
                } catch (...) {
                }
                file->write(reinterpret_cast<const char*>(buffer->Data()) + srcOff, accept);
                // 不必每块都 flush，性能更好；如需稳妥可保留 flush()
                // file->flush();
            } else if (opts.OutputStream) {
                opts.OutputStream->write(reinterpret_cast<const char*>(buffer->Data()) + srcOff, accept);
                // opts.OutputStream->flush();
            } else if (!opts.OutputPath.empty()) {
                std::fstream ofs(opts.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
                if (!ofs) {
                    std::ofstream create(opts.OutputPath, std::ios::binary);
                    create.close();
                    ofs.open(opts.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
                }
                if (ofs) {
                    ofs.seekp(static_cast<std::streamoff>(dstOff), std::ios::beg);
                    ofs.write(reinterpret_cast<const char*>(buffer->Data()) + srcOff, accept);
                }
            }

            readSum += accept;

            if (active.parentTaskId != 0) {
                notifyBufferReady(active.parentTaskId, wrBeg, wrEnd);
            }
        }

        buffer = buffer->Next();
    }

    // 记录分片实际读到的量 + 心跳
    if (readSum > 0) {
        std::lock_guard<std::mutex> l(mMtx);
        auto it = mActiveByPtrP2p.find(raw);
        if (it != mActiveByPtrP2p.end()) {
            it->second.actualGot += readSum;
            it->second.lastTouched = std::chrono::steady_clock::now();
        }
    }
    if (readSum > 0 && active.parentTaskId != 0) {
        updateTaskProgress(active.parentTaskId, readSum);
        notifyBufferReady(active.parentTaskId, active.offset, active.offset + readSum);
    }

    // 结束/短读判定
    bool isEnd = ev->IsEnd();
    uint64_t got = 0;
    {
        std::lock_guard<std::mutex> l(mMtx);
        auto it = mActiveByPtrP2p.find(raw);
        if (it != mActiveByPtrP2p.end())
            got = it->second.actualGot;
    }
    bool endByLength = (!active.isProbe && active.length > 0 && got >= active.length);
    bool shortRead = (!active.isProbe && active.length > 0 && isEnd && got < active.length);

    if ((isEnd || endByLength) && active.parentTaskId != 0 && ev->Status() == util::DownloaderTask::Completed) {
        // TODO: refactor duplication code in if else
        if (shortRead) {
            const size_t missStart = active.offset + got;
            const size_t missEnd = active.offset + active.length - 1;
            {
                // insert missing to pending
                std::lock_guard<std::mutex> l(mMtx);
                mPendingRanges[active.parentTaskId].push_front(Range{missStart, missEnd});
                // cancel this
                mActiveByPtrP2p.erase(raw);
                auto& vec = mDlByTaskP2p[active.parentTaskId];
                vec.erase(
                    std::remove_if(
                        vec.begin(),
                        vec.end(),
                        [raw](const std::shared_ptr<dcdn::util::DownloaderTask>& p) { return p.get() == raw; }),
                    vec.end());
            }
            logWarn << "[P2P]子任务 " << active.index << " 短读, got=" << got << " < need=" << active.length
                    << ", 回填缺口: [" << (active.offset + got) << ", " << (active.offset + active.length - 1) << "]";
        } else {
            uint64_t actualGot = got;
            {
                std::lock_guard<std::mutex> l(mMtx);
                mActiveByPtrP2p.erase(raw);
                auto& vec = mDlByTaskP2p[active.parentTaskId];
                vec.erase(
                    std::remove_if(
                        vec.begin(),
                        vec.end(),
                        [raw](const std::shared_ptr<dcdn::util::DownloaderTask>& p) { return p.get() == raw; }),
                    vec.end());
            }
            if (!active.isProbe && active.length > 0) {
                logInfo << "[P2P]子任务 " << active.index << " 结束, start: " << active.offset
                        << ", end: " << (active.offset + active.length - 1) << ", actually got: " << actualGot
                        << ", expect length: " << active.length << (endByLength && !isEnd ? " (size-guard)" : "");
            } else {
                logInfo << "[P2P]子任务 " << active.index << " 结束 (probe)";
            }
        }

        // —— 续排一个 pending（先看是否暂停/取消）
        bool paused = false, cancelled = false;
        {
            std::lock_guard<std::mutex> lk(mTasksMutex);
            auto it = mTasks.find(active.parentTaskId);
            if (it != mTasks.end()) {
                paused = it->second.Paused.load();
                cancelled = it->second.Cancelled.load();
            }
        }
        if (!paused && !cancelled) {
            Range r;
            bool hasNext = false;
            size_t idxNext = 0;
            {
                std::lock_guard<std::mutex> l(mMtx);
                auto& q = mPendingRanges[active.parentTaskId];
                if (!q.empty()) {
                    r = q.front();
                    q.pop_front();
                    idxNext = mNextRangeIdx[active.parentTaskId]++;
                    hasNext = true;
                }
            }
            if (hasNext) {
                logDebug << "[P2P][Re-queue] pending: " << r.start << "-" << r.end << ", idxNext: " << idxNext;
                auto succ = p2pQueryPeersAsync_(active.parentTaskId, r.start, r.end);
                if (!succ) {
                    logWarn << "[P2P][Re-queue] p2pQueryPeersAsync failed" << ", taskId: " << active.parentTaskId
                            << ", offset: " << r.start;
                }
            }
        }

        bool hasRunning = false, hasPending = false, hasQueryOnFlight = false;
        {
            std::lock_guard<std::mutex> l(mMtx);
            auto itD = mDlByTaskP2p.find(active.parentTaskId);
            hasRunning = (itD != mDlByTaskP2p.end() && !itD->second.empty());
            auto itP = mPendingRanges.find(active.parentTaskId);
            hasPending = (itP != mPendingRanges.end() && !itP->second.empty());

            auto it = p2pStates_.find(active.parentTaskId);
            if (it != p2pStates_.end()) {
                const auto& inner = it->second;
                hasQueryOnFlight =
                    std::any_of(inner.begin(), inner.end(), [](const auto& kv) { return kv.second.queryInFlight; });
            }
        }

        if (!hasRunning && !hasPending && !hasQueryOnFlight) {
            maybeFinalizeTask(active.parentTaskId);
        }
    }

    if (active.parentTaskId != 0) {
        maybeFinalizeTask(active.parentTaskId);
    }
}

// ============ 配置接口 ============

void DownloadManager::SetStrategy(DownloadStrategy strategy)
{
    // mStrategy = strategy;
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

uint64_t DownloadManager::AddDownloadTask(
    const std::string& url,
    const std::string& contentHash,
    const FileDownloadOptions& options)
{
    auto prom = std::make_shared<std::promise<uint64_t>>();
    auto fut = prom->get_future();
    auto strategy = options.Strategy;
    this->PostEvent(std::make_shared<ArgEvent<std::function<void()>>>(
        EventType::FunctionCall, [this, prom, url, contentHash, options, strategy]() {
            DownloadTask task;
            task.Id = genTaskId();
            task.Url = url;
            task.ContentHash = contentHash;
            task.TotalSize = 0;
            task.Downloaded = 0;
            task.StartTime = std::chrono::system_clock::now();
            task.LastUpdate = task.StartTime;
            task.Status = TaskStatus::Pending;

            {
                std::lock_guard<std::mutex> lk(mTasksMutex);
                mTasks[task.Id] = task;
                mTaskOptions[task.Id] = options;
            }

            // 打开父任务共享文件（随机写）
            if (!options.OutputPath.empty()) {
                auto fs =
                    std::make_shared<std::fstream>(options.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
                if (!fs->is_open()) {
                    std::ofstream create(options.OutputPath, std::ios::binary);
                    create.close();
                    fs->open(options.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
                }
                if (fs->is_open()) {
                    std::lock_guard<std::mutex> l(mMtx);
                    mParentFiles[task.Id] = fs;
                }
            }

            if (strategy == DownloadStrategy::HTTP_ONLY) {
                const bool wantRange = options.HasRange;
                const size_t userStart = options.RangeStart;
                const bool endKnown = options.HasRange && options.RangeEnd != SIZE_MAX;

                if (wantRange && endKnown) {
                    const size_t length = (options.RangeEnd >= userStart) ? (options.RangeEnd - userStart + 1) : 0;
                    if (length == 0) {
                        std::lock_guard<std::mutex> lk(mTasksMutex);
                        auto itT = mTasks.find(task.Id);
                        if (itT != mTasks.end())
                            itT->second.Status = TaskStatus::Failed;
                        prom->set_value(task.Id);
                        return;
                    }
                    {
                        std::lock_guard<std::mutex> lk(mTasksMutex);
                        mTasks[task.Id].TotalSize = length;
                        mTasks[task.Id].Status = TaskStatus::Running;
                    }
                    splitTask(task.Id, length, userStart);
                    prom->set_value(task.Id);
                    return;
                }

                // (B) 未知 end 或整文件：需要 Content-Length（probe）
                dcdn::util::HttpDownloaderTaskOption opt;
                opt.Request = std::make_shared<dcdn::util::HttpRequest>(url);
                opt.Start = 0; // 探测
                opt.Notify = &DownloadManager::coreNotifyCallbackHttp;
                opt.Receiver = this;

                auto probe = mHttpDownloader->CreateTask(&opt);
                if (!probe) {
                    std::lock_guard<std::mutex> lk(mTasksMutex);
                    auto itT = mTasks.find(task.Id);
                    if (itT != mTasks.end())
                        itT->second.Status = TaskStatus::Failed;
                    prom->set_value(task.Id);
                    return;
                }

                {
                    std::lock_guard<std::mutex> l(mMtx);
                    ActiveSubTask a;
                    a.parentTaskId = task.Id;
                    a.downloader = probe;
                    a.offset = 0;
                    a.length = 0;
                    a.index = 0;
                    a.isProbe = true;
                    a.lastTouched = std::chrono::steady_clock::now();

                    auto itF = mParentFiles.find(task.Id);
                    if (itF != mParentFiles.end())
                        a.file = itF->second;

                    mActiveByPtrHttp[probe.get()] = std::move(a);
                    mDlByTaskHttp[task.Id].push_back(probe);
                }
                mHttpDownloader->AddTask(probe);
                logInfo << "[Launch] probe Task " << 0;

                {
                    std::lock_guard<std::mutex> lk(mTasksMutex);
                    auto itT = mTasks.find(task.Id);
                    if (itT != mTasks.end())
                        itT->second.Status = TaskStatus::Running;
                }
                prom->set_value(task.Id);
                return;
            } else if (strategy == DownloadStrategy::P2P_ONLY) {
                const bool wantRange = options.HasRange;
                const size_t userStart = options.RangeStart;
                const bool endKnown = options.HasRange && options.RangeEnd != SIZE_MAX;

                if (wantRange && endKnown) {
                    const size_t length = (options.RangeEnd >= userStart) ? (options.RangeEnd - userStart + 1) : 0;
                    if (length == 0) {
                        std::lock_guard<std::mutex> lk(mTasksMutex);
                        auto itT = mTasks.find(task.Id);
                        if (itT != mTasks.end())
                            itT->second.Status = TaskStatus::Failed;
                        prom->set_value(task.Id);
                        return;
                    }
                    {
                        std::lock_guard<std::mutex> lk(mTasksMutex);
                        mTasks[task.Id].TotalSize = length;
                        auto itT = mTasks.find(task.Id);
                        if (itT != mTasks.end())
                            itT->second.Status = TaskStatus::Running;
                    }
                }
                startP2pDownload(task.Id);
                prom->set_value(task.Id);
                return;
            } else { // HYBRID（place holder）
                {
                    std::lock_guard<std::mutex> lk(mTasksMutex);
                    auto itT = mTasks.find(task.Id);
                    if (itT != mTasks.end())
                        itT->second.Status = TaskStatus::Pending;
                }
                startHybridDownload(task.Id); // TODO
                prom->set_value(task.Id);
                return;
            }
        }));
    // TODO: return task id without wait
    return fut.get();
}

bool DownloadManager::CancelDownloadTask(uint64_t taskId)
{
    auto prom = std::make_shared<std::promise<bool>>();
    auto fut = prom->get_future();

    this->PostEvent(std::make_shared<ArgEvent<std::function<void()>>>(EventType::FunctionCall, [this, prom, taskId]() {
        {
            std::lock_guard<std::mutex> lk(mTasksMutex);
            auto it = mTasks.find(taskId);
            if (it == mTasks.end()) {
                prom->set_value(false);
                return;
            }
            it->second.Cancelled = true;
            it->second.Status = TaskStatus::Cancelled;
        }

        // 取消所有子任务 & 标记 raw 已取消
        {
            std::lock_guard<std::mutex> l(mMtx);
            auto itVec = mDlByTaskHttp.find(taskId);
            if (itVec != mDlByTaskHttp.end()) {
                for (auto& sp : itVec->second) {
                    if (!sp)
                        continue;
                    mHttpDownloader->CancelTask(sp);
                    mCancelledRawHttp.insert(sp.get());
                    mActiveByPtrHttp.erase(sp.get());
                }
                itVec->second.clear();
            }
            // 清空 pending
            mPendingRanges[taskId].clear();
            // 丢弃事件队列里属于该任务的事件
            for (auto it2 = mDlEventsHttp.begin(); it2 != mDlEventsHttp.end();) {
                if (mCancelledRawHttp.count(it2->first))
                    it2 = mDlEventsHttp.erase(it2);
                else
                    ++it2;
            }
            // 删除输出文件（如果指定了文件路径）
            FileDownloadOptions opts;
            {
                std::lock_guard<std::mutex> lk2(mTasksMutex);
                auto itOpt = mTaskOptions.find(taskId);
                if (itOpt != mTaskOptions.end())
                    opts = itOpt->second;
            }
            if (!opts.OutputPath.empty()) {
                mParentFiles.erase(taskId);
                std::remove(opts.OutputPath.c_str());
            }
        }
        prom->set_value(true);
    }));
    return fut.get();
}

bool DownloadManager::PauseDownloadTask(uint64_t taskId)
{
    auto prom = std::make_shared<std::promise<bool>>();
    auto fut = prom->get_future();

    this->PostEvent(std::make_shared<ArgEvent<std::function<void()>>>(EventType::FunctionCall, [this, prom, taskId]() {
        {
            std::lock_guard<std::mutex> lk(mTasksMutex);
            auto it = mTasks.find(taskId);
            if (it == mTasks.end()) {
                prom->set_value(false);
                return;
            }
            it->second.Paused = true;
            it->second.Status = TaskStatus::Paused;
        }
        {
            std::lock_guard<std::mutex> l(mMtx);
            auto itVec = mDlByTaskHttp.find(taskId);
            if (itVec != mDlByTaskHttp.end()) {
                for (auto& sp : itVec->second) {
                    if (sp)
                        mHttpDownloader->PauseTask(sp);
                }
            }
        }
        prom->set_value(true);
    }));
    return fut.get();
}

bool DownloadManager::ResumeDownloadTask(uint64_t taskId)
{
    auto prom = std::make_shared<std::promise<bool>>();
    auto fut = prom->get_future();

    this->PostEvent(std::make_shared<ArgEvent<std::function<void()>>>(EventType::FunctionCall, [this, prom, taskId]() {
        {
            std::lock_guard<std::mutex> lk(mTasksMutex);
            auto it = mTasks.find(taskId);
            if (it == mTasks.end()) {
                prom->set_value(false);
                return;
            }
            it->second.Paused = false;
            it->second.Status = TaskStatus::Running;
        }
        {
            std::lock_guard<std::mutex> l(mMtx);
            auto itVec = mDlByTaskHttp.find(taskId);
            if (itVec != mDlByTaskHttp.end()) {
                for (auto& sp : itVec->second) {
                    if (!sp)
                        continue;
                    if (sp->IsEnd())
                        continue;
                    logInfo << "resume task: " << sp.get();
                    if (sp)
                        mHttpDownloader->ResumeTask(sp);
                }
            }
        }
        prom->set_value(true);
    }));
    return fut.get();
}

// ============ 状态查询与回调注册 ============
DownloadTask DownloadManager::GetTaskStatus(uint64_t taskId) const
{
    std::lock_guard<std::mutex> l(mTasksMutex);
    auto it = mTasks.find(taskId);
    if (it != mTasks.end())
        return it->second;
    return {};
}

std::vector<DownloadTask> DownloadManager::GetAllTasks() const
{
    std::lock_guard<std::mutex> l(mTasksMutex);
    std::vector<DownloadTask> out;
    out.reserve(mTasks.size());
    for (auto& kv : mTasks)
        out.push_back(kv.second);
    return out;
}

double DownloadManager::GetOverallSpeed() const
{
    std::lock_guard<std::mutex> l(mTasksMutex);
    double sum = 0.0;
    for (auto& kv : mTasks)
        sum += kv.second.Speed;
    return sum;
}

void DownloadManager::SetBufferReadyCallback(uint64_t taskId, BufferReadyCallback callback)
{
    std::lock_guard<std::mutex> l(mTasksMutex);
    mBufferCallbacks[taskId] = std::move(callback);
}
void DownloadManager::RemoveBufferReadyCallback(uint64_t taskId)
{
    std::lock_guard<std::mutex> l(mTasksMutex);
    mBufferCallbacks.erase(taskId);
}
std::vector<std::pair<size_t, size_t>> DownloadManager::GetAvailableRanges(uint64_t taskId) const
{
    std::lock_guard<std::mutex> l(mTasksMutex);
    auto it = mTasks.find(taskId);
    if (it != mTasks.end())
        return it->second.CompletedRanges;
    return {};
}

void DownloadManager::SetHttpBandwidthRatio(float ratio)
{
    mHttpBandwidthRatio = ratio;
}
void DownloadManager::SetP2pBandwidthRatio(float ratio)
{
    mP2pBandwidthRatio = ratio;
}

// ============ 进度计算与通知 ============
void DownloadManager::updateTaskProgress(uint64_t taskId, size_t downloaded)
{
    auto now = std::chrono::system_clock::now();
    bool reachedEnd = false;

    {
        std::lock_guard<std::mutex> l(mTasksMutex);
        auto it = mTasks.find(taskId);
        if (it == mTasks.end())
            return;

        it->second.Downloaded += downloaded;
        if (it->second.TotalSize > 0 && it->second.Downloaded >= it->second.TotalSize) {
            it->second.Downloaded = it->second.TotalSize; // 防御
            reachedEnd = true;
            if (it->second.Downloaded > it->second.TotalSize) {
                logWarn << "Task " << taskId << " downloaded more than expected: " << it->second.Downloaded << " > "
                        << it->second.TotalSize;
            }
        }
        it->second.LastUpdate = now;
        calculateSpeedLocked(it->second, now);
    }

    if (reachedEnd) {
        maybeFinalizeTask(taskId);
    }
}

void DownloadManager::calculateSpeedLocked(DownloadTask& t, std::chrono::system_clock::time_point now)
{
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now - t.StartTime).count();
    t.Speed = (seconds > 0) ? (static_cast<double>(t.Downloaded) / static_cast<double>(seconds)) : 0.0;
}

void DownloadManager::notifyBufferReady(const uint64_t& taskId, size_t start, size_t end)
{
    std::lock_guard<std::mutex> l(mTasksMutex);
    auto it = mBufferCallbacks.find(taskId);
    if (it != mBufferCallbacks.end()) {
        auto cb = it->second;
        if (cb)
            cb(taskId, start, end);
    }
}

// ============ 预留策略入口（未实现） ============
void DownloadManager::startHttpDownload(const uint64_t& taskId)
{
    (void)taskId;
}

void DownloadManager::startP2pDownload(const uint64_t& taskId)
{
    FileDownloadOptions opt;
    DownloadTask t;
    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto it = mTasks.find(taskId);
        if (it == mTasks.end())
            return;
        t = it->second;
        auto itOpt = mTaskOptions.find(taskId);
        if (itOpt != mTaskOptions.end())
            opt = itOpt->second;
    }

    logDebug << "P2P peers 查询开始 taskId=" << taskId << "offset =" << (opt.HasRange ? opt.RangeStart : 0)
             << std::endl;
    auto start = (opt.HasRange ? opt.RangeStart : 0);
    auto end = (opt.HasRange ? opt.RangeEnd : 0);
    if (!p2pQueryPeersAsync_(taskId, start, end)) {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto it = mTasks.find(taskId);
        if (it != mTasks.end()) {
            logError << "P2P peers 查询失败 taskId=" << taskId << std::endl;
            // TODO: NEED retry
            it->second.Status = TaskStatus::Pending;
        }
    }
}

void DownloadManager::startHybridDownload(const uint64_t& taskId)
{
    (void)taskId;
}

// ============ 切分 & 调度（HTTP_ONLY） ============
void DownloadManager::splitTask(uint64_t taskId, size_t totalSize, size_t baseOffset)
{
    FileDownloadOptions opts;
    std::string url;
    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto it = mTasks.find(taskId);
        if (it == mTasks.end())
            return;
        url = it->second.Url;
        auto itOpt = mTaskOptions.find(taskId);
        if (itOpt != mTaskOptions.end())
            opts = itOpt->second;
    }

    // 预分配/截断文件到正确大小（相对写：长度就是 totalSize）
    if (!opts.OutputPath.empty() && totalSize > 0) {
        {
            std::ofstream reset(opts.OutputPath, std::ios::binary | std::ios::trunc);
        }
        auto fs = std::make_shared<std::fstream>(opts.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
        if (!fs->is_open()) {
            std::ofstream create(opts.OutputPath, std::ios::binary | std::ios::trunc);
            create.close();
            fs->open(opts.OutputPath, std::ios::in | std::ios::out | std::ios::binary);
        }
        if (fs->is_open()) {
            try {
                fs->seekp(static_cast<std::streamoff>(totalSize - 1), std::ios::beg);
                char z = 0;
                fs->write(&z, 1);
                fs->flush();
            } catch (...) {
            }
            std::lock_guard<std::mutex> l(mMtx);
            mParentFiles[taskId] = fs;
        }
    }

    // 若有探测中的 probe，需要取消并统计已拉到的字节
    std::shared_ptr<dcdn::util::DownloaderTask> probeToCancel;
    size_t already = 0;
    {
        std::lock_guard<std::mutex> l(mMtx);
        auto itD = mDlByTaskHttp.find(taskId);
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
    if (probeToCancel) {
        already = probeToCancel->Size();
        mHttpDownloader->CancelTask(probeToCancel);

        std::lock_guard<std::mutex> l(mMtx);
        mActiveByPtrHttp.erase(probeToCancel.get());
        auto& vec = mDlByTaskHttp[taskId];
        vec.erase(std::remove(vec.begin(), vec.end(), probeToCancel), vec.end());
        mCancelledRawHttp.insert(probeToCancel.get());
        mDlEventsHttp.erase(probeToCancel.get());
        logInfo << "http probe 已取消";
    }

    // 需要下载的绝对区间为 [baseOffset, baseOffset + totalSize - 1]
    const size_t absBegin = baseOffset;
    const size_t absEnd = baseOffset + totalSize - 1;

    // 用 probe 已有字节前移起点，避免重复下载 / 计数错位
    // 排除 probe 已下载部分
    size_t rangeStart = absBegin + already;
    if (rangeStart > absEnd) {
        // probe 已经覆盖完任务（极端情况），直接 finalize
        maybeFinalizeTask(taskId);
        return;
    }

    if (already > 0) {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto itT = mTasks.find(taskId);
        if (itT != mTasks.end()) {
            itT->second.LastUpdate = std::chrono::system_clock::now();
            calculateSpeedLocked(itT->second, itT->second.LastUpdate);
        }
    }

    // 生成 ranges
    const size_t chunk = (opts.ChunkSize ? opts.ChunkSize : (1u << 20)); // 默认 1MB
    std::vector<Range> ranges;
    for (size_t pos = rangeStart; pos <= absEnd;) {
        size_t rEnd = std::min(pos + chunk - 1, absEnd);
        ranges.push_back({pos, rEnd});
        std::cout << "range: [" << pos << ", " << rEnd << "]" << std::endl;
        if (rEnd == absEnd)
            break;
        pos = rEnd + 1;
    }

    if (ranges.empty()) {
        maybeFinalizeTask(taskId);
        return;
    }

    logInfo << "[Split] Task " << taskId << " split into " << ranges.size()
            << " ranges, total bytes = " << (absEnd - rangeStart + 1) << " chunkSize = " << chunk;

    // 并发启动
    const size_t canLaunch = std::min(ranges.size(), mMaxConcurrent);

    std::shared_ptr<std::fstream> parentFile;
    {
        std::lock_guard<std::mutex> l(mMtx);
        auto itF = mParentFiles.find(taskId);
        if (itF != mParentFiles.end())
            parentFile = itF->second;
        mNextRangeIdx[taskId] = 0;
    }

    // 若任务已暂停/取消，则不启动分片，直接塞入 pending 等待
    bool paused = false, cancelled = false;
    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto itT = mTasks.find(taskId);
        if (itT != mTasks.end()) {
            paused = itT->second.Paused.load();
            cancelled = itT->second.Cancelled.load();
        }
    }

    if (paused || cancelled || canLaunch == 0) {
        std::lock_guard<std::mutex> l(mMtx);
        auto& q = mPendingRanges[taskId];
        for (auto& r : ranges)
            q.push_back(r);
        maybeFinalizeTask(taskId);
        return;
    }

    for (size_t i = 0; i < canLaunch; ++i) {
        auto r = ranges[i];
        dcdn::util::HttpDownloaderTaskOption opt;
        opt.Request = std::make_shared<dcdn::util::HttpRequest>(url);
        opt.Start = r.start;
        opt.End = r.end;
        opt.Notify = &DownloadManager::coreNotifyCallbackHttp;
        opt.Receiver = this;

        auto sub = mHttpDownloader->CreateTask(&opt);
        if (!sub)
            continue;

        {
            std::lock_guard<std::mutex> l(mMtx);
            ActiveSubTask st;
            st.parentTaskId = taskId;
            st.offset = r.start;
            st.length = r.end - r.start + 1;
            st.index = static_cast<int>(i);
            st.downloader = sub;
            st.file = parentFile;
            st.isProbe = false;
            st.lastTouched = std::chrono::steady_clock::now();

            mActiveByPtrHttp[sub.get()] = std::move(st);
            mDlByTaskHttp[taskId].push_back(sub);
            mNextRangeIdx[taskId] = i + 1;
        }

        mHttpDownloader->AddTask(sub);
        logInfo << "[Launch] Task " << i << " range " << r.start << "-" << r.end;
    }

    // 加入 pending
    if (ranges.size() > canLaunch) {
        std::lock_guard<std::mutex> l(mMtx);
        auto& q = mPendingRanges[taskId];
        for (size_t i = canLaunch; i < ranges.size(); ++i)
            q.push_back(ranges[i]);
    }

    // 兜底 finalize（maybeFinalizeTask 自幂等）
    maybeFinalizeTask(taskId);
}

// ============ 幂等的完成判定 ============
bool DownloadManager::maybeFinalizeTask(uint64_t taskId)
{
    // TODO: refactor, duplicate code in if-else clause
    auto opts = mTaskOptions.find(taskId);
    if (opts == mTaskOptions.end())
        return false;

    if (opts->second.Strategy == DownloadStrategy::HTTP_ONLY) {
        bool hasRunning = false;
        bool hasPending = false;
        {
            std::lock_guard<std::mutex> l(mMtx);
            auto itD = mDlByTaskHttp.find(taskId);
            hasRunning = (itD != mDlByTaskHttp.end() && !itD->second.empty());
            auto itP = mPendingRanges.find(taskId);
            hasPending = (itP != mPendingRanges.end() && !itP->second.empty());
        }
        if (hasRunning || hasPending)
            return false;

        {
            std::lock_guard<std::mutex> lk(mTasksMutex);
            auto itT = mTasks.find(taskId);
            if (itT == mTasks.end())
                return false;

            auto& t = itT->second;
            if (t.Cancelled) {
                t.Status = TaskStatus::Cancelled;
                return true;
            }
            // —— 兜底对齐：既然没有在跑/待排队的分片，说明下载流程上的“工作”已完成
            // 若计数略小于总长（常见于 probe + 预分配/短读边界），直接对齐。
            if (t.TotalSize > 0 && t.Downloaded < t.TotalSize) {
                logWarn << "任务" << taskId << "下载完成(无running 和pending),但计数不对齐，修正为" << t.TotalSize;
                t.Downloaded = t.TotalSize;
            }
            t.Status = TaskStatus::Completed;
        }
        // 幂等清理
        {
            std::lock_guard<std::mutex> l(mMtx);
            mDlByTaskHttp.erase(taskId);
            mPendingRanges.erase(taskId);
            mParentFiles.erase(taskId);
        }
    } else if (opts->second.Strategy == DownloadStrategy::P2P_ONLY) {
        bool hasRunning = false;
        bool hasPending = false;
        {
            std::lock_guard<std::mutex> l(mMtx);
            auto itD = mDlByTaskP2p.find(taskId);
            hasRunning = (itD != mDlByTaskP2p.end() && !itD->second.empty());
            auto itP = mPendingRanges.find(taskId);
            hasPending = (itP != mPendingRanges.end() && !itP->second.empty());
        }
        if (hasRunning || hasPending)
            return false;

        {
            std::lock_guard<std::mutex> lk(mTasksMutex);
            auto itT = mTasks.find(taskId);
            if (itT == mTasks.end())
                return false;

            auto& t = itT->second;
            if (t.Cancelled) {
                t.Status = TaskStatus::Cancelled;
                return true;
            }
            // —— 兜底对齐：既然没有在跑/待排队的分片，说明下载流程上的“工作”已完成
            // 若计数略小于总长（常见于 probe + 预分配/短读边界），直接对齐。
            if (t.TotalSize > 0 && t.Downloaded < t.TotalSize) {
                logWarn << "任务" << taskId << "下载完成(无running 和pending),但计数不对齐，修正为" << t.TotalSize;
                t.Downloaded = t.TotalSize;
            }
            t.Status = TaskStatus::Completed;
        }
        // 幂等清理
        {
            std::lock_guard<std::mutex> l(mMtx);
            mDlByTaskP2p.erase(taskId);
            mPendingRanges.erase(taskId);
            mParentFiles.erase(taskId);
        }
    }

    return true;
}

bool DownloadManager::p2pQueryPeersAsync_(uint64_t taskId, size_t start, size_t end)
{
    logDebug << "enter p2pQueryPeersAsync_ taskId=" << taskId << " offset=" << start << std::endl;
    // TODO: need some refactor, the outer caller has the same code snippet
    DownloadTask t;
    FileDownloadOptions opt;
    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto it = mTasks.find(taskId);
        if (it == mTasks.end())
            return false;
        t = it->second;
        auto itOpt = mTaskOptions.find(taskId);
        if (itOpt != mTaskOptions.end())
            opt = itOpt->second;
    }

    // nlohmann::json arg;
    auto arg = BuildQueryPeersRequest("192.168.1.1", t.Url, t.ContentHash, std::to_string(start), "", true);

    void* reqId = nullptr;
    auto succ = [this, taskId, start, end](nlohmann::json& res) { this->onP2PPeerQuerySuccess_(taskId, res, start, end); };
    auto fail = [this, taskId, start](int code) { this->onP2PPeerQueryFail_(taskId, code, start); };

    auto mm = MainManager::Singlet();
    if (mm == nullptr) {
        logError << "[P2P] MainManager::Singlet() is null";
        return false;
    }
    int rc = mm->AsyncApiPostWithToken(&reqId, kApiPeersByHashOrUrl, arg, this, succ, fail);

    // rc 1 is success?
    if (rc != 1) {
        logWarn << "[P2P] async query failed rc=" << rc << " taskId=" << taskId;
        return false;
    }

    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        p2pStates_[taskId][start].queryReqId = reqId;
        p2pStates_[taskId][start].queryInFlight = true;
        p2pStates_[taskId][start].offset = start;
        p2pStates_[taskId][start].lastQueryErr = 0;
    }
    return true;
}

void DownloadManager::onP2PPeerQueryFail_(uint64_t taskId, int errCode, size_t start)
{
    logError << "[P2P] peers 查询失败 err=" << errCode << " taskId=" << taskId << std::endl;

    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        p2pStates_[taskId][start].queryInFlight = false;
        p2pStates_[taskId][start].queryDone = true;
        p2pStates_[taskId][start].lastQueryErr = errCode;

        auto it = mTasks.find(taskId);
        if (it != mTasks.end()) {
            // P2P_ONLY模式简单处理：标记失败；如果在 HYBRID 里回退 HTTP，在此触发 HTTP 流程
            it->second.Status = TaskStatus::Failed;
        }
    }
}

void DownloadManager::onP2PPeerQuerySuccess_(uint64_t taskId, nlohmann::json& res, size_t start, size_t end)
{
    std::cout << "raw query result:"<< res.dump();
    logDebug << "query peers success taskId=" << taskId << " offset=" << start << std::endl;
    std::vector<PeerChunk> plan;
    // total size of the file (if we request a block, it means the size of the file that the block belong to)
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
                pc.end = std::stoull(p.value("end", "0"));
                plan.emplace_back(std::move(pc)); 
            }
        }
    } catch (const std::exception& ex) {
        logWarn << "[P2P] parse response error: " << ex.what() << " taskId=" << taskId;
        onP2PPeerQueryFail_(taskId, -2, start);
        return;
    }

    logDebug << "onP2PPeerQuerySuccess_ taskId=" << taskId << " offset =" << start << " plan size=" << plan.size();

    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto it = mTasks.find(taskId);
        if (it == mTasks.end())
            return;

        if (it->second.TotalSize == 0 && totalSizeFromApi > 0) {
            it->second.TotalSize = totalSizeFromApi;
        }

        p2pStates_[taskId][start].queryInFlight = false;
        p2pStates_[taskId][start].queryDone = true;
        p2pStates_[taskId][start].lastQueryErr = 0;
        p2pStates_[taskId][start].plan = plan;

        if (it->second.Status == TaskStatus::Pending) {
            it->second.Status = TaskStatus::Running;
        }
    }

    scheduleP2PChunks_(taskId, plan, start, end);
}

void DownloadManager::scheduleP2PChunks_(uint64_t taskId, const std::vector<PeerChunk>& plan, size_t start, size_t end)
{
    std::vector<PeerChunk> planO = plan;
#ifdef DEBUG_LOCAL_P2P
    planO.clear();
    planO.push_back({.peerId = "peer1", .start = 0, .end = 2097152 - 1});

    logInfo << "scheduleP2PChunks_ taskId=" << taskId << " plan size=" << planO.size();
    std::cout << "INPUT connMeta " << std::endl;
    std::string remote_sdp;
    std::string line;
    while (std::getline(std::cin, line) && !line.empty()) {
        remote_sdp += line + "\r\n";
    }
    planO.back().remoteSdp = remote_sdp;
    planO.back().remoteSdp.pop_back(); // \n
    planO.back().remoteSdp.pop_back(); // \r
#endif

    if (planO.empty()) {
        logError << "scheduleP2PChunks_ taskId=" << taskId << " plan is empty";
        return;
    }

    auto longestPlan_ = planO[0];
    for (auto& pc : planO) {
        if (pc.end > longestPlan_.end) {
            longestPlan_ = pc;
        }
    }

    FileDownloadOptions opts;
    DownloadTask t;
    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto itT = mTasks.find(taskId);
        if (itT == mTasks.end())
            return;
        t = itT->second;
        auto itOpt = mTaskOptions.find(taskId);
        if (itOpt != mTaskOptions.end())
            opts = itOpt->second;
    }

    if (t.Paused || t.Cancelled)
        return;

    size_t runningP2p = 0;
    {
        std::lock_guard<std::mutex> l(mMtx);
        auto it = mDlByTaskP2p.find(taskId);
        if (it != mDlByTaskP2p.end())
            runningP2p = it->second.size();
        if (mNextRangeIdx.find(taskId) == mNextRangeIdx.end())
            mNextRangeIdx[taskId] = 0;
    }

    PeerChunk clipped = longestPlan_;
    size_t s = start, e = std::min(longestPlan_.end, end);
    clipped.start = s;
    clipped.end = e;

    if (clipped.start > start){
        logError << "un-expected scheduleP2PChunks_ clipped.start > start";
    }else if (clipped.end < end){
        size_t missStart = clipped.end + 1;    
        size_t missEnd = end;
        std::lock_guard<std::mutex> l(mMtx);
        mPendingRanges[taskId].push_front(Range{missStart, missEnd});
        logInfo << "clipped.end < end, pending range: [" << missStart << ", " << missEnd << "]" << " previous range: [" << start << ", " << end << "]";
    }

    const size_t room = (mMaxConcurrent > runningP2p) ? (mMaxConcurrent - runningP2p) : 0;
    if (room == 0) {
        logError << "未实现的P2p并发限制分支, 请先将并发调大绕过";
        // 把 plan 全部扔进 pending，等回调/看门狗来续排
        std::lock_guard<std::mutex> l(mMtx);
        auto& q = mPendingRanges[taskId];

        logDebug << "scheduleP2PChunks_ pending ranges, taskId=" << taskId << " offset=" << start << " s=" << s
                 << " e=" << e;

        // TODO: implement
        // q.push_back(pc);
        return;
    }

    if (startOneP2PChunk_(taskId, clipped)) {
        return;
    } else {
        logError << "startOneP2PChunk_ failed taskId=" << taskId << " offset=" << start << "chunk=" << clipped.start
                 << "-" << clipped.end << "peer " << clipped.peerId << std::endl;
    }

    // 兜底 finalize
    maybeFinalizeTask(taskId);
}

bool DownloadManager::startOneP2PChunk_(uint64_t taskId, const PeerChunk& pc)
{
    // no launch
    {
        std::lock_guard<std::mutex> lk(mTasksMutex);
        auto it = mTasks.find(taskId);
        if (it == mTasks.end())
            return false;
        if (it->second.Paused || it->second.Cancelled)
            return false;
    }

    // output file fd
    std::shared_ptr<std::fstream> parentFile;
    {
        std::lock_guard<std::mutex> l(mMtx);
        auto itF = mParentFiles.find(taskId);
        if (itF != mParentFiles.end())
            parentFile = itF->second;
    }

    download::P2PDownloaderTaskOption opt;
    opt.PeerID = pc.peerId;
    opt.PeerSdp = pc.remoteSdp;
#ifdef DEBUG_LOCAL_P2P
    opt.IceUfrag = "kul9";
    opt.IcePwd = "HArM7vdA12b4f+NrSE1hMu";
    opt.ContentHash = "cd4a7faf4ed9cd3486cb08ff1dcfd040";
#else
    opt.IceUfrag = pc.iceUfrag;
    opt.IcePwd = pc.icePwd;
    opt.ContentHash = pc.hash;
#endif
    opt.Start = pc.start;
    opt.End = pc.end;
    opt.Notify = &DownloadManager::coreNotifyCallbackP2p;
    opt.Receiver = this;
    logDebug << "[P2P] CreateTask peer=" << pc.peerId << " range=" << pc.start << "-" << pc.end << " sdp = " << opt.PeerSdp << " iceUfrag = " << opt.IceUfrag << " icePwd = " << opt.IcePwd << " hash = " << opt.ContentHash;
    auto sub = mP2pDownloader->CreateTask(&opt);
    if (!sub) {
        logWarn << "[P2P] CreateTask failed peer=" << pc.peerId << " range=" << pc.start << "-" << pc.end;
        return false;
    }

    size_t idx = 0;
    {
        std::lock_guard<std::mutex> l(mMtx);
        idx = mNextRangeIdx[taskId]++;

        ActiveSubTask st;
        st.parentTaskId = taskId;
        st.offset = pc.start;
        st.length = pc.end - pc.start + 1;
        st.index = static_cast<int>(idx);
        st.downloader = sub;
        st.file = parentFile;
        st.isProbe = false;
        st.lastTouched = std::chrono::steady_clock::now();

        mActiveByPtrP2p[sub.get()] = std::move(st);
        mDlByTaskP2p[taskId].push_back(sub);
    }

    mP2pDownloader->AddTask(sub);
    logDebug << "[P2P][Launch] idx=" << idx << " range " << pc.start << "-" << pc.end << " peer=" << pc.peerId
             << " peerSdp" << pc.remoteSdp << "ufrag=" << pc.iceUfrag << "pwd=" << pc.icePwd << std::endl;
    return true;
}
