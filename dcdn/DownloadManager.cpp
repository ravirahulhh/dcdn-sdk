#include "DownloadManager.h"

#include <algorithm>
#include <chrono>
#include <cstdio> // std::remove
#include <deque>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

using namespace dcdn;

// ============ 工具：生成 64 位 TaskId ============
namespace {
static uint64_t genTaskId()
{
    static std::atomic<uint64_t> cnt{0};
    static const uint64_t base = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    return base + cnt++;
}
} // namespace

// ============ 本文件内部使用的类型 ============
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

// ============ PersistenceHelper（占位） ============
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
DownloadManager::DownloadManager(): BaseManager(nullptr)
{
    // 初始化 HTTP 引擎
    mHttpDownloader = std::make_unique<util::HttpDownloader>();
    int ret = mHttpDownloader->Init(nullptr);
    if (ret != 1) {
        std::cerr << "Warning: HttpDownloader Init returned " << ret << std::endl;
    }
    mHttpDownloader->Start();

    // 注册事件处理器：FunctionCall
    this->registerHandler(EventType::FunctionCall, &DownloadManager::handleFunctionCall);

    // 启动事件线程（后台）
    Start(true);
}

DownloadManager::~DownloadManager()
{
    if (mHttpDownloader) {
        mHttpDownloader.reset();
    }
}

// BaseManager 线程主函数：统一处理 FunctionCall 事件 + 下载器事件 + 看门狗
void DownloadManager::run()
{
    using namespace std::chrono_literals;

    auto lastReap = std::chrono::steady_clock::now();
    const auto reapInterval = 1s; // 看门狗巡检周期
    const auto staleTimeout = 8s; // 分片 N 秒无任何进展视为卡死

    while (true) {
        // 等待：若无 FunctionCall 事件，可被下载器通知（mCv）唤醒
        {
            std::unique_lock<std::mutex> lk(mMtx);
            mCv.wait_for(lk,reapInterval, [&] { return !mDlEvents.empty() || !mEvents.empty(); });
        }

        if (mDlEvents.size() == 0 && mEvents.size() == 0) {
            logError << "EventLoop: mDlEvents.size() == 0 && mEvents.size() == 0";
        }

        // 处理 1 个 FunctionCall 事件(若有)
        this->waitEvent(10ms);

        // 抽取一批 downloader 事件
        std::vector<std::shared_ptr<dcdn::util::DownloaderTask>> batch;
        {
            std::lock_guard<std::mutex> l(mMtx);
            batch.reserve(mDlEvents.size());
            for (auto& kv : mDlEvents)
                batch.push_back(kv.second);
            mDlEvents.clear();
        }
        for (auto& t : batch) {
            try {
                processDownloaderEvent(t);
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
                snapshot.reserve(mActiveByPtr.size());
                for (auto& kv : mActiveByPtr)
                    snapshot.push_back(kv.second);
            }

            for (auto& st : snapshot) {
                if (st.isProbe || st.length == 0)
                    continue;

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
                    logWarn << "看门狗扫描：子任务 " << st.index << " 无任何进展，miss[" << missStart << ", " << missEnd << "]";
                    // 回填 + 续排（注意：续排前再次读 paused/cancelled）
                    {
                        std::lock_guard<std::mutex> l(mMtx);
                        auto raw = st.downloader.get();

                        mActiveByPtr.erase(raw);
                        auto& vec = mDlByTask[st.parentTaskId];
                        vec.erase(
                            std::remove_if(
                                vec.begin(),
                                vec.end(),
                                [raw](const std::shared_ptr<dcdn::util::DownloaderTask>& p) { return p.get() == raw; }),
                            vec.end());

                        mPendingRanges[st.parentTaskId].push_front(Range{missStart, missEnd});
                        mCancelledRaw.insert(raw);
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
                            opt.Notify = &DownloadManager::coreNotifyCallback;
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

                                    mActiveByPtr[sub.get()] = std::move(st2);
                                    mDlByTask[st.parentTaskId].push_back(sub);
                                }
                                mHttpDownloader->AddTask(sub);
                                logWarn << "[watchdog] 子任务无进展，回填续排原来index任务：" << st.index << ", 回填range: " << r.start << "-" << r.end;
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
                    toCheck.reserve(mDlByTask.size());
                    for (auto& kv : mDlByTask)
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

// ============ 统一 downloader 通知入口（HttpDownloader 使用） ============
void DownloadManager::coreNotifyCallback(std::shared_ptr<dcdn::util::DownloaderTask> task, void* receiver)
{
    if (!task || !receiver)
        return;
    auto* self = static_cast<DownloadManager*>(receiver);
    {
        std::lock_guard<std::mutex> g(self->mMtx);
        if (self->mCancelledRaw.count(task.get()))
            return; // 丢弃取消后的迟到回调
        self->mDlEvents[task.get()] = std::move(task); // 去重：同一个 raw 只保留一个
    }
    self->mCv.notify_all();
}

// 包装（预留）
void DownloadManager::onDownloaderNotify(std::shared_ptr<dcdn::util::DownloaderTask> task)
{
    (void)task;
}

// 实际处理一个 downloader 事件（读取 -> 写入 -> 进度/续排/完成判定）
void DownloadManager::processDownloaderEvent(std::shared_ptr<dcdn::util::DownloaderTask> ev)
{
    auto* raw = ev.get();

    // 任务取消后的迟到通知直接丢弃
    {
        std::lock_guard<std::mutex> g(mMtx);
        if (mCancelledRaw.count(raw))
            return;
    }

    // 找到 ActiveSubTask
    ActiveSubTask active;
    {
        std::lock_guard<std::mutex> g(mMtx);
        auto it = mActiveByPtr.find(raw);
        if (it == mActiveByPtr.end()) {
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
        auto it = mActiveByPtr.find(raw);
        if (it != mActiveByPtr.end()) {
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
        auto it = mActiveByPtr.find(raw);
        if (it != mActiveByPtr.end())
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
                mActiveByPtr.erase(raw);
                auto& vec = mDlByTask[active.parentTaskId];
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
                mActiveByPtr.erase(raw);
                auto& vec = mDlByTask[active.parentTaskId];
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
                    opt.Notify = &DownloadManager::coreNotifyCallback;
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

                            mActiveByPtr[sub.get()] = std::move(st);
                            mDlByTask[active.parentTaskId].push_back(sub);
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
            auto itD = mDlByTask.find(active.parentTaskId);
            hasRunning = (itD != mDlByTask.end() && !itD->second.empty());
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

// ============ 配置接口 ============
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

// ============ 任务管理（HTTP_ONLY 实现） ============
uint64_t DownloadManager::AddDownloadTask(
    const std::string& url,
    const std::string& contentHash,
    const FileDownloadOptions& options)
{
    auto prom = std::make_shared<std::promise<uint64_t>>();
    auto fut = prom->get_future();

    this->PostEvent(std::make_shared<ArgEvent<std::function<void()>>>(
        EventType::FunctionCall, [this, prom, url, contentHash, options]() {
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

            if (mStrategy == DownloadStrategy::HTTP_ONLY) {
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
                opt.Notify = &DownloadManager::coreNotifyCallback;
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

                    mActiveByPtr[probe.get()] = std::move(a);
                    mDlByTask[task.Id].push_back(probe);
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
            }

            // 其他策略暂未实现
            {
                std::lock_guard<std::mutex> lk(mTasksMutex);
                auto itT = mTasks.find(task.Id);
                if (itT != mTasks.end())
                    itT->second.Status = TaskStatus::Pending;
            }
            prom->set_value(task.Id);
        }));

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
            auto itVec = mDlByTask.find(taskId);
            if (itVec != mDlByTask.end()) {
                for (auto& sp : itVec->second) {
                    if (!sp)
                        continue;
                    mHttpDownloader->CancelTask(sp);
                    mCancelledRaw.insert(sp.get());
                    mActiveByPtr.erase(sp.get());
                }
                itVec->second.clear();
            }
            // 清空 pending
            mPendingRanges[taskId].clear();
            // 丢弃事件队列里属于该任务的事件
            for (auto it2 = mDlEvents.begin(); it2 != mDlEvents.end();) {
                if (mCancelledRaw.count(it2->first))
                    it2 = mDlEvents.erase(it2);
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
            auto itVec = mDlByTask.find(taskId);
            if (itVec != mDlByTask.end()) {
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
            auto itVec = mDlByTask.find(taskId);
            if (itVec != mDlByTask.end()) {
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
    (void)taskId;
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
        auto itD = mDlByTask.find(taskId);
        if (itD != mDlByTask.end()) {
            for (auto& sp : itD->second) {
                if (!sp)
                    continue;
                auto raw = sp.get();
                auto itActive = mActiveByPtr.find(raw);
                if (itActive != mActiveByPtr.end() && itActive->second.isProbe) {
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
        mActiveByPtr.erase(probeToCancel.get());
        auto& vec = mDlByTask[taskId];
        vec.erase(std::remove(vec.begin(), vec.end(), probeToCancel), vec.end());
        mCancelledRaw.insert(probeToCancel.get());
        mDlEvents.erase(probeToCancel.get());
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
        opt.Notify = &DownloadManager::coreNotifyCallback;
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

            mActiveByPtr[sub.get()] = std::move(st);
            mDlByTask[taskId].push_back(sub);
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
    bool hasRunning = false;
    bool hasPending = false;
    {
        std::lock_guard<std::mutex> l(mMtx);
        auto itD = mDlByTask.find(taskId);
        hasRunning = (itD != mDlByTask.end() && !itD->second.empty());
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
        mDlByTask.erase(taskId);
        mPendingRanges.erase(taskId);
        mParentFiles.erase(taskId);
    }
    return true;
}
