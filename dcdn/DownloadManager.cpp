#include "DownloadManager.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
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

namespace {

// 传输类型（为未来 P2P/HYBRID 预留）
enum class Transport
{
    HTTP,
    P2P
};

// 统一的最小子任务描述（HTTP/P2P 复用）
// all access should be in core->mtx
struct ActiveSubTask
{
    uint64_t parentTaskId = 0; // 所属主任务 id, non-zero
    Transport transport = Transport::HTTP;
    size_t offset = 0; // Range start（绝对）
    size_t length = 0; // Range 长度（end-start+1）
    int index = 0; // 该子任务在父任务的分片序号
    std::shared_ptr<dcdn::util::DownloaderTask> downloader;
    std::shared_ptr<std::fstream> file; // 父任务共享随机写文件（写入时可能需要相对偏移）
    bool isProbe = false;
    uint64_t actualGot = 0; // Edge case处理：实际读到的数据量,用于处理"短读"问题
};

// 引擎上下文（协议无关）：统一串行执行 public API + 处理 downloader 事件
struct CoreContext
{
    std::mutex mtx;
    std::condition_variable cv;

    // downloader raw 指针 -> 子任务
    std::unordered_map<dcdn::util::DownloaderTask*, ActiveSubTask> tasksByPtr;

    // 父任务 -> 当前运行中的 downloader 列表（运行中的子任务）
    std::unordered_map<uint64_t, std::vector<std::shared_ptr<dcdn::util::DownloaderTask>>> downloaderByTaskId;

    // 事件队列（去重）
    // 只存一次事件，key 为裸指针，value 持有生命周期
    std::unordered_map<dcdn::util::DownloaderTask*, std::shared_ptr<dcdn::util::DownloaderTask>> events;

    // 串行化命令队列
    std::deque<std::function<void()>> cmdQueue;

    // 父任务 -> 等待调度的 Range 队列
    struct Range
    {
        size_t start;
        size_t end;
    };
    std::unordered_map<uint64_t, std::deque<Range>> pendingRanges;
    std::unordered_map<uint64_t, size_t> nextRangeIndex;

    // 父任务 -> 共享随机写文件句柄
    std::unordered_map<uint64_t, std::shared_ptr<std::fstream>> parentFiles;

    // 已取消的 raw*（用于丢弃取消后的残留回调）
    std::unordered_set<dcdn::util::DownloaderTask*> cancelledRaw;

    std::thread worker;
    bool running = false;
};

// 全局：manager -> CoreContext
static std::unordered_map<DownloadManager*, std::unique_ptr<CoreContext>> g_core;

static CoreContext* getCore(DownloadManager* mgr)
{
    auto it = g_core.find(mgr);
    if (it == g_core.end())
        return nullptr;
    return it->second.get();
}

// 统一的 downloader notify 回调（瘦身：只入队，不写状态）
static void CoreNotifyCallback(std::shared_ptr<dcdn::util::DownloaderTask> task, void* receiver)
{
    if (!task || !receiver)
        return;
    auto* core = getCore(static_cast<DownloadManager*>(receiver));
    if (!core)
        return;
    {
        std::lock_guard<std::mutex> g(core->mtx);
        if (core->cancelledRaw.count(task.get()))
            return; // 取消后的迟到通知直接丢
        core->events[task.get()] = task;
    }
    core->cv.notify_one();
}

// 生成整数 TaskID（64位）
// 用当前时间作为 base + 递增计数，避免进程内冲突；无需跨进程唯一
static uint64_t genTaskId()
{
    static std::atomic<uint64_t> cnt{0};
    static const uint64_t base = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    return base + cnt++;
}

// 在 manager 线程同步执行（串行化 public API）
template<typename R>
static R runSyncOnCore(CoreContext* core, std::function<R()> fn)
{
    auto prom = std::make_shared<std::promise<R>>();
    auto fut = prom->get_future();
    {
        std::lock_guard<std::mutex> l(core->mtx);
        core->cmdQueue.emplace_back([prom, fn]() {
            try {
                prom->set_value(fn());
            } catch (...) {
                try {
                    prom->set_exception(std::current_exception());
                } catch (...) {
                }
            }
        });
    }
    core->cv.notify_one();
    return fut.get();
}

static void runAsyncOnCore(CoreContext* core, std::function<void()> fn)
{
    {
        std::lock_guard<std::mutex> l(core->mtx);
        core->cmdQueue.emplace_back(std::move(fn));
    }
    core->cv.notify_one();
}

} // namespace

////////////////////////
// DownloadTask 序列化 (简单实现)
////////////////////////
std::string DownloadTask::serialize() const
{
    std::ostringstream ss;
    ss << id << "|" << url << "|" << contentHash << "|" << totalSize << "|" << downloaded << "|"
       << static_cast<int>(status);
    return ss.str();
}

DownloadTask DownloadTask::deserialize(const std::string& data)
{
    DownloadTask task;
    std::istringstream ss(data);
    std::string token;
    if (!std::getline(ss, token, '|'))
        return task;
    // 解析 uint64_t id
    try {
        task.id = static_cast<uint64_t>(std::stoull(token));
    } catch (...) {
        task.id = 0;
    }
    std::getline(ss, task.url, '|');
    std::getline(ss, task.contentHash, '|');
    if (!std::getline(ss, token, '|'))
        return task;
    task.totalSize = static_cast<size_t>(std::stoull(token));
    if (!std::getline(ss, token, '|'))
        return task;
    task.downloaded = static_cast<size_t>(std::stoull(token));
    if (!std::getline(ss, token, '|'))
        return task;
    task.status = static_cast<TaskStatus>(std::stoi(token));
    return task;
}

////////////////////////
// PersistenceHelper (占位 TODO)
////////////////////////
DownloadManager::PersistenceHelper::PersistenceHelper(const std::string& dbPath)
{
    db_ = nullptr;
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

////////////////////////
// DownloadManager 构造 / 析构
////////////////////////
DownloadManager::DownloadManager()
{
    // 初始化 HTTP 引擎
    httpDownloader_ = std::make_unique<util::HttpDownloader>();
    int ret = httpDownloader_->Init(nullptr);
    if (ret != 1) {
        std::cerr << "Warning: HttpDownloader Init returned " << ret << std::endl;
    }
    httpDownloader_->Start();

    // 构建通用上下文, 并启动 worker
    auto coreUP = std::make_unique<CoreContext>();
    CoreContext* coreRaw = coreUP.get();
    coreRaw->running = true;

    g_core[this] = std::move(coreUP);

    // worker: 串行执行 cmdQueue，然后处理 events（读数据 -> 写文件(文件模式)/流回调通知(流模式) -> 更新状态）
    coreRaw->worker = std::thread([this, coreRaw]() {
        CoreContext* core = coreRaw; // 直接使用指针，不会为空

        while (true) {
            std::function<void()> cmd;
            std::shared_ptr<dcdn::util::DownloaderTask> ev;

            {
                std::unique_lock<std::mutex> l(core->mtx);
                core->cv.wait(l, [&] { return !core->cmdQueue.empty() || !core->events.empty() || !core->running; });
                if (!core->running && core->cmdQueue.empty() && core->events.empty())
                    break;

                if (!core->cmdQueue.empty()) {
                    cmd = std::move(core->cmdQueue.front());
                    core->cmdQueue.pop_front();
                } else if (!core->events.empty()) {
                    auto it = core->events.begin();
                    ev = std::move(it->second);
                    core->events.erase(it);
                }
            }

            if (cmd) {
                try {
                    cmd();
                } catch (...) {
                }
                continue;
            }
            if (!ev)
                continue;

            // task event
            auto* raw = ev.get();

            // 任务取消后的迟到通知直接丢弃
            {
                std::lock_guard<std::mutex> g(core->mtx);
                if (core->cancelledRaw.count(raw))
                    continue;
            }

            // 找到触发数据继续的事件对应的SubTask
            ActiveSubTask active;
            bool bound = false;
            {
                std::lock_guard<std::mutex> g(core->mtx);
                auto it = core->tasksByPtr.find(raw);
                if (it != core->tasksByPtr.end()) {
                    active = it->second;
                    bound = true;
                } else {
                    logWarn << "warn: event for unknown raw " << raw;
                }
            }

            // 若是 probe 子任务：尝试拿到 Content-Length 触发 split
            if (active.isProbe && active.parentTaskId != 0) {
                bool needSplit = false;
                size_t clen = 0;
                size_t baseOffset = 0;
                size_t totalLen = 0;
                FileDownloadOptions opts;

                {
                    std::lock_guard<std::mutex> lk(tasksMutex_);
                    auto it = tasks_.find(active.parentTaskId);
                    if (it != tasks_.end() && it->second.totalSize == 0) {
                        if (auto ht = dynamic_cast<dcdn::util::HttpDownloaderTask*>(raw)) {
                            clen = ht->ContentLength(); // 整个资源总长
                            auto itOpt = taskOptions_.find(active.parentTaskId);
                            if (itOpt != taskOptions_.end())
                                opts = itOpt->second;

                            if (clen > 0) {
                                if (opts.hasRange) {
                                    baseOffset = opts.rangeStart;
                                    if (opts.rangeEnd != SIZE_MAX) {
                                        totalLen = (opts.rangeEnd >= opts.rangeStart)
                                            ? (opts.rangeEnd - opts.rangeStart + 1)
                                            : 0;
                                    } else {
                                        totalLen = (baseOffset >= clen) ? 0 : (clen - baseOffset);
                                    }
                                } else {
                                    baseOffset = 0;
                                    totalLen = clen; // 整个文件
                                }

                                if (totalLen > 0) {
                                    it->second.totalSize = totalLen;
                                    needSplit = true;
                                } else {
                                    it->second.status = TaskStatus::Failed;
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
            auto buffer = raw->Read();
            size_t readSum = 0;

            FileDownloadOptions opts;
            std::shared_ptr<std::fstream> file;
            {
                std::lock_guard<std::mutex> lk(tasksMutex_);
                auto itOpt = taskOptions_.find(active.parentTaskId);
                if (itOpt != taskOptions_.end())
                    opts = itOpt->second;
            }
            {
                std::lock_guard<std::mutex> l(core->mtx);
                auto itF = core->parentFiles.find(active.parentTaskId);
                if (itF != core->parentFiles.end())
                    file = itF->second;
            }

            while (buffer) {
                size_t len = buffer->Length();
                size_t off = buffer->Offset(); // 绝对偏移

                // 计算写入偏移（相对 or 绝对）
                size_t writeOff = off;
                if (opts.hasRange && opts.writeRangeToSeparateFile) {
                    if (off >= opts.rangeStart)
                        writeOff = off - opts.rangeStart;
                    else
                        writeOff = 0; // 防御
                }

                if (file && file->good()) {
                    try {
                        file->seekp(static_cast<std::streamoff>(writeOff), std::ios::beg);
                    } catch (...) {
                    }
                    file->write(reinterpret_cast<const char*>(buffer->Data()), len);
                    file->flush();
                } else if (opts.outputStream) {
                    opts.outputStream->write(reinterpret_cast<const char*>(buffer->Data()), len);
                    opts.outputStream->flush();
                } else if (!opts.outputPath.empty()) {
                    std::fstream ofs(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary);
                    if (!ofs) {
                        std::ofstream create(opts.outputPath, std::ios::binary);
                        create.close();
                        ofs.open(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary);
                    }
                    if (ofs) {
                        ofs.seekp(static_cast<std::streamoff>(writeOff), std::ios::beg);
                        ofs.write(reinterpret_cast<const char*>(buffer->Data()), len);
                    }
                }

                if (opts.streamCallback) {
                    opts.streamCallback(reinterpret_cast<const char*>(buffer->Data()), len, off);
                }

                readSum += len;
                buffer = buffer->Next();
            }

            // debug
            auto it = core->tasksByPtr.find(raw);
            if (it != core->tasksByPtr.end()) {
                {
                    std::lock_guard<std::mutex> l(core->mtx);
                    it->second.actualGot += readSum; // update sub task in container
                }
                active.actualGot += readSum; // update the local copy
                bound = true;
            } else {
                logWarn << "warn: event for unknown raw " << raw;
            }

            if (readSum > 0 && active.parentTaskId != 0) {
                updateTaskProgress(active.parentTaskId, readSum);
                notifyBufferReady(active.parentTaskId, active.offset, active.offset + readSum);
            }

            // 收尾：确认是否结束（再次取 IsEnd 防止实现差异）
            // 兜底：非 probe 分片如果 Size() 已达到该分片长度，也视为结束（很多实现不会再额外发一次 EOF 通知）
            bool isEnd = raw->IsEnd();
            // size_t got = raw->Size();
            auto got = active.actualGot;
            bool endByLength = (!active.isProbe && active.length > 0 && got >= active.length);

            // 短读检测（只对非 probe 分片）----
            bool shortRead = (!active.isProbe && active.length > 0 && isEnd && got < active.length);

            if ((isEnd || endByLength) && active.parentTaskId != 0) {
                // 短读 -> 把未完成的尾部区间回填到 pendingRanges，然后不要把它当成"完整结束"
                if (shortRead) {
                    const size_t missStart = active.offset + got;
                    const size_t missEnd = active.offset + active.length - 1;
                    {
                        std::lock_guard<std::mutex> l(core->mtx);
                        auto& q = core->pendingRanges[active.parentTaskId];
                        q.push_front(CoreContext::Range{missStart, missEnd});
                    }
                    logWarn << "子任务 " << active.index << " 短读, got=" << got << " < need=" << active.length
                            << ", 回填缺口: [" << missStart << ", " << missEnd << "]" << std::endl;
                    // 清理 tasksByPtr/downloaderByTaskId，
                    {
                        std::lock_guard<std::mutex> l(core->mtx);
                        core->tasksByPtr.erase(raw);
                        auto& vec = core->downloaderByTaskId[active.parentTaskId];
                        vec.erase(
                            std::remove_if(
                                vec.begin(),
                                vec.end(),
                                [raw](const std::shared_ptr<dcdn::util::DownloaderTask>& p) { return p.get() == raw; }),
                            vec.end());
                    }
                    // 触发一次续排（下段的"续排"逻辑），所以这里不 return
                } else {
                    uint64_t actualGot = 0;
                    auto it = core->tasksByPtr.find(raw);
                    if (it != core->tasksByPtr.end()) {
                        actualGot = it->second.actualGot;
                        bound = true;
                    } else {
                        logWarn << "warn: event for unknown raw " << raw;
                    }
                    // 正常完整结束（IsEnd 或 size-guard 命中）
                    {
                        std::lock_guard<std::mutex> l(core->mtx);
                        core->tasksByPtr.erase(raw);
                        auto& vec = core->downloaderByTaskId[active.parentTaskId];
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
                                << ", expect length: " << active.length
                                << (endByLength && !isEnd ? " (size-guard)" : "") << std::endl;
                    } else {
                        logInfo << "子任务 " << active.index << " 结束 (probe)" << std::endl;
                    }

                    // debug
                    if (actualGot != active.length) {
                        logInfo << "####子任务 " << active.index << " 不完整结束, actually got: " << actualGot
                                << ", expect length: " << active.length << " isEnd flag : " << isEnd << std::endl;
                    }
                }

                // 统一的"续排"逻辑（不论短读回填还是完整结束，都尝试拉起下一个 pending）
                {
                    std::lock_guard<std::mutex> l(core->mtx);
                    auto itP = core->pendingRanges.find(active.parentTaskId);
                    if (itP != core->pendingRanges.end() && !itP->second.empty()) {
                        auto r = itP->second.front();
                        itP->second.pop_front();
                        size_t idx = core->nextRangeIndex[active.parentTaskId]++;

                        std::string urlLocal;
                        {
                            std::lock_guard<std::mutex> lk(tasksMutex_);
                            auto itT = tasks_.find(active.parentTaskId);
                            if (itT != tasks_.end()) {
                                urlLocal = itT->second.url;

                                dcdn::util::HttpDownloaderTaskOption opt;
                                opt.Request = std::make_shared<dcdn::util::HttpRequest>(urlLocal);
                                opt.Start = r.start;
                                opt.End = r.end;
                                opt.Notify = CoreNotifyCallback;
                                opt.Receiver = this;

                                auto sub = httpDownloader_->CreateTask(&opt);
                                if (sub) {
                                    std::shared_ptr<std::fstream> f;
                                    auto itF = core->parentFiles.find(active.parentTaskId);
                                    if (itF != core->parentFiles.end())
                                        f = itF->second;

                                    ActiveSubTask st;
                                    st.parentTaskId = active.parentTaskId;
                                    st.transport = Transport::HTTP;
                                    st.offset = r.start;
                                    st.length = r.end - r.start + 1;
                                    st.index = static_cast<int>(idx);
                                    st.downloader = sub;
                                    st.file = f;
                                    st.isProbe = false;

                                    core->tasksByPtr[sub.get()] = std::move(st);
                                    core->downloaderByTaskId[active.parentTaskId].push_back(sub);

                                    httpDownloader_->AddTask(sub);
                                    logInfo << "子任务 " << idx << " 续排, start: " << r.start << ", end: " << r.end
                                            << std::endl;
                                } else {
                                    logInfo << "ERROR 创建子任务失败" << std::endl;
                                }
                            }
                        }
                    }
                }

                // 无运行子任务且无 pending -> 幂等 finalize
                bool hasRunning = false, hasPending = false;
                {
                    std::lock_guard<std::mutex> l(core->mtx);
                    auto itD = core->downloaderByTaskId.find(active.parentTaskId);
                    hasRunning = (itD != core->downloaderByTaskId.end() && !itD->second.empty());
                    auto itP = core->pendingRanges.find(active.parentTaskId);
                    hasPending = (itP != core->pendingRanges.end() && !itP->second.empty());
                }
                if (!hasRunning && !hasPending) {
                    maybeFinalizeTask_(active.parentTaskId);
                }
            }

        } // while
    });
}

DownloadManager::~DownloadManager()
{
    // 停止 CoreContext worker
    auto it = g_core.find(this);
    if (it != g_core.end()) {
        CoreContext* core = it->second.get();
        {
            std::lock_guard<std::mutex> l(core->mtx);
            core->running = false;
            core->cv.notify_all();
        }
        if (core->worker.joinable())
            core->worker.join();
        g_core.erase(it);
    }

    if (httpDownloader_) {
        httpDownloader_.reset();
    }
}

////////////////////////
// 配置接口
////////////////////////
void DownloadManager::setStrategy(DownloadStrategy strategy)
{
    strategy_ = strategy;
}
void DownloadManager::setMaxConcurrentDownloads(size_t max)
{
    maxConcurrent_ = max;
}
void DownloadManager::setPersistPath(const std::string& path)
{
    persistPath_ = path;
    dbHelper_ = std::make_unique<PersistenceHelper>(path);
}

////////////////////////
// 任务管理（HTTP_ONLY 实现）
////////////////////////
uint64_t DownloadManager::addDownloadTask(
    const std::string& url,
    const std::string& contentHash,
    const FileDownloadOptions& options)
{
    CoreContext* core = getCore(this);
    if (!core)
        return 0;

    return runSyncOnCore<uint64_t>(core, [this, core, url, contentHash, options]() -> uint64_t {
        DownloadTask task;
        task.id = genTaskId();
        task.url = url;
        task.contentHash = contentHash;
        task.totalSize = 0; // 对区间任务：稍后设为区间长度；对整文件：设为 Content-Length
        task.downloaded = 0;
        task.startTime = std::chrono::system_clock::now();
        task.lastUpdate = task.startTime;
        task.status = TaskStatus::Pending;

        tasks_[task.id] = task;
        taskOptions_[task.id] = options;

        // 打开父任务共享文件（随机写）
        if (!options.outputPath.empty()) {
            auto fs =
                std::make_shared<std::fstream>(options.outputPath, std::ios::in | std::ios::out | std::ios::binary);
            if (!fs->is_open()) {
                std::ofstream create(options.outputPath, std::ios::binary);
                create.close();
                fs->open(options.outputPath, std::ios::in | std::ios::out | std::ios::binary);
            }
            if (fs->is_open()) {
                std::lock_guard<std::mutex> l(core->mtx);
                core->parentFiles[task.id] = fs;
            }
        }

        if (strategy_ == DownloadStrategy::HTTP_ONLY) {
            const bool wantRange = options.hasRange;
            const size_t userStart = options.rangeStart;
            const bool endKnown = options.hasRange && options.rangeEnd != SIZE_MAX;

            if (wantRange && endKnown) {
                // (A) 完整区间已知：不需要 probe，直接切片
                const size_t length = (options.rangeEnd >= userStart) ? (options.rangeEnd - userStart + 1) : 0;
                if (length == 0) {
                    auto itT = tasks_.find(task.id);
                    if (itT != tasks_.end()) {
                        itT->second.status = TaskStatus::Failed;
                    }
                    return task.id;
                }
                {
                    std::lock_guard<std::mutex> lk(tasksMutex_);
                    tasks_[task.id].totalSize = length;
                    // 在split Task之前，避免split 之后，把 Completed 覆盖回 Running
                    tasks_[task.id].status = TaskStatus::Running;
                }
                // 如果按相对写入，预分配的大小应该是 length；绝对写入的话会非常大，不建议
                splitTask(task.id, length, userStart);
                return task.id;
            }

            // (B) 未知 end 或整文件：需要 Content-Length
            dcdn::util::HttpDownloaderTaskOption opt;
            opt.Request = std::make_shared<dcdn::util::HttpRequest>(url);
            opt.Start = 0; // 从 0 获取 Content-Length
            opt.Notify = CoreNotifyCallback;
            opt.Receiver = this;

            auto probe = httpDownloader_->CreateTask(&opt);
            if (!probe) {
                auto itT = tasks_.find(task.id);
                if (itT != tasks_.end()) {
                    itT->second.status = TaskStatus::Failed;
                }
                return task.id;
            }

            {
                std::lock_guard<std::mutex> l(core->mtx);
                ActiveSubTask a;
                a.parentTaskId = task.id;
                a.transport = Transport::HTTP;
                a.downloader = probe;
                a.offset = 0;
                a.length = 0;
                a.index = 0;
                a.isProbe = true;

                auto itF = core->parentFiles.find(task.id);
                if (itF != core->parentFiles.end())
                    a.file = itF->second;

                core->tasksByPtr[probe.get()] = std::move(a);
                core->downloaderByTaskId[task.id].push_back(probe);
            }
            httpDownloader_->AddTask(probe);
            logInfo << "[Launch] probe Task " << 0 << std::endl;

            auto itT = tasks_.find(task.id);
            if (itT != tasks_.end()) {
                itT->second.status = TaskStatus::Running;
            }
            return task.id;
        }

        // 其他策略暂未实现
        auto itT = tasks_.find(task.id);
        if (itT != tasks_.end()) {
            itT->second.status = TaskStatus::Pending;
        }
        return task.id;
    });
}

bool DownloadManager::cancelDownloadTask(uint64_t taskId)
{
    CoreContext* core = getCore(this);
    if (!core)
        return false;
    return runSyncOnCore<bool>(core, [this, core, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end())
            return false;
        it->second.cancelled = true;
        it->second.status = TaskStatus::Cancelled;

        // 1) 取消所有子任务，并标记 raw 已取消
        auto itVec = core->downloaderByTaskId.find(taskId);
        if (itVec != core->downloaderByTaskId.end()) {
            for (auto& sp : itVec->second) {
                if (!sp)
                    continue;
                httpDownloader_->CancelTask(sp);
                core->cancelledRaw.insert(sp.get()); // 标记取消
                core->tasksByPtr.erase(sp.get()); // 清理映射
            }
            itVec->second.clear();
        }

        // 2) 清空 pending
        core->pendingRanges[taskId].clear();

        // 3) 丢弃事件队列里属于该任务的事件
        for (auto it2 = core->events.begin(); it2 != core->events.end();) {
            if (core->cancelledRaw.count(it2->first) > 0) {
                it2 = core->events.erase(it2);
            } else {
                ++it2;
            }
        }

        // 4) 删除输出文件（如果指定了文件路径）
        FileDownloadOptions opts;
        {
            std::lock_guard<std::mutex> lk(tasksMutex_);
            auto itOpt = taskOptions_.find(taskId);
            if (itOpt != taskOptions_.end())
                opts = itOpt->second;
        }
        if (!opts.outputPath.empty()) {
            // 关闭 manager 共享句柄
            core->parentFiles.erase(taskId);
            std::remove(opts.outputPath.c_str()); // 忽略失败（例如文件不存在或被占用）
        }

        return true;
    });
}

bool DownloadManager::pauseDownloadTask(uint64_t taskId)
{
    CoreContext* core = getCore(this);
    if (!core)
        return false;
    return runSyncOnCore<bool>(core, [this, core, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end())
            return false;
        it->second.paused = true;
        it->second.status = TaskStatus::Paused;

        auto itVec = core->downloaderByTaskId.find(taskId);
        if (itVec != core->downloaderByTaskId.end()) {
            for (auto& sp : itVec->second) {
                if (sp)
                    httpDownloader_->PauseTask(sp);
            }
        }
        return true;
    });
}

bool DownloadManager::resumeDownloadTask(uint64_t taskId)
{
    CoreContext* core = getCore(this);
    if (!core)
        return false;
    return runSyncOnCore<bool>(core, [this, core, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end())
            return false;
        it->second.paused = false;
        it->second.status = TaskStatus::Running;

        auto itVec = core->downloaderByTaskId.find(taskId);
        if (itVec != core->downloaderByTaskId.end()) {
            for (auto& sp : itVec->second) {
                if (sp)
                    httpDownloader_->ResumeTask(sp);
            }
        }
        return true;
    });
}

////////////////////////
// 状态查询与回调注册
////////////////////////
DownloadTask DownloadManager::getTaskStatus(uint64_t taskId) const
{
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = tasks_.find(taskId);
    if (it != tasks_.end())
        return it->second;
    return {};
}

std::vector<DownloadTask> DownloadManager::getAllTasks() const
{
    std::lock_guard<std::mutex> l(tasksMutex_);
    std::vector<DownloadTask> out;
    out.reserve(tasks_.size());
    for (auto& kv : tasks_)
        out.push_back(kv.second);
    return out;
}

double DownloadManager::getOverallSpeed() const
{
    std::lock_guard<std::mutex> l(tasksMutex_);
    double sum = 0.0;
    for (auto& kv : tasks_)
        sum += kv.second.speed;
    return sum;
}

void DownloadManager::setBufferReadyCallback(uint64_t taskId, BufferReadyCallback callback)
{
    std::lock_guard<std::mutex> l(tasksMutex_);
    bufferCallbacks_[taskId] = std::move(callback);
}

void DownloadManager::removeBufferReadyCallback(uint64_t taskId)
{
    std::lock_guard<std::mutex> l(tasksMutex_);
    bufferCallbacks_.erase(taskId);
}

std::vector<std::pair<size_t, size_t>> DownloadManager::getAvailableRanges(uint64_t taskId) const
{
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = tasks_.find(taskId);
    if (it != tasks_.end())
        return it->second.completedRanges;
    return {};
}

void DownloadManager::setHttpBandwidthRatio(float ratio)
{
    httpBandwidthRatio_ = ratio;
}
void DownloadManager::setP2pBandwidthRatio(float ratio)
{
    p2pBandwidthRatio_ = ratio;
}

////////////////////////
// 内部：进度计算与通知（以父任务为单位）
////////////////////////
void DownloadManager::updateTaskProgress(uint64_t taskId, size_t downloaded)
{
    auto now = std::chrono::system_clock::now();

    bool reachedEnd = false;

    {
        std::lock_guard<std::mutex> l(tasksMutex_);
        auto it = tasks_.find(taskId);
        if (it == tasks_.end())
            return;

        it->second.downloaded += downloaded;
        if (it->second.totalSize > 0 && it->second.downloaded >= it->second.totalSize) {
            it->second.downloaded = it->second.totalSize; // 防御
            reachedEnd = true;
            logWarn << "Task " << taskId << " downloaded more than expected: " << it->second.downloaded << " > "
                    << it->second.totalSize << std::endl;
        }
        it->second.lastUpdate = now;
        calculateSpeedLocked(it->second, now);
    }

    if (reachedEnd) {
        maybeFinalizeTask_(taskId);
    }
}

// 仅在已持有 tasksMutex_ 时调用
void DownloadManager::calculateSpeedLocked(DownloadTask& t, std::chrono::system_clock::time_point now)
{
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now - t.startTime).count();
    t.speed = (seconds > 0) ? (static_cast<double>(t.downloaded) / static_cast<double>(seconds)) : 0.0;
}

void DownloadManager::notifyBufferReady(const uint64_t& taskId, size_t start, size_t end)
{
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = bufferCallbacks_.find(taskId);
    if (it != bufferCallbacks_.end()) {
        auto cb = it->second;
        if (cb)
            cb(taskId, start, end);
    }
}

////////////////////////
// SubTask / P2P / 混合策略（占位）
////////////////////////
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

// 在探测到 Content-Length 或明确区间后切分 & 调度（HTTP_ONLY）
void DownloadManager::splitTask(uint64_t taskId, size_t totalSize, size_t baseOffset)
{
    CoreContext* core = getCore(this);
    if (!core)
        return;

    FileDownloadOptions opts;
    std::string url;
    {
        std::lock_guard<std::mutex> lk(tasksMutex_);
        auto it = tasks_.find(taskId);
        if (it == tasks_.end())
            return;
        url = it->second.url;
        auto itOpt = taskOptions_.find(taskId);
        if (itOpt != taskOptions_.end())
            opts = itOpt->second;
    }

    // 预分配/截断文件到正确大小（相对写：长度就是 totalSize）
    if (!opts.outputPath.empty() && totalSize > 0) {
        // 先截断为 0，避免旧文件更大
        {
            std::ofstream reset(opts.outputPath, std::ios::binary | std::ios::trunc);
        }
        auto fs = std::make_shared<std::fstream>(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary);
        if (!fs->is_open()) {
            std::ofstream create(opts.outputPath, std::ios::binary | std::ios::trunc);
            create.close();
            fs->open(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary);
        }
        if (fs->is_open()) {
            try {
                fs->seekp(static_cast<std::streamoff>(totalSize - 1), std::ios::beg);
                char z = 0;
                fs->write(&z, 1);
                fs->flush();
            } catch (...) {
            }
            std::lock_guard<std::mutex> l(core->mtx);
            core->parentFiles[taskId] = fs;
        }
    }

    std::shared_ptr<dcdn::util::DownloaderTask> probeToCancel;
    size_t already = 0;
    {
        std::lock_guard<std::mutex> l(core->mtx);
        auto itD = core->downloaderByTaskId.find(taskId);
        if (itD != core->downloaderByTaskId.end()) {
            for (auto& sp : itD->second) {
                if (!sp)
                    continue;
                auto raw = sp.get();
                auto itActive = core->tasksByPtr.find(raw);
                if (itActive != core->tasksByPtr.end() && itActive->second.isProbe) {
                    probeToCancel = sp;
                    break;
                }
            }
        }
    }
    if (probeToCancel) {
        // probe 的 Size() 是已拉到的数据量（从 Start=0 开始）
        already = probeToCancel->Size();
        // 取消 probe + 清理映射/事件，避免迟到通知
        httpDownloader_->CancelTask(probeToCancel);
        std::lock_guard<std::mutex> l(core->mtx);
        core->tasksByPtr.erase(probeToCancel.get());
        auto& vec = core->downloaderByTaskId[taskId];
        vec.erase(std::remove(vec.begin(), vec.end(), probeToCancel), vec.end());
        core->cancelledRaw.insert(probeToCancel.get());
        core->events.erase(probeToCancel.get());
        logInfo << "http probe 已取消" << std::endl;
    }

    // 这次任务需要下载的绝对区间为 [baseOffset, baseOffset + totalSize - 1]
    const size_t absBegin = baseOffset;
    const size_t absEnd = baseOffset + totalSize - 1;

    // 把 probe 已下载的 [absBegin, absBegin + already) 排除掉（若根本没 probe，already=0，不会生效）
    size_t rangeStart = absBegin;
    if (already > 0) {
        size_t probeEnd = absBegin + already - 1;
        if (probeEnd >= absBegin)
            rangeStart = probeEnd + 1;
        if (rangeStart > absEnd) {
            // probe 已经把需要的范围全下完（极端但允许）
            maybeFinalizeTask_(taskId);
            return;
        }
    }

    // 生成 ranges（从 rangeStart 到 absEnd）
    const size_t chunk = (opts.chunkSize ? opts.chunkSize : (1u << 20)); // 默认 1MB
    std::vector<CoreContext::Range> ranges;
    for (size_t pos = rangeStart; pos <= absEnd;) {
        size_t rEnd = std::min(pos + chunk - 1, absEnd);
        ranges.push_back({pos, rEnd});
        if (rEnd == absEnd)
            break;
        pos = rEnd + 1;
    }

    // 无可下载分片 -> 尝试 finalize（幂等）
    if (ranges.empty()) {
        maybeFinalizeTask_(taskId);
        return;
    }

    logInfo << "[Split] Task " << taskId << " split into " << ranges.size()
            << " ranges, total bytes = " << (absEnd - rangeStart + 1) << " chunkSize = " << chunk << std::endl;

    // 并发启动
    const size_t canLaunch = std::min(ranges.size(), maxConcurrent_);

    std::shared_ptr<std::fstream> parentFile;
    {
        std::lock_guard<std::mutex> l(core->mtx);
        auto itF = core->parentFiles.find(taskId);
        if (itF != core->parentFiles.end())
            parentFile = itF->second;
        core->nextRangeIndex[taskId] = 0;
    }

    for (size_t i = 0; i < canLaunch; ++i) {
        auto r = ranges[i];
        dcdn::util::HttpDownloaderTaskOption opt;
        opt.Request = std::make_shared<dcdn::util::HttpRequest>(url);
        opt.Start = r.start;
        opt.End = r.end;
        opt.Notify = CoreNotifyCallback;
        opt.Receiver = this;

        auto sub = httpDownloader_->CreateTask(&opt);
        if (!sub)
            continue;

        ActiveSubTask st;
        st.parentTaskId = taskId;
        st.transport = Transport::HTTP;
        st.offset = r.start;
        st.length = r.end - r.start + 1;
        st.index = static_cast<int>(i);
        st.downloader = sub;
        st.file = parentFile;
        st.isProbe = false; // 普通分片

        {
            std::lock_guard<std::mutex> l(core->mtx);
            core->tasksByPtr[sub.get()] = std::move(st);
            core->downloaderByTaskId[taskId].push_back(sub);
            core->nextRangeIndex[taskId] = i + 1;
        }

        httpDownloader_->AddTask(sub);
        logInfo << "[Launch] Task " << i << " range " << r.start << "-" << r.end << std::endl;
    }

    // add to pending list
    if (ranges.size() > canLaunch) {
        std::lock_guard<std::mutex> l(core->mtx);
        auto& q = core->pendingRanges[taskId];
        for (size_t i = canLaunch; i < ranges.size(); ++i)
            q.push_back(ranges[i]);
    }

    // 极端 canLaunch==0 等情况，兜底 finalize（maybeFinalizeTask_ 本身幂等且会检查 running/pending）
    maybeFinalizeTask_(taskId);
}

// 幂等的任务完成判定：无运行子任务 && 无待调度分片 && 进度已满 -> Completed / Cancelled
bool DownloadManager::maybeFinalizeTask_(uint64_t taskId)
{
    CoreContext* core = getCore(this);
    if (!core)
        return false;

    bool hasRunning = false;
    bool hasPending = false;
    {
        std::lock_guard<std::mutex> l(core->mtx);
        auto itD = core->downloaderByTaskId.find(taskId);
        hasRunning = (itD != core->downloaderByTaskId.end() && !itD->second.empty());

        auto itP = core->pendingRanges.find(taskId);
        hasPending = (itP != core->pendingRanges.end() && !itP->second.empty());
    }
    if (hasRunning || hasPending)
        return false;

    // 没有运行子任务且没有 pending：看进度与取消状态
    {
        std::lock_guard<std::mutex> lk(tasksMutex_);
        auto itT = tasks_.find(taskId);
        if (itT == tasks_.end())
            return false;

        auto& t = itT->second;
        if (t.cancelled) {
            t.status = TaskStatus::Cancelled;
            return true;
        }
        if (t.totalSize > 0 && t.downloaded < t.totalSize) {
            // 还未满，不 finalize
            return false;
        }
        // 满即完成（或 totalSize==0 但确实无子任务/队列时，也按完成处理）
        if (t.totalSize > 0)
            t.downloaded = t.totalSize;
        t.status = TaskStatus::Completed;
    }
    // 完成时，做一次幂等"扫尾"清理，防止容器里残留把外部观察到的状态拖回"运行中"
    {
        std::lock_guard<std::mutex> l(core->mtx);
        core->downloaderByTaskId.erase(taskId);
        core->pendingRanges.erase(taskId);
        core->parentFiles.erase(taskId);
    }
    return true;
}
