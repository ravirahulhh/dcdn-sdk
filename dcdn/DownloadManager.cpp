#include "DownloadManager.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <thread>
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <deque>
#include <future>
#include <algorithm>
#include <sstream>
#include <cstdio>   // std::remove

using namespace dcdn;

namespace {

// 传输类型（为未来 P2P/HYBRID 预留）
enum class Transport { HTTP, P2P };

// 统一的最小子任务描述（HTTP/P2P 复用）
struct ActiveSubTask {
    std::string parentTaskId; // 所属主任务 id
    Transport transport = Transport::HTTP;
    size_t offset = 0;          // Range start（含）
    size_t length = 0;          // Range 长度（end-start+1）, 0 表示不确定（直到 IsEnd 或 ContentLength 可知）
    int index = 0;              // 该子任务在父任务的分片序号
    std::shared_ptr<dcdn::util::DownloaderTask> downloader;
    std::shared_ptr<std::fstream> file;  // 父任务共享随机写文件（按 offset 写需 seek）
};

// 引擎上下文（协议无关）：统一串行执行 public API + 处理 downloader 事件
struct CoreContext {
    std::mutex mtx;
    std::condition_variable cv;

    // downloader raw 指针 -> 子任务
    std::unordered_map<dcdn::util::DownloaderTask*, ActiveSubTask> tasksByPtr;

    // 父任务 -> 当前运行中的 downloader 列表
    std::unordered_map<std::string, std::vector<std::shared_ptr<dcdn::util::DownloaderTask>>> downloaderByTaskId;

    // 事件队列（只存 shared_ptr，保障生命周期）
    // 只存一次事件，key 为裸指针，value 持有生命周期
    std::unordered_map<dcdn::util::DownloaderTask*, std::shared_ptr<dcdn::util::DownloaderTask>> events;

    // 串行化命令队列
    std::deque<std::function<void()>> cmdQueue;

    // 父任务 -> 等待调度的 Range 队列
    struct Range { size_t start; size_t end; };
    std::unordered_map<std::string, std::deque<Range>> pendingRanges;
    std::unordered_map<std::string, size_t> nextRangeIndex;

    // 父任务 -> 共享随机写文件句柄
    std::unordered_map<std::string, std::shared_ptr<std::fstream>> parentFiles;

    // 已取消的 raw*（用于丢弃取消后的残留回调）
    std::unordered_set<dcdn::util::DownloaderTask*> cancelledRaw;

    // “尚未绑定”的尝试计数（避免无限重试）
    std::unordered_map<dcdn::util::DownloaderTask*, int> orphanRetryCount;

    std::thread worker;
    bool running = false;
};

// 全局：manager -> CoreContext
static std::unordered_map<DownloadManager*, std::unique_ptr<CoreContext>> g_core;

static CoreContext* getCore(DownloadManager* mgr) {
    auto it = g_core.find(mgr);
    if (it == g_core.end()) return nullptr;
    return it->second.get();
}

// 统一的 downloader notify 回调（瘦身：只入队，不写状态）
static void CoreNotifyCallback(std::shared_ptr<dcdn::util::DownloaderTask> task, void* receiver) {
    if (!task || !receiver) return;
    auto* mgr  = static_cast<DownloadManager*>(receiver);
    auto* core = getCore(mgr);
    if (!core) return;

    {
        std::lock_guard<std::mutex> g(core->mtx);
        // 取消后的迟到通知直接丢弃
        if (core->cancelledRaw.count(task.get())) {
            return;
        }
        core->events[task.get()] = task; // 覆盖或插入
    }
    core->cv.notify_one();
}

static std::string genTaskId() {
    static std::atomic<uint64_t> cnt{0};
    std::ostringstream ss;
    ss << std::chrono::steady_clock::now().time_since_epoch().count() << "_" << cnt++;
    return ss.str();
}

// 在 manager 线程同步执行（串行化 public API）
template<typename R>
static R runSyncOnCore(CoreContext* core, std::function<R()> fn) {
    auto prom = std::make_shared<std::promise<R>>();
    auto fut = prom->get_future();
    {
        std::lock_guard<std::mutex> l(core->mtx);
        core->cmdQueue.emplace_back([prom, fn]() {
            try { prom->set_value(fn()); }
            catch (...) { try { prom->set_exception(std::current_exception()); } catch (...) {} }
        });
    }
    core->cv.notify_one();
    return fut.get();
}

static void runAsyncOnCore(CoreContext* core, std::function<void()> fn) {
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
std::string DownloadTask::serialize() const {
    std::ostringstream ss;
    ss << id << "|" << url << "|" << contentHash << "|" << totalSize << "|"
       << downloaded << "|" << static_cast<int>(status);
    return ss.str();
}

DownloadTask DownloadTask::deserialize(const std::string& data) {
    DownloadTask task;
    std::istringstream ss(data);
    std::string token;
    std::getline(ss, task.id, '|');
    std::getline(ss, task.url, '|');
    std::getline(ss, task.contentHash, '|');
    if (!std::getline(ss, token, '|')) return task;
    task.totalSize = static_cast<size_t>(std::stoull(token));
    if (!std::getline(ss, token, '|')) return task;
    task.downloaded = static_cast<size_t>(std::stoull(token));
    if (!std::getline(ss, token, '|')) return task;
    task.status = static_cast<TaskStatus>(std::stoi(token));
    return task;
}

////////////////////////
// PersistenceHelper (占位 TODO)
////////////////////////
DownloadManager::PersistenceHelper::PersistenceHelper(const std::string& dbPath) {
    db_ = nullptr;
    (void)dbPath;
}

DownloadManager::PersistenceHelper::~PersistenceHelper() {}

bool DownloadManager::PersistenceHelper::saveTask(const DownloadTask& task) { (void)task; return true; }
bool DownloadManager::PersistenceHelper::loadTasks(std::vector<DownloadTask>& tasks) { (void)tasks; return true; }
bool DownloadManager::PersistenceHelper::deleteTask(const std::string& taskId) { (void)taskId; return true; }
bool DownloadManager::PersistenceHelper::saveSubTasks(const std::string& taskId, const std::vector<SubTask>& subtasks) {
    (void)taskId; (void)subtasks; return true;
}
bool DownloadManager::PersistenceHelper::loadSubTasks(const std::string& taskId, std::vector<SubTask>& subtasks) {
    (void)taskId; (void)subtasks; return true;
}

////////////////////////
// DownloadManager 构造 / 析构
////////////////////////
DownloadManager::DownloadManager() {
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
    // worker: 串行执行 cmdQueue，然后处理 events（读数据 -> 写文件 -> 更新状态）
    coreRaw->worker = std::thread([this, coreRaw]() {
        CoreContext* core = coreRaw;          // 直接使用指针，不会为空

        while (true) {
            std::function<void()> cmd;
            std::shared_ptr<dcdn::util::DownloaderTask> ev;

            {
                std::unique_lock<std::mutex> l(core->mtx);
                core->cv.wait(l, [&]{
                    return !core->cmdQueue.empty() || !core->events.empty() || !core->running;
                });
                if (!core->running && core->cmdQueue.empty() && core->events.empty()) break;

                if (!core->cmdQueue.empty()) {
                    cmd = std::move(core->cmdQueue.front());
                    core->cmdQueue.pop_front();
                } else if (!core->events.empty()) {
                    auto it = core->events.begin();
                    ev = std::move(it->second);
                    core->events.erase(it);
                }
            }

            if (cmd) { // 执行控制命令
                try { cmd(); } catch (...) {}
                continue;
            }
            if (!ev) continue;

            auto* raw = ev.get();

            // task 取消后的迟到通知直接丢弃
            {
                std::lock_guard<std::mutex> g(core->mtx);
                if (core->cancelledRaw.count(raw)) continue;
            }

            // 找绑定（可能早到）
            ActiveSubTask active;
            bool bound = false;
            {
                std::lock_guard<std::mutex> g(core->mtx);
                auto it = core->tasksByPtr.find(raw);
                if (it != core->tasksByPtr.end()) {
                    active = it->second;
                    bound = true;
                }
            }

            if (!bound) {
                bool retry = false;
                {
                    std::lock_guard<std::mutex> g(core->mtx);
                    int& c = core->orphanRetryCount[raw];
                    if (c < 20) { // 最多重试 20 次
                        ++c;
                        core->events[ev.get()] = ev; // 重回事件队列 
                        retry = true;
                    } else {
                        core->orphanRetryCount.erase(raw); // 放弃，避免无限循环
                    }
                }
                if (retry) std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }

            // 若是 HTTP probe：尝试拿到 Content-Length 触发 split
            if (strategy_ == DownloadStrategy::HTTP_ONLY && !active.parentTaskId.empty()) {
                bool needSplit = false;
                size_t clen = 0;
                {
                    std::lock_guard<std::mutex> lk(tasksMutex_);
                    auto it = tasks_.find(active.parentTaskId);
                    if (it != tasks_.end() && it->second.totalSize == 0) {
                        if (auto ht = dynamic_cast<dcdn::util::HttpDownloaderTask*>(raw)) {
                            clen = ht->ContentLength();
                            if (clen > 0) {
                                it->second.totalSize = clen;
                                needSplit = true;
                            }
                        }
                    }
                }
                if (needSplit) {
                    splitTask(active.parentTaskId, clen);
                }
            }

            // 读取并写入
            bool isEnd = raw->IsEnd();
            auto buffer = raw->Read();
            size_t readSum = 0;

            FileDownloadOptions opts;
            std::shared_ptr<std::fstream> file;
            {
                std::lock_guard<std::mutex> lk(tasksMutex_);
                auto itOpt = taskOptions_.find(active.parentTaskId);
                if (itOpt != taskOptions_.end()) opts = itOpt->second;
            }
            {
                std::lock_guard<std::mutex> l(core->mtx);
                auto itF = core->parentFiles.find(active.parentTaskId);
                if (itF != core->parentFiles.end()) file = itF->second;
            }

            while (buffer) {
                size_t len = buffer->Length();
                size_t off = buffer->Offset();

                if (file && file->good()) {
                    try { file->seekp(static_cast<std::streamoff>(off), std::ios::beg); } catch (...) {}
                    file->write(reinterpret_cast<const char*>(buffer->Data()), len);
                    file->flush();
                } else if (opts.outputStream) {
                    opts.outputStream->write(reinterpret_cast<const char*>(buffer->Data()), len);
                    opts.outputStream->flush();
                } else if (!opts.outputPath.empty()) {
                    // 兜底：随机写
                    std::fstream ofs(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary);
                    if (!ofs) { std::ofstream create(opts.outputPath, std::ios::binary); create.close();
                                ofs.open(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary); }
                    if (ofs) { ofs.seekp(static_cast<std::streamoff>(off), std::ios::beg);
                               ofs.write(reinterpret_cast<const char*>(buffer->Data()), len); }
                }

                if (opts.streamCallback) {
                    opts.streamCallback(reinterpret_cast<const char*>(buffer->Data()), len, off);
                }

                readSum += len;
                buffer = buffer->Next();
            }

            if (readSum > 0 && !active.parentTaskId.empty()) {
                updateTaskProgress(active.parentTaskId, readSum);
                notifyBufferReady(active.parentTaskId, active.offset, active.offset + readSum);
            }

            // 子任务结束：清理并尝试续排 pendingRanges
            if (isEnd && !active.parentTaskId.empty()) {
                {
                    std::lock_guard<std::mutex> l(core->mtx);
                    core->tasksByPtr.erase(raw);
                    auto &vec = core->downloaderByTaskId[active.parentTaskId];
                    vec.erase(std::remove_if(vec.begin(), vec.end(),
                        [raw](const std::shared_ptr<dcdn::util::DownloaderTask>& p){ return p.get() == raw; }),
                        vec.end());
                }

                // 续排
                bool launched = false;
                {
                    std::lock_guard<std::mutex> l(core->mtx);
                    auto &q = core->pendingRanges[active.parentTaskId];
                    if (!q.empty()) {
                        auto r = q.front(); q.pop_front();
                        size_t idx = core->nextRangeIndex[active.parentTaskId]++;

                        dcdn::util::HttpDownloaderTaskOption opt;
                        {
                            std::lock_guard<std::mutex> lk(tasksMutex_);
                            opt.Request = std::make_shared<dcdn::util::HttpRequest>(tasks_.at(active.parentTaskId).url);
                        }
                        opt.Start   = r.start;
                        opt.End     = r.end;
                        opt.Notify  = CoreNotifyCallback;
                        opt.Receiver= this;
                        auto sub = httpDownloader_->CreateTask(&opt);
                        if (sub) {
                            httpDownloader_->AddTask(sub);
                            std::shared_ptr<std::fstream> f;
                            auto itF = core->parentFiles.find(active.parentTaskId);
                            if (itF != core->parentFiles.end()) f = itF->second;

                            ActiveSubTask st;
                            st.parentTaskId = active.parentTaskId;
                            st.transport = Transport::HTTP;
                            st.offset = r.start;
                            st.length = r.end - r.start + 1;
                            st.index = static_cast<int>(idx);
                            st.downloader = sub;
                            st.file = f;

                            core->tasksByPtr[sub.get()] = std::move(st);
                            core->downloaderByTaskId[active.parentTaskId].push_back(sub);
                            launched = true;
                        }
                    }
                }

                // 无运行子任务且无 pending，则父任务完成/失败
                bool stillRunning = false;
                {
                    std::lock_guard<std::mutex> l(core->mtx);
                    if (!core->downloaderByTaskId[active.parentTaskId].empty()) stillRunning = true;
                    if (!stillRunning && !core->pendingRanges[active.parentTaskId].empty()) stillRunning = true;
                }
                if (!stillRunning) {
                    auto st = ev->Status();
                    std::lock_guard<std::mutex> lk(tasksMutex_);
                    auto &parent = tasks_.at(active.parentTaskId);
                    if (st == dcdn::util::DownloaderTask::Completed && !parent.cancelled)
                        parent.status = TaskStatus::Completed;
                    else if (parent.cancelled)
                        parent.status = TaskStatus::Cancelled;
                    else
                        parent.status = TaskStatus::Failed;
                }
            }
        } // while
    });

   
}

DownloadManager::~DownloadManager() {
    // 停止 CoreContext worker
    auto it = g_core.find(this);
    if (it != g_core.end()) {
        CoreContext* core = it->second.get();
        {
            std::lock_guard<std::mutex> l(core->mtx);
            core->running = false;
            core->cv.notify_all();
        }
        if (core->worker.joinable()) core->worker.join();
        g_core.erase(it);
    }

    if (httpDownloader_) {
        httpDownloader_.reset();
    }
}

////////////////////////
// 配置接口
////////////////////////
void DownloadManager::setStrategy(DownloadStrategy strategy) { strategy_ = strategy; }
void DownloadManager::setMaxConcurrentDownloads(size_t max) { maxConcurrent_ = max; }
void DownloadManager::setPersistPath(const std::string& path) {
    persistPath_ = path;
    dbHelper_ = std::make_unique<PersistenceHelper>(path);
}

////////////////////////
// 任务管理（HTTP_ONLY 实现） 
////////////////////////
std::string DownloadManager::addDownloadTask(
    const std::string& url,
    const std::string& contentHash,
    const FileDownloadOptions& options
) {
    CoreContext* core = getCore(this);
    if (!core) return {};

    return runSyncOnCore<std::string>(core, [this, core, url, contentHash, options]() -> std::string {
        DownloadTask task;
        task.id = genTaskId();
        task.url = url;
        task.contentHash = contentHash;
        task.totalSize = 0;
        task.downloaded = 0;
        task.startTime = std::chrono::system_clock::now();
        task.lastUpdate = task.startTime;
        task.status = TaskStatus::Pending;

        tasks_[task.id] = task;
        taskOptions_[task.id] = options;

        // 打开父任务共享文件（随机写）
        if (!options.outputPath.empty()) {
            auto fs = std::make_shared<std::fstream>(options.outputPath,
                        std::ios::in | std::ios::out | std::ios::binary);
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
            // 先创建一个 probe 以获取 Content-Length
            dcdn::util::HttpDownloaderTaskOption opt;
            opt.Request = std::make_shared<dcdn::util::HttpRequest>(url);
            opt.Start = 0;
            opt.Notify = CoreNotifyCallback;
            opt.Receiver = this;

            auto probe = httpDownloader_->CreateTask(&opt);
            if (!probe) {
                tasks_.at(task.id).status = TaskStatus::Failed;
                return task.id;
            }
            httpDownloader_->AddTask(probe);
            std::cout << "Http probe task created" <<   std::endl;

            // 立即注册映射（防早到）
            {
                std::lock_guard<std::mutex> l(core->mtx);
                ActiveSubTask a;
                a.parentTaskId = task.id;
                a.transport = Transport::HTTP;
                a.downloader = probe;
                a.offset = 0;
                a.length = 0;
                a.index = 0;

                auto itF = core->parentFiles.find(task.id);
                if (itF != core->parentFiles.end()) a.file = itF->second;

                core->tasksByPtr[probe.get()] = std::move(a);
                core->downloaderByTaskId[task.id].push_back(probe);
            }

            tasks_.at(task.id).status = TaskStatus::Running;
            return task.id;
        }

        // 其他策略暂未实现
        tasks_.at(task.id).status = TaskStatus::Pending;
        return task.id;
    });
}

bool DownloadManager::cancelDownloadTask(const std::string& taskId) {
    CoreContext* core = getCore(this);
    if (!core) return false;
    return runSyncOnCore<bool>(core, [this, core, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return false;
        it->second.cancelled = true;
        it->second.status = TaskStatus::Cancelled;

        // 1) 取消所有子任务，并标记 raw 已取消
        auto itVec = core->downloaderByTaskId.find(taskId);
        if (itVec != core->downloaderByTaskId.end()) {
            for (auto &sp : itVec->second) {
                if (!sp) continue;
                httpDownloader_->CancelTask(sp);
                core->cancelledRaw.insert(sp.get());  // 标记取消
                core->tasksByPtr.erase(sp.get());     // 清理映射
            }
            itVec->second.clear();
        }

        // 2) 清空 pending
        core->pendingRanges[taskId].clear();

        // 3) 丢弃事件队列里属于该任务的事件
        for (auto it = core->events.begin(); it != core->events.end(); ) {
            if (core->cancelledRaw.count(it->first) > 0) {
                it = core->events.erase(it); // 按 key 擦
            } else {
                ++it;
            }
        }

        // 4) 删除输出文件（如果指定了文件路径）
        FileDownloadOptions opts;
        {
            std::lock_guard<std::mutex> lk(tasksMutex_);
            auto itOpt = taskOptions_.find(taskId);
            if (itOpt != taskOptions_.end()) opts = itOpt->second;
        }
        if (!opts.outputPath.empty()) {
            // 关闭 manager 共享句柄
            core->parentFiles.erase(taskId);
            std::remove(opts.outputPath.c_str()); // 忽略失败（例如文件不存在或被占用）
        }

        return true;
    });
}

bool DownloadManager::pauseDownloadTask(const std::string& taskId) {
    CoreContext* core = getCore(this);
    if (!core) return false;
    return runSyncOnCore<bool>(core, [this, core, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return false;
        it->second.paused = true;
        it->second.status = TaskStatus::Paused;

        auto itVec = core->downloaderByTaskId.find(taskId);
        if (itVec != core->downloaderByTaskId.end()) {
            for (auto &sp : itVec->second) {
                if (sp) httpDownloader_->PauseTask(sp);
            }
        }
        return true;
    });
}

bool DownloadManager::resumeDownloadTask(const std::string& taskId) {
    CoreContext* core = getCore(this);
    if (!core) return false;
    return runSyncOnCore<bool>(core, [this, core, taskId]() -> bool {
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return false;
        it->second.paused = false;
        it->second.status = TaskStatus::Running;

        auto itVec = core->downloaderByTaskId.find(taskId);
        if (itVec != core->downloaderByTaskId.end()) {
            for (auto &sp : itVec->second) {
                if (sp) httpDownloader_->ResumeTask(sp);
            }
        }
        return true;
    });
}

////////////////////////
// 状态查询与回调注册
////////////////////////
DownloadTask DownloadManager::getTaskStatus(const std::string& taskId) const {
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = tasks_.find(taskId);
    if (it != tasks_.end()) return it->second;
    return {};
}

std::vector<DownloadTask> DownloadManager::getAllTasks() const {
    std::lock_guard<std::mutex> l(tasksMutex_);
    std::vector<DownloadTask> out;
    out.reserve(tasks_.size());
    for (auto &kv : tasks_) out.push_back(kv.second);
    return out;
}

double DownloadManager::getOverallSpeed() const {
    std::lock_guard<std::mutex> l(tasksMutex_);
    double sum = 0.0;
    for (auto &kv : tasks_) sum += kv.second.speed;
    return sum;
}

void DownloadManager::setBufferReadyCallback(const std::string& taskId, BufferReadyCallback callback) {
    std::lock_guard<std::mutex> l(tasksMutex_);
    bufferCallbacks_[taskId] = std::move(callback);
}

void DownloadManager::removeBufferReadyCallback(const std::string& taskId) {
    std::lock_guard<std::mutex> l(tasksMutex_);
    bufferCallbacks_.erase(taskId);
}

std::vector<std::pair<size_t, size_t>> DownloadManager::getAvailableRanges(const std::string& taskId) const {
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = tasks_.find(taskId);
    if (it != tasks_.end()) return it->second.completedRanges;
    return {};
}

void DownloadManager::setHttpBandwidthRatio(float ratio) { httpBandwidthRatio_ = ratio; }
void DownloadManager::setP2pBandwidthRatio(float ratio) { p2pBandwidthRatio_ = ratio; }

////////////////////////
// 内部：进度计算与通知（以父任务为单位）
////////////////////////
void DownloadManager::updateTaskProgress(const std::string& taskId, size_t downloaded) {
    auto now = std::chrono::system_clock::now();
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = tasks_.find(taskId);
    if (it == tasks_.end()) return;
    it->second.downloaded += downloaded;
    it->second.lastUpdate = now;
    calculateSpeedLocked(it->second, now);  // 不再二次加锁
}

// 该函数必须在外部持锁访问
void DownloadManager::calculateSpeedLocked(DownloadTask& t,
                                 std::chrono::system_clock::time_point now) {
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now - t.startTime).count();
    t.speed = (seconds > 0) ? (static_cast<double>(t.downloaded) / static_cast<double>(seconds)) : 0.0;
}

void DownloadManager::notifyBufferReady(const std::string& taskId, size_t start, size_t end) {
    std::lock_guard<std::mutex> l(tasksMutex_);
    auto it = bufferCallbacks_.find(taskId);
    if (it != bufferCallbacks_.end()) {
        auto cb = it->second;
        if (cb) cb(taskId, start, end);
    }
}

////////////////////////
// SubTask / P2P / 混合策略（占位）
////////////////////////
void DownloadManager::startHttpDownload(const std::string& taskId) { (void)taskId; }
void DownloadManager::startP2pDownload(const std::string& taskId) { (void)taskId; }
void DownloadManager::startHybridDownload(const std::string& taskId) { (void)taskId; }

// 在探测到 Content-Length 后切分 & 调度（HTTP_ONLY）
void DownloadManager::splitTask(const std::string& taskId, size_t totalSize) {
    CoreContext* core = getCore(this);
    if (!core) return;

    FileDownloadOptions opts;
    std::string url;
    {
        std::lock_guard<std::mutex> lk(tasksMutex_);
        auto it = tasks_.find(taskId);
        if (it == tasks_.end()) return;
        url = it->second.url;
        auto itOpt = taskOptions_.find(taskId);
        if (itOpt != taskOptions_.end()) opts = itOpt->second;
    }

    // 预分配/截断文件到 totalSize
    if (!opts.outputPath.empty()) {
        auto fs = std::make_shared<std::fstream>(opts.outputPath,
                    std::ios::in | std::ios::out | std::ios::binary);
        if (!fs->is_open()) {
            std::ofstream create(opts.outputPath, std::ios::binary);
            create.close();
            fs->open(opts.outputPath, std::ios::in | std::ios::out | std::ios::binary);
        }
        if (fs->is_open() && totalSize > 0) {
            try {
                fs->seekp(static_cast<std::streamoff>(totalSize - 1), std::ios::beg);
                char z = 0; fs->write(&z, 1); fs->flush();
            } catch (...) {}
            std::lock_guard<std::mutex> l(core->mtx);
            core->parentFiles[taskId] = fs;
        }
    }

    // 取消 probe（若在跑）
    std::shared_ptr<dcdn::util::DownloaderTask> probeToCancel;
    size_t already = 0;
    {
        std::lock_guard<std::mutex> l(core->mtx);
        auto &vec = core->downloaderByTaskId[taskId];
        if (!vec.empty()) probeToCancel = vec.front();
    }
    if (probeToCancel) {
        already = probeToCancel->Size();
        httpDownloader_->CancelTask(probeToCancel);
        std::cout << "http probe 取消" << std::endl;
        std::lock_guard<std::mutex> l(core->mtx);
        core->tasksByPtr.erase(probeToCancel.get());
        auto &vec = core->downloaderByTaskId[taskId];
        vec.erase(std::remove(vec.begin(), vec.end(), probeToCancel), vec.end());
    }

    // 生成 ranges
    size_t chunk = (opts.chunkSize ? opts.chunkSize : (1u << 20)); // default 1MB
    std::vector<CoreContext::Range> ranges;
    ranges.reserve((totalSize + chunk - 1) / chunk);

    // 第一块考虑 probe 已下载 [0, already)
    if (already < totalSize) {
        size_t firstStart = already;
        size_t firstEnd   = std::min(already + chunk - 1, totalSize - 1);
        ranges.push_back({ firstStart, firstEnd });
    }
    // 其余块
    for (size_t pos = ((already < chunk) ? chunk : ((already + chunk - 1) / chunk) * chunk);
         pos < totalSize; pos += chunk) {
        size_t end = std::min(pos + chunk - 1, totalSize - 1);
        ranges.push_back({ pos, end });
    }

    // 并发启动
    size_t canLaunch = std::min(ranges.size(), maxConcurrent_);

    std::shared_ptr<std::fstream> parentFile;
    {
        std::lock_guard<std::mutex> l(core->mtx);
        auto itF = core->parentFiles.find(taskId);
        if (itF != core->parentFiles.end()) parentFile = itF->second;
        core->nextRangeIndex[taskId] = 0;
    }

    for (size_t i = 0; i < canLaunch; ++i) {
        auto r = ranges[i];
        dcdn::util::HttpDownloaderTaskOption opt;
        // opt.Url = url;
        opt.Request = std::make_shared<dcdn::util::HttpRequest>(url);
        opt.Start = r.start;
        opt.End   = r.end;
        opt.Notify = CoreNotifyCallback;
        opt.Receiver = this;

        auto sub = httpDownloader_->CreateTask(&opt);
        if (!sub) continue;
        httpDownloader_->AddTask(sub);

        ActiveSubTask st;
        st.parentTaskId = taskId;
        st.transport = Transport::HTTP;
        st.offset = r.start;
        st.length = r.end - r.start + 1;
        st.index = static_cast<int>(i);
        st.downloader = sub;
        st.file = parentFile;

        std::lock_guard<std::mutex> l(core->mtx);
        core->tasksByPtr[sub.get()] = std::move(st);
        core->downloaderByTaskId[taskId].push_back(sub);
        core->nextRangeIndex[taskId] = i + 1;
    }

    // 余下入队
    if (ranges.size() > canLaunch) {
        std::lock_guard<std::mutex> l(core->mtx);
        auto &q = core->pendingRanges[taskId];
        for (size_t i = canLaunch; i < ranges.size(); ++i) q.push_back(ranges[i]);
    }
}

void DownloadManager::onSubTaskCompleted(const std::string& taskId, size_t subtaskIndex) {
    (void)taskId; (void)subtaskIndex;
    // 逻辑已在 worker 的 IsEnd 分支里做了统一处理
}
