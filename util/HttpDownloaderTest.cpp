#include "HttpDownloader.h"

#include <plog/Appenders/ConsoleAppender.h>
#include <plog/Initializers/RollingFileInitializer.h>
#include <plog/Log.h>

#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using dcdn::util::DownloaderTask;
using dcdn::util::HttpClientOption;
using dcdn::util::HttpDownloader;
using dcdn::util::HttpDownloaderTask;
using dcdn::util::HttpDownloaderTaskOption;
using dcdn::util::HttpRequest;
struct Task
{
    std::string url;
    std::shared_ptr<DownloaderTask> task;
    std::string filename;
    std::shared_ptr<std::ofstream> file;
    size_t lastReportSize = 0;
};

class Manager
{
public:
    Manager() {}
    HttpClientOption& Option()
    {
        return mDownloader;
    }
    void Run(const std::vector<std::string> urls)
    {
        int ret = mDownloader.Init(nullptr);
        if (ret != 1) {
            logWarn << "Init fail" << std::endl;
            return;
        }
        auto lastReportTime = std::chrono::steady_clock::now();
        mDownloader.Start();
        int i = 1;
        for (auto& url : urls) {
            HttpDownloaderTaskOption opt;
            opt.Request = std::make_shared<HttpRequest>(url);
            opt.Notify = notifyCallback;
            opt.Receiver = this;
            Task task;
            task.url = url;
            char path[1024];
            snprintf(path, sizeof(path), "p%d.dat", i++);
            task.filename = path;
            task.file = std::make_shared<std::ofstream>(path, std::ios::binary);
            if (!*task.file) {
                logWarn << "fail to open file: " << path << " for url: " << url;
            }
            auto t = mDownloader.CreateTask(&opt);
            if (t) {
                task.task = t;
                mTasks[t.get()] = task;
                logInfo << "create http download task for url: " << url << " save to: " << path;
            } else {
                logWarn << "fail to add http download task";
            }
            mDownloader.AddTask(t);
        }
        std::vector<unsigned char> data;
        while (!mTasks.empty()) {
            DownloaderTask* t = nullptr;
            do {
                std::unique_lock<std::mutex> lck(mMtx);
                while (mTaskEvents.empty()) {
                    mCv.wait(lck);
                }
                t = *mTaskEvents.begin();
                mTaskEvents.erase(mTaskEvents.begin());
            } while (!t);
            auto it = mTasks.find(t);
            if (it == mTasks.end()) {
                continue;
            }
            auto& task = it->second;
            logVerb << "handle task url:" << task.url;
            bool isEnd = t->IsEnd();
            auto data = t->Read();
            while (data) {
                task.file->write((const char*)data->Data(), data->Length());
                data = data->Next();
                if (!*task.file) {
                    logWarn << "fail to write file: " << task.filename;
                    mDownloader.CancelTask(task.task);
                    mTasks.erase(it);
                    continue;
                }
            }
            if (isEnd) {
                auto st = task.task->Status();
                if (st == DownloaderTask::Completed) {
                    std::string meta;
                    if (auto ht = dynamic_cast<HttpDownloaderTask*>(task.task.get())) {
                        meta += " type: " + ht->ContentType();
                        meta += " length: " + std::to_string(ht->ContentLength());
                    }
                    logInfo << "success for url: " << task.url << " file: " << task.filename << meta;
                } else {
                    logWarn << "fail for url: " << task.url << " file: " << task.filename;
                }
                mTasks.erase(it);
            }
            auto now = std::chrono::steady_clock::now();
            auto duration = now - lastReportTime;
            if (duration >= std::chrono::seconds(2)) {
                for (auto& it : mTasks) {
                    auto t = it.first;
                    size_t sz = t->Size() - it.second.lastReportSize;
                    double elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() / 1000.;
                    size_t len = 0;
                    if (auto ht = dynamic_cast<HttpDownloaderTask*>(t)) {
                        len = ht->ContentLength();
                    }
                    logInfo << "task url:" << it.second.url << " progress: " << t->Size() << "/" << len << " "
                            << " speed: " << size_t(sz / elapsed) << " Bytes/s";
                    it.second.lastReportSize = t->Size();
                }
                lastReportTime = now;
            }
        }
    }

private:
    static void notifyCallback(std::shared_ptr<DownloaderTask> task, void* r)
    {
        static_cast<Manager*>(r)->notify(task.get());
    }
    void notify(DownloaderTask* task)
    {
        std::unique_lock<std::mutex> lck(mMtx);
        mTaskEvents.insert(task);
        mCv.notify_one();
    }

private:
    std::mutex mMtx;
    std::condition_variable mCv;
    HttpDownloader mDownloader;
    std::unordered_map<DownloaderTask*, Task> mTasks;
    std::unordered_set<DownloaderTask*> mTaskEvents;
};

int main(int argc, char* argv[])
{
    static plog::ConsoleAppender<plog::TxtFormatter> consoleAppender;
    plog::Severity lvl = plog::debug;
    std::string ua;
    int follow = 0;
    std::vector<std::string> urls;
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-v") == 0) {
            lvl = plog::verbose;
        } else if (strcmp(argv[i], "--ua") == 0) {
            if (++i < argc) {
                ua = argv[i];
            }
        } else if (strcmp(argv[i], "--follow") == 0) {
            if (++i < argc) {
                follow = atoi(argv[i]);
            }
        } else {
            urls.push_back(argv[i]);
        }
    }
    if (urls.empty()) {
        std::cout << "Usage: " << argv[0] << " [-v]"
                  << " [--ua xxx]"
                  << " [--follow int]"
                  << " <url>..." << std::endl;
        std::cout << "eg:\n"
                  << argv[0] << " "
                  << "--follow 1 --ua 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 "
                     "Safari/537.36' https://curl.se/download/curl-8.15.0.tar.gz "
                     "https://archives.boost.io/release/1.88.0/source/boost_1_88_0.tar.gz"
                  << std::endl;
        return 1;
    }
    plog::init<DCDN_LOGGER_ID>(lvl, &consoleAppender);
    Manager m;
    if (!ua.empty()) {
        m.Option().SetUserAgent(ua.c_str());
    }
    if (follow > 0) {
        m.Option().SetFollowLocation(follow);
    }
    m.Run(urls);
    return 0;
}
