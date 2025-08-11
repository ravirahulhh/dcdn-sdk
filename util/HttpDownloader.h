#ifndef _DCDN_UTIL_HTTP_DOWNLOADER_H_
#define _DCDN_UTIL_HTTP_DOWNLOADER_H_

#include <curl/curl.h>

#include <any>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "Downloader.h"
#include "HttpClient.h"

NS_BEGIN(dcdn)
NS_BEGIN(util)

class HttpDownloaderTask;
class HttpDownloader;

struct HttpDownloaderTaskOption: public DownloaderTaskOption
{
    std::shared_ptr<HttpRequest> Request;
};

class HttpDownloaderTask: public DownloaderTask
{
public:
    HttpDownloaderTask(CURL* curl, const HttpDownloaderTaskOption& opt): mCurl(curl), mOpt(opt)
    {
        mOffset = opt.Start;
    }
    ~HttpDownloaderTask()
    {
        if (mCurl) {
            curl_easy_cleanup(mCurl);
        }
    }
    const HttpDownloaderTaskOption* Option() const
    {
        return &mOpt;
    }
    long Code() const
    {
        return mCode;
    }
    const std::string& ContentType() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        return mContentType;
    }
    size_t ContentLength() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        return mContentLength;
    }

private:
    void setCode(long code)
    {
        mCode = code;
    }
    void setContent(const std::string& tp, size_t length)
    {
        std::unique_lock<std::mutex> lck(mMtx);
        mContentType = tp;
        mContentLength = length;
    }
    void write(const unsigned char* dat, size_t len)
    {
        auto buf = std::make_shared<Buffer>();
        buf->Set(mOffset, dat, dat + len);
        mOffset += len;
        append(buf);
    }

private:
    typedef DownloaderTaskContainerBuffer<std::vector<unsigned char>> Buffer;
    friend class HttpDownloader;
    CURL* mCurl = nullptr;
    bool mCurlAdded = false;
    HttpDownloaderTaskOption mOpt;
    HttpHeaders::CurlHeaders mCurlHeaders;
    size_t mOffset = 0;
    std::atomic<long> mCode = 0;
    size_t mContentLength = 0;
    std::string mContentType;
};

struct HttpDownloaderOption: DownloaderOption
{
};

class HttpDownloader: public HttpClientOption, public BaseDownloader
{
public:
    HttpDownloader() {}
    ~HttpDownloader()
    {
        if (mCM) {
            curl_multi_cleanup(mCM);
            mCM = nullptr;
        }
    }
    HttpDownloader(const HttpDownloader& oth) = delete;
    HttpDownloader(HttpDownloader&& oth) = delete;
    HttpDownloader& operator=(const HttpDownloader& oth) = delete;
    HttpDownloader& operator=(HttpDownloader&& oth) = delete;
    int Init(const DownloaderOption* opt)
    {
        mCM = curl_multi_init();
        if (!mCM) {
            return ErrorCodeErr;
        }
        return ErrorCodeOk;
    }
    std::shared_ptr<DownloaderTask> CreateTask(const DownloaderTaskOption* bopt)
    {
        auto opt = dynamic_cast<const HttpDownloaderTaskOption*>(bopt);
        if (!opt) {
            return nullptr;
        }
        logDebug << "AddTask url:" << opt->Request->Url();
        auto c = curl_easy_init();
        if (!c) {
            return nullptr;
        }
        auto t = std::make_shared<HttpDownloaderTask>(c, *opt);
        auto req = opt->Request;
        curl_easy_setopt(c, CURLOPT_URL, req->Url().c_str());
        fill(c);
        if (!req->Headers().Empty()) {
            t->mCurlHeaders = req->Headers().Curl();
            curl_easy_setopt(c, CURLOPT_HTTPHEADER, (curl_slist*)(t->mCurlHeaders));
        }
        if (req->Method() == HttpRequest::Post) {
            curl_easy_setopt(c, CURLOPT_POST, 1L);
            curl_easy_setopt(c, CURLOPT_POSTFIELDS, req->Body().data());
            curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, static_cast<long>(req->Body().size()));
        }
        curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(c, CURLOPT_WRITEDATA, t.get());
        if (opt->Start > 0) {
            std::string range = std::to_string(opt->Start) + "-";
            if (opt->End >= opt->Start) {
                range += std::to_string(opt->End);
                curl_easy_setopt(c, CURLOPT_RANGE, range.c_str());
            }
        }

        return t;
    }
    void AddTask(std::shared_ptr<DownloaderTask> task)
    {
        postEvent(EventType::AddTask, task);
    }
    void CancelTask(std::shared_ptr<DownloaderTask> task)
    {
        postEvent(EventType::CancelTask, task);
    }
    void PauseTask(std::shared_ptr<DownloaderTask> task)
    {
        postEvent(EventType::PauseTask, task);
    }
    void ResumeTask(std::shared_ptr<DownloaderTask> task)
    {
        postEvent(EventType::ResumeTask, task);
    }

private:
    template<class T>
    void postEvent(EventType tp, const T& arg)
    {
        BaseDownloader::postEvent(tp, arg);
        curl_multi_wakeup(mCM);
    }

    void run()
    {
        std::vector<HttpDownloaderTask*> rmTasks;
        std::list<std::shared_ptr<Event>> evts;
        while (true) {
            int num = 0;
            int mc = curl_multi_perform(mCM, &num);
            logVerb << "curl_multi_perform:" << mc << " num:" << num;
            if (mc != CURLM_OK) {
                logWarn << "unexpected curl_multi_perform error";
                abortAll();
                continue;
            }

            CURLMsg* msg;
            int msgs_left = 0;
            while ((msg = curl_multi_info_read(mCM, &msgs_left))) {
                if (msg->msg == CURLMSG_DONE) {
                    CURL* c = msg->easy_handle;
                    CURLcode res = msg->data.result;
                    auto t = mCurlTasks[c];
                    if (res == CURLE_OK) {
                        t->setStatus(HttpDownloaderTask::Completed);
                        logDebug << "task url:" << t->mOpt.Request->Url() << " completed";
                    } else {
                        t->setStatus(HttpDownloaderTask::Fail);
                    }
                }
            }

            for (auto it : mTasks) {
                auto t = it.first;
                bool notify = false;
                if (t->HasData() || t->IsEnd()) {
                    notify = true;
                }
                if (t->outOfBuf()) {
                    if (t->mCurlAdded) {
                        curl_multi_remove_handle(mCM, t->mCurl);
                        t->mCurlAdded = false;
                    }
                } else if (!t->mCurlAdded) {
                    int ret = curl_multi_add_handle(mCM, t->mCurl);
                    if (ret != CURLM_OK) {
                        t->mCurlAdded = false;
                        t->setStatus(HttpDownloaderTask::Fail);
                        notify = true;
                    }
                }
                if (notify) {
                    t->notify(it.second);
                }
                if (t->IsEnd()) {
                    rmTasks.push_back(t);
                }
            }
            if (!rmTasks.empty()) {
                for (auto t : rmTasks) {
                    removeTask(t);
                }
                rmTasks.clear();
            }

            mc = curl_multi_poll(mCM, NULL, 0, 10000, &num);
            if (mc != CURLM_OK) {
                logWarn << "unexpected curl_multi_poll error";
                abortAll();
                continue;
            }

            do {
                std::unique_lock<std::mutex> lck(mMtx);
                evts.swap(mEvts);
            } while (0);
            while (!evts.empty()) {
                auto evt = evts.front();
                evts.pop_front();
                handleEvent(evt);
            }
        }
    }
    void abortAll()
    {
        while (!mTasks.empty()) {
            auto it = mTasks.begin();
            auto t = it->first;
            t->setStatus(HttpDownloaderTask::Fail);
            t->notify(it->second);
            removeTask(t);
        }
    }
    void removeTask(HttpDownloaderTask* t)
    {
        if (t->mCurlAdded) {
            curl_multi_remove_handle(mCM, t->mCurl);
            t->mCurlAdded = false;
        }
        mCurlTasks.erase(t->mCurl);
        mTasks.erase(t);
    }
    void handleEvent(std::shared_ptr<Event> evt)
    {
        switch (evt->first) {
            case EventType::AddTask:
                handleAddTaskEvent(evt);
                break;
            case EventType::CancelTask:
                handleCancelTaskEvent(evt);
                break;
            case EventType::PauseTask:
                handlePauseTaskEvent(evt);
                break;
            default:
                break;
        }
    }
    void handleAddTaskEvent(std::shared_ptr<Event> evt)
    {
        auto bt = std::any_cast<std::shared_ptr<DownloaderTask>>(evt->second);
        auto t = std::dynamic_pointer_cast<HttpDownloaderTask>(bt);
        if (!t) {
            return;
        }
        logDebug << "AddTaskEvent url:" << t->mOpt.Request->Url();
        if (mTasks.find(t.get()) != mTasks.end()) {
            return;
        }
        mTasks[t.get()] = t;
        mCurlTasks[t->mCurl] = t;
        int ret = curl_multi_add_handle(mCM, t->mCurl);
        if (ret != CURLM_OK) {
            t->mCurlAdded = false;
            t->setStatus(HttpDownloaderTask::Fail);
            t->notify(t);
            return;
        }
        t->mCurlAdded = true;
        t->setStatus(HttpDownloaderTask::Running);
    }
    void handleCancelTaskEvent(std::shared_ptr<Event> evt)
    {
        auto t = std::any_cast<std::shared_ptr<HttpDownloaderTask>>(evt->second);
        auto it = mTasks.find(t.get());
        if (it != mTasks.end()) {
            if (t->cancel()) {
                t->notify(it->second);
            }
            removeTask(t.get());
        }
    }
    void handlePauseTaskEvent(std::shared_ptr<Event> evt)
    {
        auto t = std::any_cast<std::shared_ptr<HttpDownloaderTask>>(evt->second);
        auto it = mTasks.find(t.get());
        if (it != mTasks.end()) {
            if (t->pause()) {
                if (t->mCurlAdded) {
                    curl_multi_remove_handle(mCM, t->mCurl);
                    t->mCurlAdded = false;
                }
                t->notify(it->second);
            }
        }
    }
    void handleResumeTaskEvent(std::shared_ptr<Event> evt)
    {
        auto t = std::any_cast<std::shared_ptr<HttpDownloaderTask>>(evt->second);
        auto it = mTasks.find(t.get());
        if (it != mTasks.end()) {
            if (t->resume()) {
                int ret = curl_multi_add_handle(mCM, t->mCurl);
                if (ret != CURLM_OK) {
                    t->mCurlAdded = false;
                    t->setStatus(HttpDownloaderTask::Fail);
                    t->notify(t);
                    return;
                }
                t->mCurlAdded = true;
            }
        }
    }

    static size_t writeCallback(void* contents, size_t size, size_t nmemb, void* data)
    {
        auto t = static_cast<HttpDownloaderTask*>(data);
        if (t->Size() == 0) {
            long code = 0;
            curl_easy_getinfo(t->mCurl, CURLINFO_RESPONSE_CODE, &code);
            t->setCode(code);
            char* ctype = nullptr;
            if (curl_easy_getinfo(t->mCurl, CURLINFO_CONTENT_TYPE, &ctype) != CURLE_OK) {
                ctype = nullptr;
            }
            curl_off_t clen = 0;
            if (curl_easy_getinfo(t->mCurl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &clen) != CURLE_OK) {
                clen = 0;
            }
            if (ctype || clen > 0) {
                t->setContent((ctype ? ctype : ""), (clen > 0 ? clen : 0));
            }
        }
        const unsigned char* p = (const unsigned char*)contents;
        size *= nmemb;
        t->write(p, size);
        logVerb << "task url:" << t->mOpt.Request->Url() << " write data:" << size;
        return size;
    }

private:
    CURLM* mCM;
    std::unordered_map<HttpDownloaderTask*, std::shared_ptr<HttpDownloaderTask>> mTasks;
    std::unordered_map<CURL*, std::shared_ptr<HttpDownloaderTask>> mCurlTasks;
};

NS_END
NS_END

#endif
