#ifndef _DCDN_SDK_API_CLIENT_H_
#define _DCDN_SDK_API_CLIENT_H_

#include <nlohmann/json.hpp>

#include <unordered_map>

#include "Config.h"
#include "Event.h"
#include "util/HttpDownloader.h"

NS_BEGIN(dcdn)

class MainManager;

class ApiClient
{
public:
    using json = nlohmann::json;
    using HttpRequest = util::HttpRequest;
    using HttpResponse = util::HttpResponse;

public:
    ApiClient(util::HttpDownloader* downloader): mDownloader(downloader) {}
    template<class E, class Succ = void (*)(HttpResponse&), class Fail = void (*)(long)>
    int Do(void** reqId, HttpRequest&& req, E* ev, Succ succ, Fail fail)
    {
        auto task = std::make_shared<Task>();
        task->client = this;
        util::HttpDownloaderTaskOption opt;
        opt.Request = std::make_shared<util::HttpRequest>(std::move(req));
        opt.Notify = notify;
        opt.Receiver = task.get();
        auto t = mDownloader->CreateTask(&opt);
        if (!t) {
            return ErrorCodeErr;
        }
        if (ev) {
            task->callback = std::make_shared<Callback<E, Succ, Fail>>(ev, succ, fail);
        }
        task->task = t;
        mTasks[task.get()] = task;
        if (reqId) {
            *reqId = task.get();
        }
        mDownloader->AddTask(t);
        return ErrorCodeOk;
    }
    bool Cancel(void* reqId)
    {
        return removeTask((Task*)reqId);
    }
    void HandleAsyncApiRequestEvent(std::shared_ptr<Event> evt)
    {
        auto e = std::static_pointer_cast<ArgEvent<std::shared_ptr<Task>>>(evt);
        auto t = e->Arg();
        if (t->callback) {
            t->callback->Done(t);
        }
    }

private:
    class Task;
    class CallbackBase
    {
    public:
        virtual ~CallbackBase() {}
        virtual void PostEvent(std::shared_ptr<Task> t) = 0;
        virtual void Done(std::shared_ptr<Task> t) = 0;
    };
    template<class E, class Succ, class Fail>
    class Callback: public CallbackBase
    {
    public:
        Callback(E* ev, Succ succ, Fail fail): mEv(ev), mSucc(succ), mFail(fail) {}
        void PostEvent(std::shared_ptr<Task> t)
        {
            auto ct = t;
            auto evt = std::make_shared<ArgEvent<std::shared_ptr<Task>>>(EventType::AsyncApiRequest, std::move(ct));
            mEv->PostEvent(evt);
        }
        void Done(std::shared_ptr<Task> t)
        {
            logDebug << "AsyncApiPost done";
            if (t->task->IsCompleted()) {
                if constexpr (std::is_invocable_r_v<void, Succ, HttpResponse&>) {
                    auto ht = static_cast<util::HttpDownloaderTask*>(t->task.get());
                    t->resp.SetStatus(ht->Code());
                    mSucc(t->resp);
                } else if constexpr (std::is_invocable_r_v<void, Succ, json&>) {
                    auto ht = static_cast<util::HttpDownloaderTask*>(t->task.get());
                    bool invalidJson = true;
                    try {
                        auto r = json::parse(t->resp.Body());
                        invalidJson = false;
                        mSucc(r);
                    } catch (std::exception& excp) {
                        logWarn << "ApiClient callback exception:" << excp.what();
                    } catch (...) {
                        logWarn << "ApiClient callback unknown exception";
                    }
                    if (invalidJson) {
                        if constexpr (!std::is_same_v<Fail, std::nullptr_t>) {
                            mFail(ErrorCodeErr);
                        }
                    }
                } else if constexpr (std::is_same_v<Succ, std::nullptr_t>) {
                    // do nothing
                } else {
                    static_assert("unsupported Succ type");
                }
            } else {
                if constexpr (!std::is_same_v<Fail, std::nullptr_t>) {
                    mFail(ErrorCodeErr);
                }
            }
        }

    private:
        E* mEv;
        Succ mSucc;
        Fail mFail;
    };
    struct Task
    {
        ApiClient* client = nullptr;
        std::shared_ptr<util::DownloaderTask> task;
        HttpResponse resp;
        std::shared_ptr<CallbackBase> callback;

        Task() {}
        Task(Task&& oth)
        {
            client = oth.client;
            oth.client = nullptr;
            task = std::move(oth.task);
            resp = std::move(oth.resp);
            callback = std::move(oth.callback);
        }
    };
    std::shared_ptr<Task> getTask(Task* t)
    {
        std::unique_lock lck(mMtx);
        auto it = mTasks.find(t);
        return it == mTasks.end() ? nullptr : it->second;
    }
    bool removeTask(Task* t)
    {
        std::unique_lock lck(mMtx);
        auto it = mTasks.find(t);
        if (it == mTasks.end()) {
            return false;
        }
        mTasks.erase(it);
        return true;
    }
    static void notify(std::shared_ptr<util::DownloaderTask> t, void* userData)
    {
        Task* task = static_cast<Task*>(userData);
        for (auto data = t->Read(); data; data = data->Next()) {
            task->resp.Body().append((const char*)data->Data(), data->Length());
        }
        if (t->IsEnd()) {
            auto st = task->client->getTask(task);
            if (st) {
                st->callback->PostEvent(st);
                task->client->removeTask(task);
            }
        }
    }

private:
    std::mutex mMtx;
    MainManager* mMan;
    util::HttpDownloader* mDownloader;
    std::unordered_map<Task*, std::shared_ptr<Task>> mTasks;
};

NS_END

#endif
