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

public:
    ApiClient(util::HttpDownloader* downloader): mDownloader(downloader) {}
    template<class E, class Succ = void (*)(long, std::string&), class Fail = void (*)(long)>
    int Post(const char* uri, json& arg, E* ev, Succ succ, Fail fail)
    {
        auto task = std::make_shared<Task>();
        task->client = this;
        util::HttpDownloaderTaskOption opt;
        opt.Request = std::make_shared<util::HttpRequest>(uri, arg.dump(), "application/json");
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
        return ErrorCodeOk;
    }
    void HandleAsyncApiRequestEvent(std::shared_ptr<Event> evt)
    {
        auto e = std::static_pointer_cast<ArgEvent<std::shared_ptr<Task>>>(evt);
        auto t = e->Arg();
        if (t->callback) {
            t->callback->Do(t);
        }
    }

private:
    class Task;
    class CallbackBase
    {
    public:
        virtual ~CallbackBase() {}
        virtual void PostEvent(std::shared_ptr<Task> t) = 0;
        virtual void Do(std::shared_ptr<Task> t) = 0;
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
        void Do(std::shared_ptr<Task> t)
        {
            if (t->task->IsCompleted()) {
                if constexpr (!std::is_same_v<Succ, std::nullptr_t>) {
                    mSucc(t->code, t->response);
                }
            } else {
                if constexpr (!std::is_same_v<Fail, std::nullptr_t>) {
                    mFail(t->code);
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
        long code = 0;
        std::string response;
        std::shared_ptr<CallbackBase> callback;

        Task() {}
        Task(Task&& oth)
        {
            client = oth.client;
            oth.client = nullptr;
            task = std::move(oth.task);
            code = oth.code;
            oth.code = 0;
            response = std::move(oth.response);
            callback = std::move(oth.callback);
        }
    };
    std::shared_ptr<Task> getTask(Task* t)
    {
        std::unique_lock lck(mMtx);
        auto it = mTasks.find(t);
        return it == mTasks.end() ? nullptr : it->second;
    }
    void removeTask(Task* t)
    {
        std::unique_lock lck(mMtx);
        mTasks.erase(t);
    }
    static void notify(std::shared_ptr<util::DownloaderTask> t, void* userData)
    {
        Task* task = static_cast<Task*>(userData);
        if (!task->task) {
            task->task = t;
        }
        for (auto data = t->Read(); data; data = data->Next()) {
            task->response.append((const char*)data->Data(), data->Length());
        }
        if (t->IsEnd()) {
            if (t->IsCompleted()) {
                auto st = task->client->getTask(task);
                if (st) {
                    st->callback->PostEvent(st);
                }
            } else {
            }
        }
        task->client->removeTask(task);
    }

private:
    std::mutex mMtx;
    MainManager* mMan;
    util::HttpDownloader* mDownloader;
    std::unordered_map<Task*, std::shared_ptr<Task>> mTasks;
    // std::unordered_map<util::HttpDownloaderTask*, std::shared_ptr<Task>>
    // mDownloaderTasks;
};

NS_END

#endif
