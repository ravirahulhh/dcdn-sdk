#ifndef _DCDN_SDK_MAIN_MANAGER_H_
#define _DCDN_SDK_MAIN_MANAGER_H_

#include <nlohmann/json.hpp>
#include <sqlite3.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "ApiClient.h"
#include "BaseManager.h"
#include "Config.h"
#include "EventLoop.h"
#include "util/HttpClient.h"

NS_BEGIN(dcdn)

// 前向声明，避免编译错误
class FileManager;
class DownloadManager;

struct MainManagerOption
{
    std::string WorkDir;
    std::string DeviceId;
    std::string ApiKey;
};

class MainManager: public BaseManager, public EventLoop<MainManager>
{
public:
    using json = nlohmann::json;

public:
    const MainManagerOption& Option() const
    {
        return mOpt;
    }
    const Config& Cfg() const
    {
        return mCfg;
    }

    int ApiPost(util::HttpClient& cli, const char* uri, json& arg, util::HttpResponse* resp);
    int ApiPost(util::HttpClient& cli, const char* uri, json& arg, json& result);

    /**************************
     *
     * ev is one of below:
     *   an EventLoop object: it must handle AsyncApiRequest Event,
     *    the callback will run in the ev's thread
     *   nullptr: the callback will run in the ApiClient's thread
     *
     * Succ is one of below:
     *   void (*succ)(util::HttpResponse& resp)
     *   void (*succ)(json& res)
     *   nullptr
     *
     * Fail is one of below:
     *   void (*fail)(int code)
     *   nullptr
     *
     *************************/
    template<class E, class Succ, class Fail>
    int AsyncApiPost(void** reqId, const char* uri, json& arg, E* ev, Succ succ, Fail fail)
    {
        logDebug << "AsyncApiPost uri:" << uri;
        std::string url = mCfg.ApiRootUrl();
        url += uri;
        return mApiClient->Do(reqId, {url, arg.dump(), "application/json"}, ev, succ, fail);
    }

    bool CancelAsyncApiPost(void* reqId)
    {
        return mApiClient->Cancel(reqId);
    }

    std::shared_ptr<FileManager> getFileManager() const;
    std::shared_ptr<DownloadManager> getDownloadManager() const;

private:
    void run();

public:
    static int Init(const MainManagerOption& opt);
    static MainManager* Singlet()
    {
        return singlet;
    }

private:
    MainManager();
    MainManager(const MainManager&) = delete;
    MainManager& operator=(const MainManager&) = delete;
    ~MainManager();
    int init(const MainManagerOption& opt);
    static std::atomic<MainManager*> singlet;

    void login();

    void handleAsyncApiRequestEvent(std::shared_ptr<Event> evt);
    void handleUploadMsgEvent(std::shared_ptr<Event> evt);
    void handleDeployMsgEvent(std::shared_ptr<Event> evt);

private:
    std::mutex mMtx;
    std::condition_variable mCv;

    MainManagerOption mOpt;
    Config mCfg;

    util::HttpClient mClient;
    std::shared_ptr<util::HttpDownloader> mHttpDownloader;
    std::shared_ptr<ApiClient> mApiClient;

    std::shared_ptr<BaseManager> mWebSkt;
    std::shared_ptr<BaseManager> mWebRtc;
    std::shared_ptr<BaseManager> mFileMgr;
    std::shared_ptr<BaseManager> mUploadMgr;
    std::shared_ptr<BaseManager> mDownloadMgr;
    std::shared_ptr<BaseManager> mDeployMgr;
};

NS_END

#endif
