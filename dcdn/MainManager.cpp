#include "MainManager.h"

#include <plog/Initializers/RollingFileInitializer.h>

#include <filesystem>

#include "DeployManager.h"
#include "DownloadManager.h"
#include "FileManager.h"
#include "UploadManager.h"
#include "WebRtcManager.h"
#include "WebSocketManager.h"
#include "util/HttpDownloader.h"

NS_BEGIN(dcdn)

std::atomic<MainManager*> MainManager::singlet = nullptr;

int MainManager::Init(const MainManagerOption& opt)
{
    std::filesystem::create_directories(opt.WorkDir);
    std::filesystem::path logFile(opt.WorkDir);
    logFile.append("dcdn.log");
    plog::init<DCDN_LOGGER_ID>(plog::debug, logFile.c_str());
    logInfo << "MainManager init";
    rtc::InitLogger(rtc::LogLevel::Debug);
    MainManager* n = nullptr;
    MainManager* m = new MainManager();
    if (!singlet.compare_exchange_strong(n, m)) {
        delete m;
        return 0;
    }
    return m->init(opt);
}

MainManager::MainManager(): BaseManager(this)
{
    mHttpDownloader = std::make_shared<util::HttpDownloader>();
    mApiClient = std::make_shared<ApiClient>(mHttpDownloader.get());

    mWebSkt = std::make_shared<WebSocketManager>(this);
    mWebRtc = std::make_shared<WebRtcManager>(this);
    mFileMgr = std::make_shared<FileManager>(this);
    mUploadMgr = std::make_shared<UploadManager>(this);
    mDeployMgr = std::make_shared<DeployManager>(this);
    mDownloadMgr = std::make_shared<dcdn::DownloadManager>();

    RegisterGlobalHandler(
        EventType::AsyncApiRequest,
        [](std::shared_ptr<Event> evt, void* userData) {
            auto cli = static_cast<ApiClient*>(userData);
            cli->HandleAsyncApiRequestEvent(evt);
        },
        mApiClient.get());

    registerHandler(EventType::UploadMsg, &MainManager::handleUploadMsgEvent);
    registerHandler(EventType::DeployMsg, &MainManager::handleDeployMsgEvent);
}

int MainManager::init(const MainManagerOption& opt)
{
    mOpt = opt;
    int ret = mCfg.CreateTable(opt.WorkDir);
    if (ret != ErrorCodeOk) {
        logError << "init config fail";
        return ret;
    }
    ret = mHttpDownloader->Init(nullptr);
    if (ret != ErrorCodeOk) {
        logError << "init HttpDownloader fail";
        return ret;
    }
    mCfg.LoadFromDB();
    auto peerId = mCfg.PeerId();
    if (peerId.empty()) {
        // TODO: generate PeerId
    }

    // Init FileManager
    FileManagerOption fileMgrOpt = FileManagerOption();
    fileMgrOpt.RootPath = std::filesystem::path(mOpt.WorkDir).append("download").string();
    if (static_cast<FileManager*>(mFileMgr.get())->Init(fileMgrOpt) != 0) {
        LOGE << "Failed to initialize FileManager";
        return -1;
    }

    DeployManagerOption deployOpt;
    deployOpt.fileMgr = std::dynamic_pointer_cast<FileManager>(this->getFileManager());
    deployOpt.downloadMgr = std::dynamic_pointer_cast<DownloadManager>(this->getDownloadManager());

    // 调用 DeployManager 的 Init 方法
    if (static_cast<DeployManager*>(mDeployMgr.get())->Init(deployOpt) != 0) {
        LOGE << "Failed to initialize DeployManager";
        return -1;
    }
    return ErrorCodeOk;
}

MainManager::~MainManager() {}

int MainManager::ApiPost(util::HttpClient& cli, const char* uri, json& arg, util::HttpResponse* resp)
{
    std::string url = mCfg.ApiRootUrl();
    url += uri;
    util::HttpRequest req(url.c_str(), arg.dump(), "application/json");
    int ret = cli.Do(req, resp);
    return ret;
}

int MainManager::ApiPost(util::HttpClient& cli, const char* uri, json& arg, json& result)
{
    util::HttpResponse resp;
    int ret = ApiPost(cli, uri, arg, &resp);
    if (ret == ErrorCodeOk) {
        try {
            auto r = json::parse(resp.Body());
            result = std::move(r);
        } catch (std::exception& excp) {
            result.clear();
            ret = ErrorCodeErr;
        } catch (...) {
            result.clear();
            ret = ErrorCodeErr;
        }
    }
    return ErrorCodeOk;
}

void MainManager::login()
{
    json msg;
    msg["device_id"] = mOpt.DeviceId;
    msg["apikey"] = mOpt.ApiKey;

    AsyncApiPost(
        nullptr,
        "/api/v1/login",
        msg,
        this,
        [this](json& res) {
            try {
                auto data = res["data"];
                std::string token = data["token"];
                mCfg.SetToken(token);
            } catch (std::exception& excp) {
            } catch (...) {
            }
        },
        nullptr);
}

void MainManager::run()
{
    logInfo << "MainManager running";
    mHttpDownloader->Start();
    login();
    mWebRtc->Start();
    mWebSkt->Start();
    mFileMgr->Start();
    mUploadMgr->Start();
    mDeployMgr->Start();
    // mDownloadMgr->Start();
    while (true) {
        waitAllEvents(std::chrono::milliseconds(1000));
    }
    logInfo << "MainManager exit";
}

void MainManager::handleAsyncApiRequestEvent(std::shared_ptr<Event> evt)
{
    mApiClient->HandleAsyncApiRequestEvent(evt);
}

void MainManager::handleUploadMsgEvent(std::shared_ptr<Event> evt)
{
    if (mUploadMgr) {
        static_cast<UploadManager*>(mUploadMgr.get())->PostEvent(evt);
    }
}

void MainManager::handleDeployMsgEvent(std::shared_ptr<Event> evt)
{
    if (mDeployMgr) {
        static_cast<DeployManager*>(mDeployMgr.get())->PostEvent(evt);
    }
}

std::shared_ptr<BaseManager> MainManager::getFileManager() const
{
    return mFileMgr;
}

std::shared_ptr<dcdn::DownloadManager> MainManager::getDownloadManager() const
{
    return mDownloadMgr;
}

std::shared_ptr<BaseManager> MainManager::getDeployManager() const
{
    return mDeployMgr;
}

NS_END
