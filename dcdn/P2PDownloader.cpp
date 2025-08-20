#include "P2PDownloader.h"

#include <rtc/rtc.hpp>

#include <memory>
#include <mutex>

#include "MainManager.h"
#include "P2PSingleTask.h"
#include "WebRtcManager.h"
#include "common/Logger.h"

NS_BEGIN(dcdn)
NS_BEGIN(download)

P2PDownloader::P2PDownloader(const Option& option): mOption(option)
{
    Start();
}

P2PDownloader::~P2PDownloader() {}

int P2PDownloader::Init(const util::DownloaderOption* opt)
{
    if (opt == nullptr) {
        logError << "DownloaderOption is null!";
        return -1;
    }

    const Option* p2pOpt = dynamic_cast<const Option*>(opt);
    if (!p2pOpt) {
        logError << "DownloaderOption cast to P2PDownloader::Option failed!";
        return -2;
    }

    {
        std::lock_guard<std::mutex> lock(mMutex);
        mOption = *p2pOpt;
    }
    return 0;
}

std::shared_ptr<util::DownloaderTask> P2PDownloader::CreateTask(const util::DownloaderTaskOption* opt)
{
    auto taskOpt = dynamic_cast<const P2PDownloaderTaskOption*>(opt);
    if (!taskOpt) {
        logError << "Invalid task option provided to P2PDownloader";
        return nullptr;
    }
    auto task = std::make_shared<P2PSingleTask>(this, *taskOpt);

    return task;
}

void P2PDownloader::AddTask(std::shared_ptr<util::DownloaderTask> task)
{
    auto p2pTask = std::dynamic_pointer_cast<P2PSingleTask>(task);
    if (!p2pTask) {
        logError << "Failed to cast task to P2PSingleTask in AddTask";
        return;
    }
    post([p2pTask, this]() { addTask(p2pTask); });
}

void P2PDownloader::initPeerConnection(const P2PDownloaderTaskOption& request, std::shared_ptr<P2PSingleTask> task)
{
    rtc::Configuration config;
    config.enableIceUdpMux = true;
    const auto cp = static_cast<WebRtcManager*>(MainManager::Singlet()->GetWebRtcManager().get())->Cert();
    config.certificatePemFile = cp.certPem;
    config.keyPemFile = cp.keyPem;
    config.iceUfrag = request.IceUfrag;
    config.icePwd = request.IcePwd;
    auto ss = MainManager::Singlet()->Cfg().StunServers();
    for (auto s : ss) {
        auto idx = s.find(':');
        if (idx == std::string::npos) {
            continue;
        }
        std::string host = s.substr(0, idx);
        int port = atoi(s.c_str() + idx + 1);
        if (port > 0 && port < 65536) {
            rtc::IceServer serv(host, port);
            serv.type = rtc::IceServer::Type::Stun;
            config.iceServers.push_back(serv);
        }
    }

    auto pc = std::make_shared<rtc::PeerConnection>(config);
    pc->setRemoteDescription(request.PeerSdp);

    {
        std::lock_guard<std::mutex> lock(mMutex);
        mPeerConnections[request.PeerID] = pc;
    }

    addSingleTask(pc, request, task);

    logDebug << "manager registered task: " << task.get();
}

void P2PDownloader::CancelTask(std::shared_ptr<util::DownloaderTask> task)
{
    post([=]() { cancelTask(task); });
}

void P2PDownloader::PauseTask(std::shared_ptr<util::DownloaderTask> task)
{
    post([=]() { pauseTask(task); });
}

void P2PDownloader::ResumeTask(std::shared_ptr<util::DownloaderTask> task)
{
    post([=]() { resumeTask(task); });
}

void P2PDownloader::post(std::function<void()>&& task)
{
    std::lock_guard<std::mutex> lock(mMutex);
    mThreadTaskQueue.push_back(std::move(task));
    mThreadTaskCv.notify_one();
}

void P2PDownloader::run()
{
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mMutex);
            mThreadTaskCv.wait(lock, [&] { return !mThreadTaskQueue.empty() || mStopFlag; });

            if (mStopFlag && mThreadTaskQueue.empty()) {
                break;
            }

            task = std::move(mThreadTaskQueue.front());
            mThreadTaskQueue.pop_front();
        }

        if (task) {
            task();
        }
    }
}

void P2PDownloader::onTaskDataReceived(
    std::shared_ptr<P2PSingleTask> task,
    std::variant<std::vector<std::byte>, std::string>&& data)
{
    task->handleIncomingDataInternal(std::move(data));
}

void P2PDownloader::addTask(std::shared_ptr<P2PSingleTask> task)
{
    std::shared_ptr<rtc::PeerConnection> peerConn;
    bool needInit = false;
    {
        std::lock_guard<std::mutex> lock(mMutex);
        auto connectionIt = mPeerConnections.find(task->mTaskOpt.PeerID);
        if (connectionIt == mPeerConnections.end()) {
            needInit = true;
        } else {
            peerConn = connectionIt->second;
        }
    }

    if (needInit) {
        logDebug << "create task init connection init for peer: " << task->mTaskOpt.PeerID;
        initPeerConnection(std::move(task->mTaskOpt), task);
    } else {
        addSingleTask(peerConn, task->mTaskOpt, task);
        logDebug << "create task without init connection" << task->mTaskOpt.PeerID;
    }
}

void P2PDownloader::addSingleTask(
    std::shared_ptr<rtc::PeerConnection> pc,
    const P2PDownloaderTaskOption& request,
    std::shared_ptr<P2PSingleTask> task)
{
    std::string label = request.ContentHash + ":" + std::to_string(request.Start) + ":" + std::to_string(request.End);
    logInfo << "create data channel label: " << label;
    auto dc = pc->createDataChannel(label);

    auto taskParams = TaskParam{request.ContentHash, request.Start, request.End};
    task->Init(taskParams, dc);

    {
        std::lock_guard<std::mutex> lock(mMutex);
        mTasks[task.get()] = task;
    }
}

void P2PDownloader::cancelTask(std::shared_ptr<util::DownloaderTask> task)
{
    if (!task) {
        return;
    }
    std::lock_guard<std::mutex> lock(mMutex);
    auto it = mTasks.find(task.get());
    if (it != mTasks.end()) {
        it->second->Cancel();
        mTasks.erase(it);
    }
}

void P2PDownloader::pauseTask(std::shared_ptr<util::DownloaderTask> task)
{
    if (!task) {
        return;
    }
    std::lock_guard<std::mutex> lock(mMutex);
    auto it = mTasks.find(task.get());
    if (it != mTasks.end()) {
        it->second->Pause();
    }
}

void P2PDownloader::resumeTask(std::shared_ptr<util::DownloaderTask> task)
{
    if (!task) {
        return;
    }
    std::lock_guard<std::mutex> lock(mMutex);
    auto it = mTasks.find(task.get());
    if (it != mTasks.end()) {
        it->second->Resume();
    }
}

NS_END
NS_END
