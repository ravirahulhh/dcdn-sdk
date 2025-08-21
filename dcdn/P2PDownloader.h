#ifndef _DCDN_SDK_P2P_DOWNLOADER_H_
#define _DCDN_SDK_P2P_DOWNLOADER_H_

#include <rtc/peerconnection.hpp>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "BaseManager.h"
#include "util/Downloader.h"

NS_BEGIN(dcdn)
NS_BEGIN(download)

class P2PSingleTask;

struct P2PDownloaderTaskOption: public util::DownloaderTaskOption
{
    std::string PeerID;
    std::string IceUfrag;
    std::string IcePwd;
    std::string PeerSdp;
    std::string FileHash;
    size_t Start = 0;
    size_t End = 0;
};

class P2PDownloader: public BaseManager, public util::BaseDownloader
{
public:
    class Option: public util::DownloaderOption
    {
    public:
        std::string ConnectionTimeout;
        std::string MaxPeerConnectionIdleTime;
    };

    explicit P2PDownloader(MainManager* man, const Option& option);
    P2PDownloader(P2PDownloader&&) = delete;
    P2PDownloader& operator=(P2PDownloader&&) = delete;
    ~P2PDownloader();

    int Init(const util::DownloaderOption* opt) override;

    std::shared_ptr<util::DownloaderTask> CreateTask(const util::DownloaderTaskOption* opt) override;
    void AddTask(std::shared_ptr<util::DownloaderTask> task) override;
    void CancelTask(std::shared_ptr<util::DownloaderTask> task) override;
    void PauseTask(std::shared_ptr<util::DownloaderTask> task) override;
    void ResumeTask(std::shared_ptr<util::DownloaderTask> task) override;

    using util::BaseDownloader::Start;
    using util::BaseDownloader::Thread;

private:
    friend class P2PSingleTask;
    void onTaskDataReceived(
        std::shared_ptr<P2PSingleTask> task,
        std::variant<std::vector<std::byte>, std::string>&& data);

    bool addTask(std::shared_ptr<P2PSingleTask> task);
    bool addSingleTask(
        std::shared_ptr<rtc::PeerConnection> pc,
        const P2PDownloaderTaskOption& request,
        std::shared_ptr<P2PSingleTask> task);
    void cancelTask(std::shared_ptr<util::DownloaderTask> task);
    void pauseTask(std::shared_ptr<util::DownloaderTask> task);
    void resumeTask(std::shared_ptr<util::DownloaderTask> task);

    std::optional<std::shared_ptr<rtc::PeerConnection>> initPeerConnection(
        const P2PDownloaderTaskOption& taskOpt,
        std::shared_ptr<P2PSingleTask> task);

    void post(std::function<void()>&& task);
    void run() override;

private:
    Option mOption;
    mutable std::mutex mMutex;

    using util::BaseDownloader::mThread;

    std::unordered_map<std::string, std::shared_ptr<rtc::PeerConnection>> mPeerConnections;
    std::unordered_map<const util::DownloaderTask*, std::shared_ptr<P2PSingleTask>> mTasks;

    std::condition_variable mThreadTaskCv;
    std::deque<std::function<void()>> mThreadTaskQueue;

    std::atomic<bool> mStopFlag = false;
};

NS_END
NS_END

#endif
