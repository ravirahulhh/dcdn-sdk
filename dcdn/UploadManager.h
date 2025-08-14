#ifndef _DCDN_SDK_UPLOAD_MANAGER_H_
#define _DCDN_SDK_UPLOAD_MANAGER_H_

#include <rtc/rtc.hpp>

#include <condition_variable>
#include <fstream>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "Cert.h"
#include "FileManager.h"
#include "MainManager.h"

NS_BEGIN(dcdn)

class TokenBucket
{
public:
    TokenBucket(): mLastTime(now()) {}

    size_t Consume(size_t tokens)
    {
        std::unique_lock<std::mutex> lock(mMutex);
        auto rate = MainManager::Singlet()->Cfg().UploadRate();
        refreshTokens(rate);

        size_t availableTokens = static_cast<size_t>(mTokens);
        size_t tokensToConsume = std::min(tokens, availableTokens);

        if (tokensToConsume > 0) {
            mTokens -= tokensToConsume;
        }

        return tokensToConsume;
    }

private:
    void refreshTokens(uint64_t rate)
    {
        auto currentTime = now();
        double elapsed = currentTime - mLastTime;
        mLastTime = currentTime;

        mTokens += elapsed * rate;
        if (mTokens > 2 * rate) {
            mTokens = 2 * rate;
        }
    }

    static double now()
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
                   .count() /
            1000000.0;
    }

private:
    std::mutex mMutex;
    double mTokens = 0;
    double mLastTime;
};

using TokenBucketPtr = std::shared_ptr<TokenBucket>;

struct UploadFileTask
{
    std::string PeerID;
    std::string FileHash;
    std::string FilePath = "";
    size_t BlockStart;
    size_t BlockEnd;
    std::string IceUfrag;
    std::string IcePwd;
    std::string RemoteSdp;

    std::string ChannelLabel;
    std::shared_ptr<rtc::PeerConnection> Pc;
    std::shared_ptr<rtc::DataChannel> Dc;

    enum State
    {
        Init,
        Pending,
        Running,
        Paused,
        Failed,
        Cancelled,
        Completed,
    };
    std::mutex StateMutex;
    State TaskState;

    size_t BytesSent = 0;
    size_t FileOffset = 0;
    size_t FileEnd = 0;
    std::ifstream File;

    UploadFileTask(
        const std::string& PeerID,
        const std::string& FileHash,
        uint64_t Start,
        uint64_t End,
        const std::string& IceUfrag,
        const std::string& IcePwd,
        const std::string& RemoteSdp)
    {
        TaskState = Init;
        this->PeerID = PeerID;
        this->FileHash = FileHash;
        this->BlockStart = Start;
        this->BlockEnd = End;
        this->IceUfrag = IceUfrag;
        this->IcePwd = IcePwd;
        this->RemoteSdp = RemoteSdp;
        this->ChannelLabel = FileHash + "-" + std::to_string(Start) + "-" + std::to_string(End);
    }

    std::string Label()
    {
        return ChannelLabel;
    }

    State GetState()
    {
        std::lock_guard<std::mutex> lock(StateMutex);
        return TaskState;
    }

    void SetState(State newState)
    {
        std::lock_guard<std::mutex> lock(StateMutex);
        TaskState = newState;
    }
};

using UploadFileTaskPtr = std::shared_ptr<UploadFileTask>;

class UploadManager: public BaseManager, public EventLoop<UploadManager>
{
public:
    UploadManager(MainManager* man, FileManager* mf, CertificatePair cert);
    ~UploadManager();

    void Start(bool detach = true);
    std::shared_ptr<std::thread> Thread()
    {
        return mThread;
    }

public:
    void run();
    void stop();
    void processTasks();
    void setupPeerConnection(UploadFileTaskPtr task);
    void handleDataChannel(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr task);
    void handleUploadMsgEvent(std::shared_ptr<Event> evt);

private:
    FileManager* mFileMgr;
    CertificatePair mCert;
    TokenBucketPtr mTokenBucket;

    std::mutex mLabelTaskMapMutex;
    std::unordered_map<std::string, UploadFileTaskPtr> mLabelTaskMap;
    std::mutex mTaskMutex;
    std::queue<UploadFileTaskPtr> mTaskQueue;
    std::condition_variable mTaskCond;

    std::atomic<bool> mStopFlag{false};
    std::shared_ptr<std::thread> mThread;
};

NS_END

#endif
