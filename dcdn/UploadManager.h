#ifndef _DCDN_SDK_UPLOAD_MANAGER_H_
#define _DCDN_SDK_UPLOAD_MANAGER_H_

#include <rtc/rtc.hpp>

#include <fstream>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "Cert.h"
#include "FileManager.h"
#include "MainManager.h"

NS_BEGIN(dcdn)

using json = nlohmann::json;

static inline double now()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch())
               .count() /
        1000000.0;
}

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
            mTokens -= static_cast<double>(tokensToConsume);
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
        if (mTokens > rate) {
            mTokens = rate;
        }
    }

private:
    std::mutex mMutex;
    double mTokens{0.0};
    double mLastTime;
};

using TokenBucketPtr = std::shared_ptr<TokenBucket>;

struct UploadFileTask
{
    uint64_t TaskID{0};
    std::string PeerID;
    std::string FileHash;
    std::string FilePath = "";
    size_t BlockStart;
    size_t BlockEnd;
    std::string IceUfrag;
    std::string IcePwd;
    std::string RemoteSdp;

    enum class State
    {
        Idle,
        Running,
        Buffered,
        Paused,
        Cancelled,
        Completed,
        Failed
    };
    std::atomic<State> TaskState = State::Idle;

    std::string ChannelLabel;
    std::shared_ptr<rtc::PeerConnection> Pc;
    std::shared_ptr<rtc::DataChannel> Dc;

    size_t BytesSent = 0;
    size_t FileStart = 0;
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
        this->PeerID = PeerID;
        this->FileHash = FileHash;
        this->BlockStart = Start;
        this->BlockEnd = End;
        this->IceUfrag = IceUfrag;
        this->IcePwd = IcePwd;
        this->RemoteSdp = RemoteSdp;
        this->ChannelLabel = FileHash + ":" + std::to_string(Start) + ":" + std::to_string(End);
    }

    std::string Label()
    {
        return ChannelLabel;
    }

    State GetState()
    {
        return TaskState.load();
    }

    bool SetState(State state)
    {
        State oldState = TaskState.load();
        while (true) {
            if (oldState == state) {
                return true;
            }
            if (oldState == State::Completed || oldState == State::Failed || oldState == State::Cancelled) {
                return false;
            }

            switch (oldState) {
                case State::Idle:
                    if (state != State::Running) {
                        return false;
                    }
                    break;
                case State::Running:
                    if (state != State::Paused && state != State::Cancelled && state != State::Buffered &&
                        state != State::Completed && state != State::Failed) {
                        return false;
                    }
                    break;
                case State::Buffered:
                    if (state != State::Running) {
                        return false;
                    }
                    break;
                case State::Paused:
                    if (state != State::Running) {
                        return false;
                    }
                    break;
                default:
                    return false;
            }

            if (TaskState.compare_exchange_weak(oldState, state)) {
                return true;
            }
        }
    }
};

using UploadFileTaskPtr = std::shared_ptr<UploadFileTask>;

struct PeerConnectionCtx
{
    std::string PeerID;
    std::string IceUfrag;
    std::string IcePwd;
    std::string RemoteSdp;
    double LastTime;

    PeerConnectionCtx(
        const std::string& peerID,
        const std::string& iceUfrag,
        const std::string& icePwd,
        const std::string& remoteSdp)
        : PeerID(peerID), IceUfrag(iceUfrag), IcePwd(icePwd), RemoteSdp(remoteSdp), LastTime(now())
    {
    }

    void UpdateTime()
    {
        LastTime = now();
    }
};

using PeerConnectionCtxPtr = std::shared_ptr<PeerConnectionCtx>;

class UploadManager: public BaseManager, public EventLoop<UploadManager>
{
public:
    UploadManager(MainManager* man, FileManager* mf, CertificatePair cert);
    ~UploadManager();

    void SetMaxBufferedAmount(uint64_t amount)
    {
        mMaxBufferedAmount.store(amount);
    }

    void SetBufferedThresholdRate(double rate)
    {
        mBufferedThresholdRate.store(rate);
    }

private:
    void run();

    void handleUploadMsgEvent(std::shared_ptr<Event> evt);
    void handleDataChannelEvent(std::shared_ptr<Event> evt);

    bool initFileOperations(UploadFileTaskPtr task);
    void performFileSending(UploadFileTaskPtr task);

    std::optional<std::shared_ptr<rtc::PeerConnection>>
    setupPeerConnection(const std::string& iceUfrag, const std::string& icePwd, const std::string& remoteSdp);
    void handleDataChannel(std::shared_ptr<rtc::DataChannel> dc, std::shared_ptr<rtc::PeerConnection> pc);

    void setupOnOpenCallback(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr channelTask);
    void setupOnBufferedAmountLowCallback(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr channelTask);
    void setupOnMessageCallback(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr channelTask);
    void setupOnClosedCallback(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr channelTask);

    std::optional<std::tuple<std::string, size_t, size_t>> parseLabel(const std::string& label);

    uint64_t getTaskID()
    {
        std::lock_guard<std::mutex> lock(mNextTaskMutex);
        return mNextTask++;
    }

    void addActiveTask(UploadFileTaskPtr task)
    {
        std::lock_guard<std::mutex> lock(mActiveTaskMutex);
        bool exists = false;
        for (const auto& t : mActiveTask) {
            if (t->TaskID == task->TaskID) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            logInfo << "Add active task: " << task->FileHash << " from block: " << task->BlockStart
                    << " to block: " << task->BlockEnd;
            mActiveTask.push_back(task);
        }
    }

    void removeActiveTask(uint64_t taskID)
    {
        std::lock_guard<std::mutex> lock(mActiveTaskMutex);
        auto it = std::remove_if(mActiveTask.begin(), mActiveTask.end(), [taskID](const UploadFileTaskPtr& t) {
            return t->TaskID == taskID;
        });
        if (it != mActiveTask.end()) {
            logInfo << "Remove active task: " << taskID;
            mActiveTask.erase(it, mActiveTask.end());
        }
    }

    void removeTask(uint64_t taskID)
    {
        {
            std::lock_guard<std::mutex> lock(mTaskMapMutex);
            if (mTaskMap.find(taskID) != mTaskMap.end()) {
                logInfo << "Remove task: " << taskID;
                mTaskMap.erase(taskID);
            }
        }

        removeActiveTask(taskID);
    }

private:
    FileManager* mFileMgr;
    CertificatePair mCert;
    TokenBucketPtr mTokenBucket;

    std::mutex mNextTaskMutex;
    uint64_t mNextTask = 0;

    std::atomic_uint64_t mMaxBufferedAmount{10 * 1024 * 1024};
    std::atomic<double> mBufferedThresholdRate{0.5};

    std::mutex mPeerConnectionMapMutex;
    std::unordered_map<std::shared_ptr<rtc::PeerConnection>, PeerConnectionCtxPtr> mPeerConnectionMap;

    std::mutex mTaskMapMutex;
    std::unordered_map<uint64_t, UploadFileTaskPtr> mTaskMap;

    std::mutex mActiveTaskMutex;
    std::vector<UploadFileTaskPtr> mActiveTask;

    std::atomic<bool> mStopFlag{false};
};

NS_END

#endif