#ifndef _DCDN_SDK_P2P_SINGLE_TASK_H_
#define _DCDN_SDK_P2P_SINGLE_TASK_H_

#include <rtc/rtc.hpp>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <variant>
#include <vector>

#include "P2PDownloader.h"
#include "util/Downloader.h"

NS_BEGIN(dcdn)
NS_BEGIN(download)

typedef unsigned long long TaskId;

struct DownloadRequest
{
    std::string Url;
    std::string FileHash;
    uint64_t Start;
    uint64_t End;
};

struct TaskParam
{
    std::string ContentHash;
    uint64_t Start;
    uint64_t End;
};

class P2PSingleTask: public util::DownloaderTask, public std::enable_shared_from_this<P2PSingleTask>
{
public:
    P2PSingleTask(P2PDownloader* manager, P2PDownloaderTaskOption opt);
    P2PSingleTask(P2PSingleTask&&) = delete;
    P2PSingleTask& operator=(P2PSingleTask&&) = delete;
    ~P2PSingleTask();

    void Start();
    bool Pause();
    bool Resume();
    bool Cancel();

    void Init(TaskParam param, std::shared_ptr<rtc::DataChannel> dc);
    void HandleIncomingData(std::variant<std::vector<std::byte>, std::string>&& data);

private:
    void handleIncomingDataInternal(std::variant<std::vector<std::byte>, std::string>&& data);

private:
    friend class P2PDownloader;
    size_t mNextReadOffset = 0;

    TaskId mTaskID = 0;
    std::string mContentHash = "";
    uint64_t mStart = 0;
    uint64_t mEnd = 0;
    size_t mTotalSize = 0;
    P2PDownloaderTaskOption mTaskOpt;

    std::shared_ptr<rtc::DataChannel> mDc;
    std::atomic<size_t> mDownloaded{0};
    std::atomic<int> mLastError{0};

    mutable std::mutex mDataMutex;
    std::condition_variable mDataAvailableCv;
    std::condition_variable mBufferFreedCv;

    size_t mMaxBufferSize = 10 * 1024 * 1024;

    P2PDownloader* mManager;
};

NS_END
NS_END

#endif
