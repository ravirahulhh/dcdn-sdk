#ifndef _DCDN_UTIL_DOWNLOADER_H_
#define _DCDN_UTIL_DOWNLOADER_H_

#include <any>
#include <list>
#include <string>
#include <thread>
#include <atomic>

#include "common/Common.h"

NS_BEGIN(dcdn)
NS_BEGIN(util)

class DownloaderTask;
struct DownloaderTaskOption
{
    size_t Start = 0;
    size_t End = 0;
    size_t MaxBuf = 0;

    void (*Notify)(std::shared_ptr<DownloaderTask> task, void* recevier) = nullptr;
    void* Receiver = nullptr;

    virtual ~DownloaderTaskOption() {}
};

struct DownloaderOption
{
    virtual ~DownloaderOption() {}
};

class DownloaderTaskBuffer
{
public:
    virtual ~DownloaderTaskBuffer() {}
    std::shared_ptr<DownloaderTaskBuffer> Next()
    {
        return mNext;
    }
    void Concat(std::shared_ptr<DownloaderTaskBuffer> buf)
    {
        mNext = buf;
    }
    virtual size_t Offset() const = 0;
    virtual const unsigned char* Data() const = 0;
    virtual size_t Length() const = 0;

protected:
    std::shared_ptr<DownloaderTaskBuffer> mNext;
};

template<class T>
class DownloaderTaskContainerBuffer: public DownloaderTaskBuffer
{
public:
    size_t Offset() const
    {
        return mOffset;
    }
    const unsigned char* Data() const
    {
        return (const unsigned char*)mData.data();
    }
    size_t Length() const
    {
        return mData.size();
    }
    template<class InputIt>
    void Set(size_t offset, InputIt first, InputIt last)
    {
        mOffset = offset;
        mData.assign(first, last);
    }
    void Set(size_t offset, T&& dat)
    {
        mOffset = offset;
        mData = std::move(dat);
    }

private:
    size_t mOffset = 0;
    T mData;
};

class DownloaderTask
{
public:
    enum StatusType
    {
        Cancelled = -2,
        Fail = -1,
        Idle = 0,
        Running,
        Paused,
        Completed,
    };
    DownloaderTask() {}
    virtual ~DownloaderTask() {}
    StatusType Status() const
    {
        return mStatus;
    }
    virtual const DownloaderTaskOption* Option() const
    {
        return nullptr;
    }
    bool IsEnd() const
    {
        auto st = Status();
        return st < Idle || st >= Completed;
    }
    bool IsCompleted() const
    {
        auto st = Status();
        return st == Completed;
    }
    size_t Size() const
    {
        return mSize;
    }
    bool HasData() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        return mDataHead != nullptr;
    }
    std::shared_ptr<DownloaderTaskBuffer> Read()
    {
        std::unique_lock<std::mutex> lck(mMtx);
        auto res = mDataHead;
        mDataHead = nullptr;
        mDataTail = nullptr;
        mBufSize = 0;
        return res;
    }

protected:
    void notify(std::shared_ptr<DownloaderTask> t)
    {
        if (auto opt = Option()) {
            if (opt->Notify) {
                opt->Notify(t, opt->Receiver);
            }
        }
    }
    void setStatus(StatusType st)
    {
        std::unique_lock<std::mutex> lck(mMtx);
        mStatus = st;
    }
    bool cancel()
    {
        std::unique_lock<std::mutex> lck(mMtx);
        if (mStatus == Idle || mStatus == Running) {
            mStatus = Cancelled;
            return true;
        }
        return false;
    }
    bool pause()
    {
        std::unique_lock<std::mutex> lck(mMtx);
        if (mStatus == Idle || mStatus == Running) {
            mStatus = Paused;
            return true;
        }
        return false;
    }
    bool resume()
    {
        std::unique_lock<std::mutex> lck(mMtx);
        if (mStatus == Paused) {
            mStatus = Running;
            return true;
        }
        return false;
    }
    bool outOfBuf() const
    {
        if (auto opt = Option()) {
            std::unique_lock<std::mutex> lck(mMtx);
            return opt->MaxBuf > 0 && mBufSize > opt->MaxBuf;
        }
        return false;
    }
    void append(std::shared_ptr<DownloaderTaskBuffer> data)
    {
        std::unique_lock<std::mutex> lck(mMtx);
        if (mDataTail) {
            mDataTail->Concat(data);
        } else {
            mDataHead = data;
        }
        mDataTail = data;
        mSize += data->Length();
        mBufSize += data->Length();
    }

protected:
    mutable std::mutex mMtx;

private:
    std::atomic<StatusType> mStatus = Idle;
    std::shared_ptr<DownloaderTaskBuffer> mDataHead;
    std::shared_ptr<DownloaderTaskBuffer> mDataTail;
    std::atomic<size_t> mSize = 0;
    size_t mBufSize = 0;
};

class BaseDownloader
{
public:
    virtual ~BaseDownloader() {}
    virtual int Init(const DownloaderOption* opt) = 0;
    void Start(bool detach = true)
    {
        mThread = std::make_shared<std::thread>([&]() { run(); });
        if (detach) {
            mThread->detach();
        }
    }
    std::shared_ptr<std::thread> Thread()
    {
        return mThread;
    }
    virtual std::shared_ptr<DownloaderTask> CreateTask(const DownloaderTaskOption* opt) = 0;
    virtual void AddTask(std::shared_ptr<DownloaderTask> task) = 0;
    virtual void CancelTask(std::shared_ptr<DownloaderTask> task) = 0;
    virtual void PauseTask(std::shared_ptr<DownloaderTask> task) = 0;
    virtual void ResumeTask(std::shared_ptr<DownloaderTask> task) = 0;

protected:
    enum class EventType
    {
        AddTask,
        CancelTask,
        PauseTask,
        ResumeTask,
    };
    typedef std::pair<EventType, std::any> Event;
    template<class T>
    void postEvent(EventType tp, const T& arg)
    {
        std::unique_lock<std::mutex> lck(mMtx);
        auto e = std::make_shared<Event>(tp, arg);
        mEvts.push_back(e);
    }

    virtual void run() = 0;

protected:
    std::shared_ptr<std::thread> mThread;
    std::mutex mMtx;
    std::list<std::shared_ptr<Event>> mEvts;
};

NS_END
NS_END

#endif
