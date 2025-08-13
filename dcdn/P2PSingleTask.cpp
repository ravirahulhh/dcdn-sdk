#include "P2PSingleTask.h"

#include <cassert>
#include <memory>
#include <string>
#include <variant>

#include "common/Logger.h"
#include "util/Downloader.h"

NS_BEGIN(dcdn)
NS_BEGIN(download)

P2PSingleTask::P2PSingleTask(P2PDownloader* manager, P2PDownloaderTaskOption opt): mManager(manager), mTaskOpt(opt)
{
    setStatus(DownloaderTask::Idle);
}

P2PSingleTask::~P2PSingleTask()
{
    Cancel();
}

void P2PSingleTask::Init(TaskParam param, std::shared_ptr<rtc::DataChannel> dc)
{
    if (Status() != DownloaderTask::Idle) {
        logWarn << "P2PSingleTask is not idle, init abort, task content hash: " << param.ContentHash
                << ", start: " << param.Start << ", end: " << param.End;
        return;
    }

    mContentHash = param.ContentHash;
    mStart = param.Start;
    mEnd = param.End;
    mTotalSize = param.End - param.Start;
    mDc = dc;

    logDebug << "P2PSingleTask init, task content hash: " << param.ContentHash << ", start: " << param.Start
             << ", end: " << param.End << ", total size: " << mTotalSize;

    mDc->onOpen([this]() {
        logDebug << "DataChannel opened for task: " << mContentHash;
        this->Start();
    });

    mDc->onClosed([this]() {
        logWarn << "DataChannel closed for task: " << mContentHash;
        if (this->Status() != DownloaderTask::Completed) {
            this->setStatus(DownloaderTask::Fail);
            this->notify(shared_from_this());
        }
    });

    mDc->onMessage(
        [this](std::variant<std::vector<std::byte>, std::string> data) { HandleIncomingData(std::move(data)); });
}

void P2PSingleTask::Start()
{
    if (Status() != DownloaderTask::Idle) {
        return;
    }

    setStatus(DownloaderTask::Running);
    notify(shared_from_this());

    logDebug << "Started P2PSingleTask. TaskId: " << mTaskID;
}

bool P2PSingleTask::Pause()
{
    if (pause()) {
        if (mDc && mDc->isOpen()) {
            mDc->send("PAUSE");
        }
        notify(shared_from_this());
        return true;
    }
    return false;
}

bool P2PSingleTask::Resume()
{
    if (resume()) {
        if (mDc && mDc->isOpen()) {
            mDc->send("RESUME");
        }
        notify(shared_from_this());
        return true;
    }
    return false;
}

bool P2PSingleTask::Cancel()
{
    if (cancel()) {
        if (mDc && mDc->isOpen()) {
            mDc->send("CANCEL");
            mDc->close();
        }
        notify(shared_from_this());
        return true;
    }
    return false;
}

void P2PSingleTask::HandleIncomingData(std::variant<std::vector<std::byte>, std::string>&& data)
{
    mManager->onTaskDataReceived(shared_from_this(), std::move(data));
}

void P2PSingleTask::handleIncomingDataInternal(std::variant<std::vector<std::byte>, std::string>&& data)
{
    if (Status() == DownloaderTask::Fail || Status() == DownloaderTask::Cancelled) {
        return;
    }

    if (std::holds_alternative<std::vector<std::byte>>(data)) {
        const auto& bytes = std::get<std::vector<std::byte>>(data);

        if (bytes.empty()) {
            return;
        }

        // 使用绝对偏移量（任务起始位置 + 已下载大小）
        uint64_t currentOffset = mStart + mDownloaded;
        auto buf = std::make_shared<util::DownloaderTaskContainerBuffer<std::vector<std::byte>>>();
        buf->Set(currentOffset, bytes.begin(), bytes.end()); // 使用绝对偏移

        append(buf); // add to task buffer
        mDownloaded += bytes.size();

        notify(shared_from_this());

        double progress = static_cast<double>(Size()) / mTotalSize;
        logDebug << "Received data. Progress: " << (progress * 100) << "%";

        if (progress >= 0.95 && mDc && mDc->isOpen()) {
            mDc->send("STOP");
            logDebug << "Sent STOP to peer, buffer near full. TaskId: " << mTaskID;
        }

        if (Size() >= mTotalSize) {
            setStatus(DownloaderTask::Completed);
            notify(shared_from_this());
        }

    } else if (std::holds_alternative<std::string>(data)) {
        const std::string& msg = std::get<std::string>(data);

        if (msg == "PAUSE_ACK") {
            setStatus(DownloaderTask::Paused);
            logDebug << "Received PAUSE_ACK. TaskId: " << mTaskID;
        } else if (msg == "CANCEL_ACK") {
            setStatus(DownloaderTask::Cancelled);
            logDebug << "Received CANCEL_ACK. TaskId: " << mTaskID;
        } else {
            logWarn << "Unhandled control message: " << msg;
        }

        notify(shared_from_this());
    }
}

NS_END
NS_END
