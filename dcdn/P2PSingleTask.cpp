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

void P2PSingleTask::postState(DownloaderTask::StatusType newState)
{
    mManager->post([=]() {
        setStatus(newState);
        notify(shared_from_this());
    });
}

void P2PSingleTask::Init(TaskParam param, std::shared_ptr<rtc::DataChannel> dc)
{
    if (Status() != DownloaderTask::Idle) {
        logWarn << "P2PSingleTask is not idle, init abort, task file hash: " << param.FileHash
                << ", start: " << param.Start << ", end: " << param.End;
        return;
    }

    mFileHash = param.FileHash;
    mStart = param.Start;
    mEnd = param.End;
    mTotalSize = param.End - param.Start;
    mDc = dc;

    logDebug << "P2PSingleTask init, task file hash: " << param.FileHash << ", start: " << param.Start
             << ", end: " << param.End << ", total size: " << mTotalSize;

    mDc->onOpen([this]() {
        logDebug << "DataChannel opened for task: " << mFileHash;
        this->Start();
    });

    mDc->onClosed([this]() {
        logWarn << "DataChannel closed for task: " << mFileHash;
        if (this->Status() != DownloaderTask::Completed) {
            postState(DownloaderTask::Fail);
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

    logDebug << "Started P2PSingleTask. TaskId: " << mTaskID;
    postState(DownloaderTask::Running);
}

bool P2PSingleTask::Pause()
{
    if (!pause()) {
        return false;
    }

    if (mDc && mDc->isOpen()) {
        try {
            mDc->send("PAUSE");
        } catch (const std::exception& e) {
            notify(shared_from_this());
            logError << "Failed to send PAUSE message: " << e.what();
            return false;
        }
    }

    notify(shared_from_this());
    return true;
}

bool P2PSingleTask::Resume()
{
    if (!resume()) {
        return false;
    }

    if (mDc && mDc->isOpen()) {
        try {
            mDc->send("RESUME");
        } catch (const std::exception& e) {
            notify(shared_from_this());
            logError << "Failed to send RESUME message: " << e.what();
            return false;
        }
    }

    notify(shared_from_this());
    return true;
}

bool P2PSingleTask::Cancel()
{
    if (!cancel()) {
        return false;
    }

    if (mDc && mDc->isOpen()) {
        try {
            mDc->send("CANCEL");
            mDc->close();
        } catch (const std::exception& e) {
            notify(shared_from_this());
            logError << "Failed to send CANCEL message or close DataChannel: " << e.what();
            return false;
        }
    }

    notify(shared_from_this());
    return true;
}

void P2PSingleTask::HandleIncomingData(std::variant<std::vector<std::byte>, std::string>&& data)
{
    mManager->onTaskDataReceived(shared_from_this(), std::move(data));
}

void P2PSingleTask::handleIncomingDataInternal(std::variant<std::vector<std::byte>, std::string>&& data)
{
    if (std::holds_alternative<std::vector<std::byte>>(data)) {
        auto& bytes = std::get<std::vector<std::byte>>(data);

        if (bytes.empty()) {
            return;
        }
        uint64_t currentOffset = mStart + mDownloaded;
        auto buf = std::make_shared<util::DownloaderTaskContainerBuffer<std::vector<std::byte>>>();
        buf->Set(currentOffset, std::move(bytes));
        append(buf);
        mDownloaded += buf->Length();

        notify(shared_from_this());

        if (mEnd > 0 && Size() >= mTotalSize) {
            postState(DownloaderTask::Completed);
        }
    } else if (std::holds_alternative<std::string>(data)) {
        const std::string& msg = std::get<std::string>(data);

        if (msg == "TRANSFER_COMPLETE") {
            postState(DownloaderTask::Completed);
            logDebug << "Received TRANSFER_COMPLETE. TaskId: " << mTaskID;
        } else if (msg == "PAUSE_ACK") {
            postState(DownloaderTask::Paused);
            logDebug << "Received PAUSE_ACK. TaskId: " << mTaskID;
        } else if (msg == "CANCEL_ACK") {
            postState(DownloaderTask::Cancelled);
            logDebug << "Received CANCEL_ACK. TaskId: " << mTaskID;
        } else if (msg.find("ERROR") != std::string::npos) {
            postState(DownloaderTask::Fail);
            logDebug << "Received ERROR. TaskId: " << mTaskID << ", errmesg: " << msg;
        } else {
            logWarn << "Unhandled control message: " << msg;
        }
    }
}

NS_END
NS_END
