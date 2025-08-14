#include "UploadManager.h"

#include <fstream>

NS_BEGIN(dcdn)

UploadManager::UploadManager(MainManager* man, FileManager* mf, CertificatePair cert)
    : BaseManager(man), mFileMgr(mf), mCert(cert)
{
    registerHandler(EventType::UploadMsg, &UploadManager::handleUploadMsgEvent);
}

UploadManager::~UploadManager() {}

void UploadManager::handleUploadMsgEvent(std::shared_ptr<Event> evt)
{
    auto argEvent = static_cast<ArgEvent<UploadFileArg>*>(evt.get());
    if (!argEvent) {
        logError << "Invalid UploadMsg event type";
        return;
    }

    const UploadFileArg& arg = argEvent->Arg();
    logDebug << "Received upload request for: " << arg.FileHash << " [" << arg.BlockStart << "-" << arg.BlockEnd << "]";

    auto task = std::make_shared<UploadFileTask>(
        arg.PeerID, arg.FileHash, arg.BlockStart, arg.BlockEnd, arg.IceUfrag, arg.IcePwd, arg.RemoteSdp);

    task->SetState(UploadFileTask::Pending);

    {
        std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
        mLabelTaskMap[task->Label()] = task;
    }

    mCv.notify_one();
}

std::vector<UploadFileTaskPtr> UploadManager::getTasksToProcess()
{
    std::vector<UploadFileTaskPtr> tasksToProcess;
    {
        std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
        for (const auto& [label, task] : mLabelTaskMap) {
            auto state = task->GetState();
            if (state == UploadFileTask::Running || state == UploadFileTask::Init || state == UploadFileTask::Pending) {
                tasksToProcess.push_back(task);
            }
        }
    }
    return tasksToProcess;
}

bool UploadManager::initFileOperations(UploadFileTaskPtr task)
{
    if (!task->File.is_open()) {
        auto fileInfo = mFileMgr->GetUploadFileResource(task->FileHash, task->BlockStart);
        if (!fileInfo.has_value()) {
            logError << "File not found: " << task->FileHash;
            try {
                task->Dc->send("ERROR:FILE_NOT_EXIST");
            } catch (const std::exception& e) {
                logError << "Send ERROR:FILE_NOT_EXIST failed: " << e.what();
            }
            task->SetState(UploadFileTask::Failed);
            if (task->Dc) {
                task->Dc->close();
            }
            return false;
        }
        auto resourceInfo = fileInfo.value();
        task->FileOffset = task->BlockStart - resourceInfo.start;
        task->FileEnd = task->BlockEnd - resourceInfo.start;
        task->FilePath = resourceInfo.path;
        task->File.open(task->FilePath, std::ios::binary);
        if (!task->File.is_open()) {
            logError << "Failed to open file: " << task->FilePath;
            try {
                task->Dc->send("ERROR:FILE_OPEN_FAILED");
            } catch (const std::exception& e) {
                logError << "Send ERROR:FILE_OPEN_FAILED failed: " << e.what();
            }
            task->SetState(UploadFileTask::Failed);
            if (task->Dc) {
                task->Dc->close();
            }
            return false;
        }
        task->File.seekg(task->FileOffset);
        if (!task->File) {
            logError << "Failed to seek to position: " << task->FileOffset << " in file: " << task->FilePath;
            try {
                task->Dc->send("ERROR:FILE_SEEK_FAILED");
            } catch (const std::exception& e) {
                logError << "Send ERROR:FILE_SEEK_FAILED failed: " << e.what();
            }
            task->SetState(UploadFileTask::Failed);
            if (task->Dc) {
                task->Dc->close();
            }
            return false;
        }
    }
    return true;
}

void UploadManager::performFileSending(UploadFileTaskPtr task)
{
    size_t totalBytesInBlock = (task->BlockEnd - task->BlockStart) + 1;
    size_t remainingBytes = totalBytesInBlock - task->BytesSent;

    if (remainingBytes > 0) {
        const size_t chunkSize = 16 * 1024;
        size_t bytesToSend = std::min(chunkSize, remainingBytes);
        size_t allowed = mTokenBucket->Consume(bytesToSend);

        if (allowed > 0) {
            std::vector<char> buffer(allowed);
            task->File.read(buffer.data(), allowed);
            size_t readBytes = task->File.gcount();

            if (readBytes > 0) {
                try {
                    task->Dc->send((std::byte*)buffer.data(), readBytes);
                    task->BytesSent += readBytes;
                } catch (const std::exception& e) {
                    logError << "Send failed: " << e.what();
                    task->SetState(UploadFileTask::Failed);
                    if (task->Dc) {
                        task->Dc->close();
                    }
                }
            }
        }
    }

    if (task->BytesSent >= totalBytesInBlock) {
        task->SetState(UploadFileTask::Completed);
        logInfo << "File transfer completed: " << task->Label();
    }
}

void UploadManager::handleRunningTask(UploadFileTaskPtr task)
{
    if (!task->Dc || !task->Dc->isOpen()) {
        return;
    }

    if (task->Dc->bufferedAmount() > 1024 * 1024) {
        task->SetState(UploadFileTask::BufferedAmount);
        return;
    }

    if (initFileOperations(task)) {
        performFileSending(task);
    }
}

void UploadManager::handleTask(UploadFileTaskPtr task)
{
    auto state = task->GetState();

    if (state == UploadFileTask::Init || state == UploadFileTask::Pending) {
        if (!task->Pc) {
            setupPeerConnection(task);
        }
        return;
    }

    if (state == UploadFileTask::Running) {
        handleRunningTask(task);
    }
}

void UploadManager::run()
{
    logInfo << "UploadManager running";
    while (!mStopFlag) {
        waitAllEvents(std::chrono::milliseconds(100));

        auto tasksToProcess = getTasksToProcess();

        if (tasksToProcess.empty()) {
            std::unique_lock<std::mutex> lock(mMtx);
            mCv.wait(lock);
            continue;
        }

        for (auto& task : tasksToProcess) {
            handleTask(task);
        }
    }
    logInfo << "UploadManager exit";
}

void UploadManager::setupPeerConnection(UploadFileTaskPtr task)
{
    rtc::Configuration config;
    config.iceUfrag = task->IceUfrag;
    config.icePwd = task->IcePwd;
    config.enableIceUdpMux = true;
    config.certificatePemFile = mCert.certPem;
    config.keyPemFile = mCert.keyPem;
    auto ss = mMan->Cfg().StunServers();
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

    task->Pc = std::make_shared<rtc::PeerConnection>(config);

    task->Pc->onDataChannel([this, task](std::shared_ptr<rtc::DataChannel> dc) { handleDataChannel(dc, task); });
}

std::optional<std::tuple<std::string, size_t, size_t>> UploadManager::parseLabel(const std::string& label)
{
    size_t first_dash = label.find('-');
    size_t second_dash = label.find('-', first_dash + 1);
    if (first_dash == std::string::npos || second_dash == std::string::npos || second_dash <= first_dash) {
        logError << "Label format is incorrect, cannot create task: " << label;
        return std::nullopt;
    }

    std::string fileHash = label.substr(0, first_dash);
    std::string offset_str = label.substr(first_dash + 1, second_dash - first_dash - 1);
    std::string len_str = label.substr(second_dash + 1);

    try {
        size_t offset = std::stoull(offset_str);
        size_t end = std::stoull(len_str);
        return std::make_tuple(fileHash, offset, end);
    } catch (const std::exception& e) {
        logError << "Failed to parse label: " << label << ", error: " << e.what();
        return std::nullopt;
    }
}

std::optional<UploadFileTaskPtr> UploadManager::findOrCreateTask(
    std::shared_ptr<rtc::DataChannel> dc,
    UploadFileTaskPtr initial_task,
    const std::string& label)
{
    std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
    auto it = mLabelTaskMap.find(label);
    if (it == mLabelTaskMap.end()) {
        logError << "Task not found for label: " << label << ", creating new task";
        auto parsed = parseLabel(label);
        if (!parsed.has_value()) {
            dc->close();
            return std::nullopt;
        }

        auto [fileHash, offset, end] = parsed.value();
        auto newTask = std::make_shared<UploadFileTask>(
            initial_task->PeerID,
            fileHash,
            offset,
            end,
            initial_task->IceUfrag,
            initial_task->IcePwd,
            initial_task->RemoteSdp);
        newTask->Pc = initial_task->Pc;
        newTask->Dc = dc;
        mLabelTaskMap[label] = newTask;
        return newTask;
    } else {
        it->second->Dc = dc;
        return it->second;
    }
}

void UploadManager::setupOnOpenCallback(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr channelTask)
{
    dc->onOpen([this, channelTask]() {
        logInfo << "DataChannel opened for task: " << channelTask->Label();
        channelTask->SetState(UploadFileTask::Running);
        mCv.notify_one();
    });
}

void UploadManager::setupOnBufferedAmountLowCallback(
    std::shared_ptr<rtc::DataChannel> dc,
    UploadFileTaskPtr channelTask)
{
    dc->setBufferedAmountLowThreshold(1024 * 1024); // set buffered amount low threshold to 1MB
    dc->onBufferedAmountLow([this, channelTask]() {
        logInfo << "DataChannel buffered amount low: " << channelTask->Label();
        if (channelTask->GetState() == UploadFileTask::BufferedAmount) {
            channelTask->SetState(UploadFileTask::Running);
            mCv.notify_one();
        }
    });
}

void UploadManager::setupOnMessageCallback(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr channelTask)
{
    dc->onMessage([this, channelTask](auto data) {
        if (std::holds_alternative<std::string>(data)) {
            std::string msg = std::get<std::string>(data);
            if (msg == "PAUSE") {
                logInfo << "Received PAUSE for: " << channelTask->Label();
                channelTask->SetState(UploadFileTask::Paused);
                try {
                    channelTask->Dc->send("PAUSE_ACK");
                } catch (const std::exception& e) {
                    logError << "Send PAUSE_ACK failed: " << e.what();
                }
            } else if (msg == "CANCEL") {
                logInfo << "Received CANCEL for: " << channelTask->Label();
                channelTask->SetState(UploadFileTask::Cancelled);
                try {
                    channelTask->Dc->send("CANCEL_ACK");
                } catch (const std::exception& e) {
                    logError << "Send CANCEL_ACK failed: " << e.what();
                }
                mCv.notify_one();
            } else if (msg == "RESUME") {
                logInfo << "Received RESUME for: " << channelTask->Label();
                channelTask->SetState(UploadFileTask::Running);
                mCv.notify_one();
            }
        }
    });
}

void UploadManager::setupOnClosedCallback(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr channelTask)
{
    dc->onClosed([this, channelTask]() {
        logInfo << "DataChannel closed: " << channelTask->Label();
        if (channelTask->Dc) {
            channelTask->Dc.reset();
        }
        {
            std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
            mLabelTaskMap.erase(channelTask->Label());
        }
    });
}

void UploadManager::handleDataChannel(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr initial_task)
{
    const std::string& label = dc->label();
    logInfo << "DataChannel created: " << label;

    auto channelTaskOpt = findOrCreateTask(dc, initial_task, label);
    if (!channelTaskOpt.has_value()) {
        return;
    }
    auto channelTask = channelTaskOpt.value();

    setupOnOpenCallback(dc, channelTask);
    setupOnBufferedAmountLowCallback(dc, channelTask);
    setupOnMessageCallback(dc, channelTask);
    setupOnClosedCallback(dc, channelTask);
}

NS_END