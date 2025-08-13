#include "UploadManager.h"

#include <fstream>

NS_BEGIN(dcdn)

UploadManager::UploadManager(MainManager* man, FileManager* mf, CertificatePair cert)
    : BaseManager(man), mFileMgr(mf), mCert(cert)
{
    registerHandler(EventType::UploadMsg, &UploadManager::handleUploadMsgEvent);
    Start(true);
}

UploadManager::~UploadManager()
{
    stop();
}

void UploadManager::Start(bool detach)
{
    mThread = std::make_shared<std::thread>([this]() { processTasks(); });
    if (detach) {
        mThread->detach();
    }
}

void UploadManager::stop()
{
    mStopFlag = true;
    mTaskCond.notify_all();

    if (mThread && mThread->joinable()) {
        mThread->join();
    }
}

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

    {
        std::lock_guard<std::mutex> lock(mTaskMutex);
        mTaskQueue.push(task);
    }

    mTaskCond.notify_one();
}

void UploadManager::run()
{
    logInfo << "UploadManager running";
    while (!mStopFlag) {
        waitAllEvents(std::chrono::milliseconds(10));
    }
    logInfo << "UploadManager exit";
}

void UploadManager::processTasks()
{
    while (!mStopFlag) {
        if (mTaskQueue.empty()) {
            std::unique_lock<std::mutex> lock(mTaskMutex);
            mTaskCond.wait_for(
                lock, std::chrono::milliseconds(100), [this] { return !mTaskQueue.empty() || mStopFlag; });
            if (mStopFlag)
                break;
            continue;
        }
        std::shared_ptr<UploadFileTask> task;
        {
            std::unique_lock<std::mutex> lock(mTaskMutex);
            task = mTaskQueue.front();
            mTaskQueue.pop();
        }

        auto state = task->GetState();

        if (state == UploadFileTask::Init || state == UploadFileTask::Pending) {
            if (!task->Pc) {
                setupPeerConnection(task);
            }
            task->SetState(UploadFileTask::Running);
            state = UploadFileTask::Running;
        }

        if (state == UploadFileTask::Cancelled) {
            if (task->Dc && task->Dc->isOpen()) {
                try {
                    task->Dc->send("CANCEL_ACK");
                } catch (const std::exception& e) {
                    logError << "Send CANCEL_ACK failed: " << e.what();
                }
            }

            std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
            mLabelTaskMap.erase(task->Label());
            continue;
        }

        if (state == UploadFileTask::Paused) {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            mTaskQueue.push(task);
            continue;
        }

        if (state == UploadFileTask::Running) {
            if (!task->Dc || !task->Dc->isOpen()) {
                logDebug << "task: " << task->Label() << " dc is not ready";
                std::lock_guard<std::mutex> lock(mTaskMutex);
                mTaskQueue.push(task);
                continue;
            }

            if (task->Dc->bufferedAmount() > 0.9 * task->Dc->maxMessageSize()) {
                std::lock_guard<std::mutex> lock(mTaskMutex);
                mTaskQueue.push(task);
                continue;
            }

            if (!task->File.is_open()) {
                auto fileInfo = mFileMgr->GetUploadFileResource(task->FileHash, task->BlockStart);
                if (!fileInfo.has_value()) {
                    logError << "File not found: " << task->FileHash;
                    try {
                        task->Dc->send("ERROR:FILE_NOT_EXIST");
                    } catch (const std::exception& e) {
                        logError << "Send ERROR:FILE_NOT_EXIST failed: " << e.what();
                    }

                    std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
                    mLabelTaskMap.erase(task->Label());
                    continue;
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
                        task->Dc->close();
                    } catch (const std::exception& e) {
                        logError << "Send ERROR:FILE_OPEN_FAILED or close dc failed: " << e.what();
                    }

                    std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
                    mLabelTaskMap.erase(task->Label());
                    continue;
                }

                task->File.seekg(task->FileOffset + task->BytesSent);
            }

            logDebug << "task: " << task->Label() << " file size: " << task->FileEnd
                     << " bytesSent: " << task->BytesSent;

            const size_t chunkSize = 16 * 1024; // 16KB
            size_t bytesToSend = std::min(chunkSize, task->FileEnd - task->BytesSent);

            if (bytesToSend > 0) {
                size_t allowed = mTokenBucket->Consume(bytesToSend);
                if (allowed == 0) {
                    // 限速中，放回队列等待下次处理
                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mTaskQueue.push(task);
                    continue;
                }

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
                        std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
                        mLabelTaskMap.erase(task->Label());
                        continue;
                    }
                }
            }

            // 检查是否完成
            if (task->FileOffset + task->BytesSent >= task->FileEnd) {
                task->SetState(UploadFileTask::Completed);
                logInfo << "File transfer completed: " << task->Label();

                std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
                mLabelTaskMap.erase(task->Label());
            } else {
                std::lock_guard<std::mutex> lock(mTaskMutex);
                mTaskQueue.push(task);
            }
        } else if (state == UploadFileTask::Failed) {
            std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
            mLabelTaskMap.erase(task->Label());
        }
    }
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

void UploadManager::handleDataChannel(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr initial_task)
{
    const std::string& label = dc->label();
    logInfo << "DataChannel created: " << label;

    UploadFileTaskPtr channelTask;

    {
        std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
        auto it = mLabelTaskMap.find(label);
        if (it == mLabelTaskMap.end()) {
            logError << "Task not found for label: " << label << ", creating new task";
            // Attempt to parse the label to create a task on-the-fly
            // Format: {HASH}-{OFFSET}-{LEN}
            size_t first_dash = label.find('-');
            size_t second_dash = label.find('-', first_dash + 1);
            if (first_dash != std::string::npos && second_dash != std::string::npos && second_dash > first_dash) {
                std::string fileHash = label.substr(0, first_dash);
                std::string offset_str = label.substr(first_dash + 1, second_dash - first_dash - 1);
                std::string len_str = label.substr(second_dash + 1);

                try {
                    size_t offset = std::stoull(offset_str);
                    size_t len = std::stoull(len_str);
                    auto newTask = std::make_shared<UploadFileTask>(
                        initial_task->PeerID, // Reuse peer_id
                        fileHash,
                        offset,
                        len,
                        initial_task->IceUfrag, // Reuse credentials
                        initial_task->IcePwd,
                        initial_task->RemoteSdp);
                    newTask->Pc = initial_task->Pc; // Reuse the existing PeerConnection
                    newTask->Dc = dc;
                    mLabelTaskMap[label] = newTask;
                    channelTask = newTask; // This is the task for this channel
                    {
                        std::lock_guard<std::mutex> lock(mTaskMutex);
                        mTaskQueue.push(newTask);
                    }
                } catch (const std::exception& e) {
                    logError << "Failed to parse label: " << label << ", error: " << e.what();
                    dc->close();
                    return;
                }
            } else {
                logError << "Label format is incorrect, cannot create task: " << label;
                dc->close();
                return;
            }
        } else {
            channelTask = it->second;
            channelTask->Dc = dc;
        }
    }

    if (!channelTask) {
        logError << "Could not associate data channel with any task for label: " << label;
        dc->close();
        return;
    }

    dc->onOpen([this, channelTask]() {
        logInfo << "DataChannel opened for task: " << channelTask->Label();
        channelTask->SetState(UploadFileTask::Running);
    });

    dc->onMessage([this, channelTask](auto data) {
        if (std::holds_alternative<std::string>(data)) {
            std::string msg = std::get<std::string>(data);
            if (msg == "PAUSE") {
                logInfo << "Received PAUSE for: " << channelTask->Label();
                channelTask->SetState(UploadFileTask::Paused);
            } else if (msg == "CANCEL") {
                logInfo << "Received CANCEL for: " << channelTask->Label();
                channelTask->SetState(UploadFileTask::Cancelled);
            } else if (msg == "RESUME") {
                logInfo << "Received RESUME for: " << channelTask->Label();
                channelTask->SetState(UploadFileTask::Running);

                std::lock_guard<std::mutex> lock(mTaskMutex);
                mTaskQueue.push(channelTask);
                mTaskCond.notify_one();
            }
        }
    });

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

NS_END