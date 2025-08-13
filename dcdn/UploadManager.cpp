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
        arg.PeerID,
        arg.FileHash,
        arg.BlockStart,
        arg.BlockEnd - arg.BlockStart + 1,
        arg.IceUfrag,
        arg.IcePwd,
        arg.RemoteSdp);

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
    while (true) {
        if (mTaskQueue.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
            if (!task->pc) {
                setupPeerConnection(task);
            }
            task->SetState(UploadFileTask::Running);
            state = UploadFileTask::Running;
        }

        if (state == UploadFileTask::Cancelled) {
            if (task->dc && task->dc->isOpen()) {
                task->dc->send("CANCEL_ACK");
            }

            std::lock_guard<std::mutex> lock(mTaskMutex);
            mLabelTaskMap.erase(task->Label());
            continue;
        }

        if (state == UploadFileTask::Paused) {
            std::lock_guard<std::mutex> lock(mTaskMutex);
            mTaskQueue.push(task);
            continue;
        }

        if (state == UploadFileTask::Running) {
            if (!task->dc || !task->dc->isOpen()) {
                logDebug << "task: " << task->Label() << " dc is not ready";
                std::lock_guard<std::mutex> lock(mTaskMutex);
                mTaskQueue.push(task);
                continue;
            }

            if (task->dc->bufferedAmount() > 0.9 * task->dc->maxMessageSize()) {
                std::lock_guard<std::mutex> lock(mTaskMutex);
                mTaskQueue.push(task);
                continue;
            }

            if (!task->file.is_open()) {
                task->FilePath = mFileMgr->GetPathByBlockHash(task->FileHash, true);
                if (task->FilePath.empty()) {
                    logError << "File not found: " << task->FileHash;
                    task->dc->send("ERROR:FILE_NOT_EXIST");
                    task->dc->close();

                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mLabelTaskMap.erase(task->Label());
                    continue;
                }

                task->file.open(task->FilePath, std::ios::binary);
                if (!task->file.is_open()) {
                    logError << "Failed to open file: " << task->FilePath;
                    task->dc->send("ERROR:FILE_OPEN_FAILED");
                    task->dc->close();

                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mLabelTaskMap.erase(task->Label());
                    continue;
                }

                task->file.seekg(task->Offset + task->bytesSent);
            }

            logDebug << "task: " << task->Label() << " file size: " << task->Len << " bytesSent: " << task->bytesSent;

            const size_t chunkSize = 16 * 1024; // 16KB
            size_t bytesToSend = std::min(chunkSize, task->Len - task->bytesSent);

            if (bytesToSend > 0) {
                size_t allowed = mTokenBucket->Consume(bytesToSend);
                if (allowed == 0) {
                    // 限速中，放回队列等待下次处理
                    std::lock_guard<std::mutex> lock(mTaskMutex);
                    mTaskQueue.push(task);
                    continue;
                }

                std::vector<char> buffer(allowed);
                task->file.read(buffer.data(), allowed);
                size_t readBytes = task->file.gcount();

                if (readBytes > 0) {
                    try {
                        task->dc->send((std::byte*)buffer.data(), readBytes);
                        task->bytesSent += readBytes;
                    } catch (const std::exception& e) {
                        logError << "Send failed: " << e.what();
                        task->SetState(UploadFileTask::Failed);
                    }
                }
            }

            // 检查是否完成
            if (task->bytesSent >= task->Len) {
                task->SetState(UploadFileTask::Completed);
                logInfo << "File transfer completed: " << task->Label();

                std::lock_guard<std::mutex> lock(mTaskMutex);
                mLabelTaskMap.erase(task->Label());
            } else {
                std::lock_guard<std::mutex> lock(mTaskMutex);
                mTaskQueue.push(task);
            }
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

    task->pc = std::make_shared<rtc::PeerConnection>(config);

    task->pc->onDataChannel([this, task](std::shared_ptr<rtc::DataChannel> dc) { handleDataChannel(dc, task); });
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
                    newTask->pc = initial_task->pc; // Reuse the existing PeerConnection
                    newTask->dc = dc;
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
            channelTask->dc = dc;
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
        if (channelTask->dc) {
            channelTask->dc.reset();
        }
        {
            std::lock_guard<std::mutex> lock(mLabelTaskMapMutex);
            mLabelTaskMap.erase(channelTask->Label());
        }
    });
}

NS_END