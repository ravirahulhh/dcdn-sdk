#include "UploadManager.h"

#include <fstream>

NS_BEGIN(dcdn)

UploadManager::UploadManager(MainManager* man, FileManager* mf, CertificatePair cert)
    : BaseManager(man), mFileMgr(mf), mCert(cert), mTokenBucket(std::make_shared<TokenBucket>())
{
    registerHandler(EventType::UploadMsg, &UploadManager::handleUploadMsgEvent);
}

UploadManager::~UploadManager() {}

void UploadManager::handleUploadMsgEvent(std::shared_ptr<Event> evt)
{
    try {
        auto argEvent = static_cast<ArgEvent<json>*>(evt.get());
        if (!argEvent) {
            logError << "Invalid UploadMsg event type";
            return;
        }

        const json& j = argEvent->Arg();
        std::string remoteSdp, iceUfrag, icePwd, peerID, fileHash;
        size_t blockStart, blockEnd;
        if (j.contains("push_conn_meta") && j["push_conn_meta"].is_object()) {
            const auto& meta = j["push_conn_meta"];
            fileHash = meta.value("hash", "");
            remoteSdp = meta.value("conn_meta", "");
            icePwd = meta.value("ice_pwd", "");
            iceUfrag = meta.value("ice_ufrag", "");
            peerID = meta.value("peer_id", "");
            blockStart = meta.value("start", 0);
            blockEnd = meta.value("end", 0);
        }

        logDebug << "Received upload request from peer: " << peerID << " for file: " << fileHash
                 << " from block: " << blockStart << " to block: " << blockEnd << " with iceUfrag: " << iceUfrag
                 << " icePwd: " << icePwd << " remoteSdp: " << remoteSdp;

        if (fileHash.empty() || remoteSdp.empty() || iceUfrag.empty() || icePwd.empty() || peerID.empty()) {
            logError << "Invalid UploadMsg event: " << j.dump();
            return;
        }

        if (blockEnd != 0 && blockEnd <= blockStart) {
            logError << "Invalid block range: start=" << blockStart << ", end=" << blockEnd;
            return;
        }

        auto fileInfo = mFileMgr->GetUploadFileResource(fileHash, blockStart);
        if (!fileInfo.has_value()) {
            logError << "File not found: " << fileHash << " for peer: " << peerID;
            return;
        }

        auto pc = setupPeerConnection(iceUfrag, icePwd, remoteSdp);
        if (!pc.has_value()) {
            logError << "Failed to setup PeerConnection for peer: " << peerID;
            return;
        }

        auto ctx = std::make_shared<PeerConnectionCtx>(peerID, iceUfrag, icePwd, remoteSdp);

        {
            std::lock_guard<std::mutex> lock(mPeerConnectionMapMutex);
            mPeerConnectionMap[pc.value()] = ctx;
        }

    } catch (const std::exception& e) {
        logError << "Exception in handleUploadMsgEvent: " << e.what();
    } catch (...) {
        logError << "Unknown exception in handleUploadMsgEvent";
    }
}

std::optional<std::shared_ptr<rtc::PeerConnection>>
UploadManager::setupPeerConnection(const std::string& iceUfrag, const std::string& icePwd, const std::string& remoteSdp)
{
    try {
        rtc::Configuration config;
        config.iceUfrag = iceUfrag;
        config.icePwd = icePwd;
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

        auto pc = std::make_shared<rtc::PeerConnection>(config);
        pc->setRemoteDescription(remoteSdp);

        if (!pc) {
            logError << "Failed to create PeerConnection";
            return std::nullopt;
        }

        pc->onDataChannel([this, pc](std::shared_ptr<rtc::DataChannel> dc) { handleDataChannel(dc, pc); });

        return pc;
    } catch (const std::exception& e) {
        logError << "Exception in setupPeerConnection: " << e.what();
        return std::nullopt;
    } catch (...) {
        logError << "Unknown exception in setupPeerConnection";
        return std::nullopt;
    }
}

void UploadManager::handleDataChannel(std::shared_ptr<rtc::DataChannel> dc, std::shared_ptr<rtc::PeerConnection> pc)
{
    try {
        auto label = dc->label();
        logDebug << "DataChannel opened: " << label;
        auto parsed = parseLabel(label);
        if (!parsed) {
            return;
        }

        auto [fileHash, start, end] = *parsed;

        // 获取PeerConnection的上下文
        PeerConnectionCtxPtr ctx;
        {
            std::lock_guard<std::mutex> lock(mPeerConnectionMapMutex);
            auto it = mPeerConnectionMap.find(pc);
            if (it == mPeerConnectionMap.end()) {
                return;
            }
            ctx = it->second;
        }

        auto task = std::make_shared<UploadFileTask>(
            ctx->PeerID, fileHash, start, end, ctx->IceUfrag, ctx->IcePwd, ctx->RemoteSdp);
        task->Pc = pc;
        task->Dc = dc;
        task->TaskID = getTaskID();

        if (!initFileOperations(task)) {
            return;
        }

        {
            std::lock_guard<std::mutex> lock(mTaskMapMutex);
            mTaskMap[task->TaskID] = task;
        }

        setupOnOpenCallback(dc, task);
        setupOnBufferedAmountLowCallback(dc, task);
        setupOnMessageCallback(dc, task);
        setupOnClosedCallback(dc, task);
    } catch (const std::exception& e) {
        logError << "Exception in handleDataChannel: " << e.what();
        dc->close();
    } catch (...) {
        logError << "Unknown exception in handleDataChannel";
        dc->close();
    }
}

bool UploadManager::initFileOperations(UploadFileTaskPtr task)
{
    try {
        logDebug << "Init file operations for task: " << task->TaskID << " for file: " << task->FileHash
                 << " from block: " << task->BlockStart << " to block: " << task->BlockEnd;

        auto fileInfo = mFileMgr->GetUploadFileResource(task->FileHash, task->BlockStart);
        if (!fileInfo.has_value()) {
            logError << "File not found: " << task->FileHash;
            try {
                task->Dc->send("ERROR:FILE_NOT_EXIST");
            } catch (const std::exception& e) {
                logError << "Send ERROR:FILE_NOT_EXIST failed: " << e.what();
            }
            return false;
        }

        auto resourceInfo = fileInfo.value();
        task->FileStart = task->BlockStart - resourceInfo.start;
        task->FileEnd = task->BlockEnd - resourceInfo.start;
        task->FilePath = resourceInfo.path;

        // 设置FileEnd
        if (task->BlockEnd == 0) {
            std::ifstream sizeCheck(task->FilePath, std::ios::binary | std::ios::ate);
            size_t fileSize = sizeCheck.tellg();
            sizeCheck.close();
            task->FileEnd = fileSize;
        }

        task->File.open(task->FilePath, std::ios::binary);
        if (!task->File.is_open()) {
            logError << "Failed to open file: " << task->FilePath;
            try {
                task->Dc->send("ERROR:FILE_OPEN_FAILED");
            } catch (const std::exception& e) {
                logError << "Send ERROR:FILE_OPEN_FAILED failed: " << e.what();
            }
            return false;
        }

        task->File.seekg(task->FileStart);
        if (!task->File) {
            logError << "Failed to seek to position: " << task->FileStart << " in file: " << task->FilePath;
            try {
                if (task->SetState(UploadFileTask::State::Failed)) {
                    if (task->Dc) {
                        task->Dc->send("ERROR:FILE_SEEK_FAILED");
                    }
                }
            } catch (const std::exception& e) {
                logError << "Send ERROR:FILE_SEEK_FAILED failed: " << e.what();
            }
            return false;
        }

        return true;
    } catch (const std::exception& e) {
        logError << "Exception in initFileOperations: " << e.what();
        return false;
    } catch (...) {
        logError << "Unknown exception in initFileOperations";
        return false;
    }
}

void UploadManager::performFileSending(UploadFileTaskPtr task)
{
    try {
        size_t totalBytesInBlock = task->FileEnd - task->FileStart;
        while (task->Dc->bufferedAmount() < mMaxBufferedAmount.load() &&
               task->GetState() == UploadFileTask::State::Running) {
            size_t remainingBytes = totalBytesInBlock - task->BytesSent;

            if (remainingBytes > 0) {
                const size_t chunkSize = 64 * 1024;
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
                            task->SetState(UploadFileTask::State::Failed);
                            removeActiveTask(task->TaskID);
                        }
                    }
                }
            }

            logDebug << "Uploading to peer: " << task->PeerID << " file: " << task->FileHash
                     << " progress: " << task->BytesSent << "/" << totalBytesInBlock
                     << "  byteSent: " << task->BytesSent << "  tellg: " << task->File.tellg();

            if (task->BytesSent >= totalBytesInBlock || task->File.eof() ||
                task->File.tellg() >= static_cast<std::streampos>(task->FileEnd)) {
                logDebug << "File transfer completed, send TRANSFER_COMPLETE to " << task->Label();
                task->Dc->send("TRANSFER_COMPLETE");
                task->SetState(UploadFileTask::State::Completed);
                removeActiveTask(task->TaskID);
            }
        }
        if (task->GetState() == UploadFileTask::State::Running) {
            logDebug << "Buffered task: " << task->Label();
            task->SetState(UploadFileTask::State::Buffered);
        }
        {
            std::lock_guard<std::mutex> lock(mPeerConnectionMapMutex);
            auto it = mPeerConnectionMap.find(task->Pc);
            if (it != mPeerConnectionMap.end()) {
                it->second->UpdateTime();
            }
        }
    } catch (const std::exception& e) {
        logError << "Exception in performFileSending: " << e.what();
        task->SetState(UploadFileTask::State::Failed);
        removeActiveTask(task->TaskID);
    } catch (...) {
        logError << "Unknown exception in performFileSending";
        task->SetState(UploadFileTask::State::Failed);
        removeActiveTask(task->TaskID);
    }
}

void UploadManager::setupOnOpenCallback(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr channelTask)
{
    dc->onOpen([this, dc, channelTask]() {
        try {
            logDebug << "DataChannel opened for task: " << channelTask->Label();
            if (channelTask->SetState(UploadFileTask::State::Running)) {
                addActiveTask(channelTask);
            }
        } catch (const std::exception& e) {
            logError << "Exception in onOpen callback: " << e.what();
        } catch (...) {
            logError << "Unknown exception in onOpen callback";
        }
    });
}

void UploadManager::setupOnBufferedAmountLowCallback(
    std::shared_ptr<rtc::DataChannel> dc,
    UploadFileTaskPtr channelTask)
{
    dc->setBufferedAmountLowThreshold(mMaxBufferedAmount.load() * mBufferedThresholdRate.load()); // 16KB
    dc->onBufferedAmountLow([this, dc, channelTask]() {
        try {
            logDebug << "DataChannel buffered amount low: " << channelTask->Label();
            if (channelTask->GetState() == UploadFileTask::State::Buffered &&
                channelTask->SetState(UploadFileTask::State::Running)) {
                addActiveTask(channelTask);
            }
        } catch (const std::exception& e) {
            logError << "Exception in onBufferedAmountLow callback: " << e.what();
        } catch (...) {
            logError << "Unknown exception in onBufferedAmountLow callback";
        }
    });
}

void UploadManager::setupOnMessageCallback(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr channelTask)
{
    dc->onMessage([this, dc, channelTask](auto data) {
        try {
            if (std::holds_alternative<std::string>(data)) {
                std::string msg = std::get<std::string>(data);
                if (msg == "PAUSE") {
                    logInfo << "Received PAUSE for: " << channelTask->Label();
                    try {
                        if (channelTask->SetState(UploadFileTask::State::Paused)) {
                            removeActiveTask(channelTask->TaskID);
                            dc->send("PAUSE_ACK");
                        }
                    } catch (const std::exception& e) {
                        logError << "Send PAUSE_ACK failed: " << e.what();
                    }
                } else if (msg == "CANCEL") {
                    logInfo << "Received CANCEL for: " << channelTask->Label();
                    try {
                        if (channelTask->SetState(UploadFileTask::State::Cancelled)) {
                            removeActiveTask(channelTask->TaskID);
                            dc->send("CANCEL_ACK");
                        }
                    } catch (const std::exception& e) {
                        logError << "Send CANCEL_ACK failed: " << e.what();
                    }
                } else if (msg == "RESUME") {
                    logInfo << "Received RESUME for: " << channelTask->Label();
                    if (channelTask->SetState(UploadFileTask::State::Running)) {
                        addActiveTask(channelTask);
                    }
                }
            }
        } catch (const std::exception& e) {
            logError << "Exception in onMessage callback: " << e.what();
        } catch (...) {
            logError << "Unknown exception in onMessage callback";
        }
    });
}

void UploadManager::setupOnClosedCallback(std::shared_ptr<rtc::DataChannel> dc, UploadFileTaskPtr channelTask)
{
    dc->onClosed([this, channelTask]() {
        try {
            logDebug << "DataChannel closed: " << channelTask->Label();

            auto taskID = channelTask->TaskID;
            removeTask(taskID);

            if (channelTask->File.is_open()) {
                channelTask->File.close();
            }
        } catch (const std::exception& e) {
            logError << "Exception in onClosed callback: " << e.what();
        } catch (...) {
            logError << "Unknown exception in onClosed callback";
        }
    });
}

std::optional<std::tuple<std::string, size_t, size_t>> UploadManager::parseLabel(const std::string& label)
{
    try {
        size_t first_dash = label.find(':');
        size_t second_dash = label.find(':', first_dash + 1);
        if (first_dash == std::string::npos || second_dash == std::string::npos || second_dash <= first_dash) {
            logError << "Label format is incorrect: " << label;
            return std::nullopt;
        }

        std::string fileHash = label.substr(0, first_dash);
        std::string offset_str = label.substr(first_dash + 1, second_dash - first_dash - 1);
        std::string len_str = label.substr(second_dash + 1);

        size_t offset = std::stoull(offset_str);
        size_t end = std::stoull(len_str);
        return std::make_tuple(fileHash, offset, end);
    } catch (const std::exception& e) {
        logError << "Failed to parse label: " << label << ", error: " << e.what();
        return std::nullopt;
    } catch (...) {
        logError << "Unknown exception while parsing label: " << label;
        return std::nullopt;
    }
}

void UploadManager::run()
{
    logInfo << "UploadManager running";
    while (!mStopFlag) {
        try {
            waitAllEvents(std::chrono::milliseconds(100));

            // 处理活动任务
            std::vector<UploadFileTaskPtr> activeTasks;
            {
                std::lock_guard<std::mutex> lock(mActiveTaskMutex);
                mActiveTask.swap(activeTasks);
            }

            if (activeTasks.empty()) {
                std::unique_lock<std::mutex> lock(mActiveTaskMutex);
                mCv.wait_for(lock, std::chrono::milliseconds(100));
                continue;
            }

            for (auto task : activeTasks) {
                if (task->Dc) {
                    if (task->Dc->isOpen()) {
                        performFileSending(task);
                    }
                }
            }
        } catch (const std::exception& e) {
            logError << "Exception in run loop: " << e.what();
        } catch (...) {
            logError << "Unknown exception in run loop";
        }
    }
    logInfo << "UploadManager exit";
}

NS_END