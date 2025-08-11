#include "dcdn/p2p_downloader.h"
#include "common/Logger.h"
#include "dcdn/Cert.h"
#include "dcdn/p2p_downloader.h"
#include "dcdn/p2p_single_task.h"
#include "rtc/datachannel.hpp"
#include "rtc/peerconnection.hpp"
#include "rtc/rtc.hpp"
#include <iostream>
#include <memory>
#include <mutex>

namespace dcdn {
namespace download {

P2PDownloader::P2PDownloader(const Option &option) : opt_(option) {
  // launch manager thread
  Start();
}

P2PDownloader::~P2PDownloader() {}

int P2PDownloader::Init(const util::DownloaderOption *opt) {
  if (opt == nullptr) {
    logError << "DownloaderOption is null!";
    return -1;
  }

  const Option *p2pOpt = dynamic_cast<const Option *>(opt);
  if (!p2pOpt) {
    logError << "DownloaderOption cast to P2PDownloader::Option failed!";
    return -2;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    opt_ = *p2pOpt;
  }
  return 0;
}

std::shared_ptr<util::DownloaderTask>
P2PDownloader::AddTask(const util::DownloaderTaskOption *opt) {
  auto taskOpt = dynamic_cast<const P2PDownloaderTaskOption *>(opt);
  if (!taskOpt) {
    logError << "Invalid task option provided to P2PDownloader";
    return nullptr;
  }
  auto task = std::make_shared<P2PSingleTask>(this);
  post([taskOpt = *taskOpt, task, this]() { addTask(taskOpt, task); });

  return task;
}

void P2PDownloader::initPeerConnection(const std::string &peerId,
                                       const std::string &peerSdp,
                                       const P2PDownloaderTaskOption &request,
                                       std::shared_ptr<P2PSingleTask> task) {

  rtc::Configuration config;
  // TODO: parse from dsp
  config.iceUfrag = "p0hI";
  config.icePwd = "aKESPyeQ51OID4NQFypOIIuJ";

  const auto cp = opt_;
  rtc::IceServer serv("47.236.146.120", 3478);
  serv.type = rtc::IceServer::Type::Stun;
  config.iceServers.push_back(serv);
  config.enableIceUdpMux = true;
  config.certificatePemFile = cp.certificate.certPem;
  config.keyPemFile = cp.certificate.keyPem;

  auto pc = std::make_shared<rtc::PeerConnection>(config);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    peerConnections_[peerId] = pc;
  }
  std::string label = request.ContentHash + ":" +
                      std::to_string(request.Start) + ":" +
                      std::to_string(request.End);
  auto dc = pc->createDataChannel(label);

  auto taskParams = TaskParam{request.ContentHash, request.Start, request.End};

  task->Init(taskParams, dc);
  task->Start();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_[task.get()] = task;
  }

  logDebug << "manager registered task: " << task.get();
}

void P2PDownloader::CancelTask(std::shared_ptr<util::DownloaderTask> task) {
  post([=]() { cancelTask(task); });
}

void P2PDownloader::PauseTask(std::shared_ptr<util::DownloaderTask> task) {
  post([=]() { pauseTask(task); });
}

void P2PDownloader::ResumeTask(std::shared_ptr<util::DownloaderTask> task) {
  post([=]() { resumeTask(task); });
}

void P2PDownloader::post(std::function<void()> &&task) {
  std::lock_guard<std::mutex> lock(mutex_);
  threadTaskQueue_.push_back(std::move(task));
  threadTaskCv_.notify_one();
}

void P2PDownloader::run() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      threadTaskCv_.wait(
          lock, [&] { return !threadTaskQueue_.empty() || stopFlag_; });

      if (stopFlag_ && threadTaskQueue_.empty()) {
        break;
      }

      task = std::move(threadTaskQueue_.front());
      threadTaskQueue_.pop_front();
    }

    if (task) {
      task();
    }
  }
}

void P2PDownloader::onTaskDataReceived(
    std::shared_ptr<P2PSingleTask> task,
    std::variant<std::vector<std::byte>, std::string> &&data) {
  task->HandleIncomingData(std::move(data));
}

void P2PDownloader::addTask(P2PDownloaderTaskOption taskOpt,
                            std::shared_ptr<P2PSingleTask> task) {

  std::shared_ptr<rtc::PeerConnection> peerConn;
  bool needInit = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto connectionIt = peerConnections_.find(taskOpt.PeerId);
    if (connectionIt == peerConnections_.end()) {
      needInit = true;
    } else {
      peerConn = connectionIt->second;
    }
  }

  if (needInit) {
    logDebug << "create task init connection init for peer: " << taskOpt.PeerId ;
    initPeerConnection(taskOpt.PeerId, taskOpt.PeerSdp, std::move(taskOpt),
                       task);
  }else{
    logDebug << "create task without init connection" << taskOpt.PeerId ;
  }

}

void P2PDownloader::cancelTask(std::shared_ptr<util::DownloaderTask> task) {
  if (!task)
    return;
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = tasks_.find(task.get());
  if (it != tasks_.end()) {
    it->second->Cancel();
  }
}

void P2PDownloader::pauseTask(std::shared_ptr<util::DownloaderTask> task) {
  if (!task)
    return;
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = tasks_.find(task.get());
  if (it != tasks_.end()) {
    it->second->Pause();
  }
}

void P2PDownloader::resumeTask(std::shared_ptr<util::DownloaderTask> task) {
  if (!task)
    return;
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = tasks_.find(task.get());
  if (it != tasks_.end()) {
    it->second->Resume();
  }
}

} // namespace download
} // namespace dcdn