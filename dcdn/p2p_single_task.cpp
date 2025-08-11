#include "p2p_single_task.h"
#include "common/Logger.h"
#include "plog/Log.h"
#include "rtc/common.hpp"
#include "util/Downloader.h"

#include <cassert>
#include <memory>
#include <string>
#include <variant>

namespace dcdn {
namespace download {

P2PSingleTask::P2PSingleTask(P2PDownloader *manager) : manager_(manager) {
  setStatus(DownloaderTask::Idle);
}

P2PSingleTask::~P2PSingleTask() {
  Cancel(); // safe cleanup
}

void P2PSingleTask::Init(TaskParam param,
                         std::shared_ptr<rtc::DataChannel> dc) {

  if (Status() != DownloaderTask::Idle) {
    logWarn << "P2PSingleTask is not idle, init abort, task content hash: "
            << param.ContentHash << ", start: " << param.Start
            << ", end: " << param.End;
    return;
  }

  contentHash_ = param.ContentHash;
  start_ = param.Start;
  end_ = param.End;
  totalSize_ = param.End - param.Start;
  dc_ = dc;
}

void P2PSingleTask::Start() {
  if (Status() != DownloaderTask::Idle) {
    return;
  }

  if (!dc_ || !dc_->isOpen()) {
    lastError_ = 4; // connection timeout or invalid
    setStatus(DownloaderTask::Fail);
    notify(shared_from_this());
    return;
  }

  // Setup datachannel message handler
  dc_->onMessage(
      [this](std::variant<std::vector<std::byte>, std::string> data) {
        HandleIncomingData(std::move(data));
      });

  setStatus(DownloaderTask::Running);
  notify(shared_from_this());

  logDebug << "Started P2PSingleTask. TaskId: " << taskId_;
}

bool P2PSingleTask::Pause() {
  if (pause()) {
    if (dc_ && dc_->isOpen()) {
      dc_->send("PAUSE");
    }
    notify(shared_from_this());
    return true;
  }
  return false;
}

bool P2PSingleTask::Resume() {
  if (resume()) {
    if (dc_ && dc_->isOpen()) {
      dc_->send("RESUME");
    }
    notify(shared_from_this());
    return true;
  }
  return false;
}

bool P2PSingleTask::Cancel() {
  if (cancel()) {
    if (dc_ && dc_->isOpen()) {
      dc_->send("CANCEL");
      dc_->close();
    }
    notify(shared_from_this());
    return true;
  }
  return false;
}

void P2PSingleTask::HandleIncomingData(
    std::variant<std::vector<std::byte>, std::string> &&data) {

  // transfer data lifecycle from network thread to manager thread
  manager_->onTaskDataReceived(shared_from_this(), std::move(data));
}

void P2PSingleTask::HandleIncomingDataInternal(
    std::variant<std::vector<std::byte>, std::string> &&data) {

  if (Status() == DownloaderTask::Fail || Status() == DownloaderTask::Cancelled) {
    return;
  }

  if (std::holds_alternative<std::vector<std::byte>>(data)) {
    const auto &bytes = std::get<std::vector<std::byte>>(data);

    auto buf = std::make_shared<
        util::DownloaderTaskContainerBuffer<std::vector<std::byte>>>();
    buf->Set(nextReadOffset_, bytes.begin(), bytes.end());
    append(buf); // add to task buffer
    downloaded_ += bytes.size();

    double progress = static_cast<double>(Size()) / totalSize_;
    logDebug << "Received data. Progress: " << (progress * 100) << "%";

    // Flow control
    if (progress >= 0.95 && dc_ && dc_->isOpen()) {
      dc_->send("STOP");
      logDebug << "Sent STOP to peer, buffer near full. TaskId: " << taskId_;
    }

    // Completion check
    if (Size() >= totalSize_) {
      setStatus(DownloaderTask::Completed);
      notify(shared_from_this());
    }

  } else if (std::holds_alternative<std::string>(data)) {
    const std::string &msg = std::get<std::string>(data);

    if (msg == "PAUSE_ACK") {
      setStatus(DownloaderTask::Paused);
      logDebug << "Received PAUSE_ACK. TaskId: " << taskId_;
    } else if (msg == "CANCEL_ACK") {
      setStatus(DownloaderTask::Cancelled);
      logDebug << "Received CANCEL_ACK. TaskId: " << taskId_;
    } else {
      logWarn << "Unhandled control message: " << msg;
    }

    notify(shared_from_this());
  }
}

} // namespace download
} // namespace dcdn
