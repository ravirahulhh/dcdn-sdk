#ifndef _DCDN_SDK_P2P_DOWNLOADER_H_
#define _DCDN_SDK_P2P_DOWNLOADER_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <deque>
#include "Cert.h"
#include "rtc/peerconnection.hpp"
#include "util/Downloader.h"

namespace dcdn {
namespace download {

class P2PSingleTask;

struct P2PDownloaderTaskOption : public util::DownloaderTaskOption {
  std::string PeerId;
  std::string PeerSdp;
  std::string ContentHash;
  size_t Start = 0;
  size_t End = 0;
};

class P2PDownloader : public util::BaseDownloader {
public:
  class Option : public util::DownloaderOption {
  public:
    // time out that connect to peer when start download
    std::string connection_timeout;
    // max time that the peer connection is idle before closing
    std::string max_peer_connection_idle_time;
    // certificate that used to create peer connection
    CertificatePair certificate;
  };

  explicit P2PDownloader(const Option &option);
  P2PDownloader(P2PDownloader &&) = delete;
  P2PDownloader &operator=(P2PDownloader &&) = delete;
  ~P2PDownloader();

  int Init(const util::DownloaderOption *opt) override;

  std::shared_ptr<util::DownloaderTask>
  AddTask(const util::DownloaderTaskOption *opt) ;
  void CancelTask(std::shared_ptr<util::DownloaderTask> task) override;
  void PauseTask(std::shared_ptr<util::DownloaderTask> task) override;
  void ResumeTask(std::shared_ptr<util::DownloaderTask> task) override;

private:
  friend class P2PSingleTask;
  void
  onTaskDataReceived(std::shared_ptr<P2PSingleTask> task,
                     std::variant<std::vector<std::byte>, std::string> &&data);

  // run in manager thread
  void addTask(P2PDownloaderTaskOption opt,
               std::shared_ptr<P2PSingleTask> task);
  void cancelTask(std::shared_ptr<util::DownloaderTask> task);
  void pauseTask(std::shared_ptr<util::DownloaderTask> task);
  void resumeTask(std::shared_ptr<util::DownloaderTask> task);

  void initPeerConnection(const std::string &peerId, const std::string &peerSdp,
                          const P2PDownloaderTaskOption &taskOpt,
                          std::shared_ptr<P2PSingleTask> task);

  // post task to thread
  void post(std::function<void()> &&task);
  void run() override;

private:

  Option opt_;
  mutable std::mutex mutex_;

  // peerId --> peer connection
  std::unordered_map<std::string, std::shared_ptr<rtc::PeerConnection>>
      peerConnections_;
  std::unordered_map<const util::DownloaderTask *,
                     std::shared_ptr<P2PSingleTask>>
      tasks_;

  std::condition_variable threadTaskCv_;
  std::deque<std::function<void()>> threadTaskQueue_;

  std::atomic<bool> stopFlag_ = false;
};

} // namespace download
} // namespace dcdn

#endif // _DCDN_SDK_P2P_DOWNLOADER_H_
