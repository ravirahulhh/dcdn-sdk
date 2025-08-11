#ifndef _DCDN_SDK_P2P_SINGLE_TASK_H_
#define _DCDN_SDK_P2P_SINGLE_TASK_H_

#include "dcdn/p2p_downloader.h"
#include "rtc/datachannel.hpp"
#include "util/Downloader.h"

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <variant>
#include <vector>

namespace dcdn {
namespace download {
typedef unsigned long long TaskId;

struct DownloadRequest {
  std::string Url;
  std::string FileHash;
  uint64_t Start;
  uint64_t End;
};

struct TaskParam {
  std::string ContentHash;
  uint64_t Start;
  uint64_t End;
};

class P2PSingleTask : public util::DownloaderTask,
                      public std::enable_shared_from_this<P2PSingleTask> {
public:
  P2PSingleTask(P2PDownloader *manager);
  P2PSingleTask(P2PSingleTask &&) = delete;
  P2PSingleTask &operator=(P2PSingleTask &&) = delete;
  ~P2PSingleTask();

  void Start();
  bool Pause();
  bool Resume();
  bool Cancel();

  // called in network thread (callback-ed by rtc::DataChannel)
  void
  HandleIncomingData(std::variant<std::vector<std::byte>, std::string> &&data);

private:
  friend class P2PDownloader;
  void Init(TaskParam param, std::shared_ptr<rtc::DataChannel> dc);

  // called in manager thread
  void HandleIncomingDataInternal(
      std::variant<std::vector<std::byte>, std::string> &&data);
  size_t nextReadOffset_ = 0;

  TaskId taskId_;
  std::string contentHash_;
  uint64_t start_;
  uint64_t end_;
  size_t totalSize_;

  std::shared_ptr<rtc::DataChannel> dc_;
  std::atomic<size_t> downloaded_{0};
  std::atomic<int> lastError_{0};

  mutable std::mutex dataMutex_;
  std::condition_variable dataAvailableCv_;
  std::condition_variable bufferFreedCv_;

  size_t maxBufferSize_ = 10 * 1024 * 1024; // 10MB

  P2PDownloader *manager_;
};

} // namespace download
} // namespace dcdn

#endif // _DCDN_SDK_P2P_SINGLE_TASK_H_
