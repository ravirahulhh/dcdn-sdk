#ifndef _DCDN_SDK_FILE_MANAGER_H_
#define _DCDN_SDK_FILE_MANAGER_H_

#include "BaseManager.h"
#include "Common.h"
#include "EventLoop.h"
#include "util/HttpClient.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <list>
#include <mutex>
#include <nlohmann/json.hpp>
#include <thread>
#include <unordered_map>

NS_BEGIN(dcdn)

enum class FileStatus { DOWNLOADING = 1, AVAILABLE = 2 };

struct FileItem : BlockInfo {
  uint64_t id;
  FileStatus status;
  std::string path;
  uint64_t file_size;
  uint64_t last_access; // 改名为更语义化的名称
  uint64_t last_report;
  std::string created_at;
};

// LRU缓存节点 - 统一管理访问记录和LRU信息
struct LRUNode {
  uint64_t file_id; // 使用数据库中的文件ID
  uint64_t last_access;
  uint64_t access_count;
  uint64_t file_size;                      // 文件大小信息，用于LRU淘汰计算
  std::string file_path;                   // 文件路径，用于删除操作
  std::list<uint64_t>::iterator list_iter; // 指向LRU列表中的位置
  bool is_dirty;                           // 标记是否需要刷新到数据库
};

struct FileManagerOption {
  std::string RootPath;
  uint64_t MaxStorageSize = 20 * 1024 * 1024 * 1024; // 20GB
  std::uint8_t LRUUpperBoundPercent = 90;            // 90% of max storage size
  std::uint8_t LRUTargetPercent = 70;                // 70% of max storage size
  std::string PCDNReportUrl;
  uint32_t AccessRecordFlushInterval = 60; // 访问记录刷新到数据库的间隔(秒)
  uint32_t LRUCheckInterval = 300;         // LRU检查间隔(秒)
  uint32_t ReportInterval = 60 * 60;       // 文件上报间隔1小时
  uint32_t ReportBatchSize = 10;           // 每次上报的文件数量
};

class StorageRef;
class FileManager : public BaseManager, public EventLoop<FileManager> {
public:
  using json = nlohmann::json;
  FileManager(MainManager *man, const FileManagerOption &opt)
      : BaseManager(man), mOpt(opt) {}
  ~FileManager();
  const FileManagerOption &Option() const { return mOpt; }

  // Get file path by block hash
  // Returns empty string if file not found
  std::string GetPathByBlockHash(const std::string &block_hash,
                                 bool need_report = false);
  std::string NewDownloadPath(const FileDescriptor &file);

  // LRU相关方法
  void RecordFileAccess(uint64_t file_id, const std::string &file_path,
                        uint64_t file_size);
  void FlushAccessRecords();
  void CheckAndEliminateFiles();

public:
  std::string FilePath(const FileDescriptor &file) {
    if (std::holds_alternative<BlockInfo>(file)) {
      const auto &block = std::get<BlockInfo>(file);
      return "blk_" + block.file_hash + "_" +
             std::to_string(block.block_start) + "_" +
             std::to_string(block.block_end);
    } else if (std::holds_alternative<Url>(file)) {
      std::string url = std::get<Url>(file);
      for (auto &c : url) {
        if (!std::isalnum(static_cast<unsigned char>(c))) {
          c = '_';
        }
      }
      return "url_" + url;
    }
    return "";
  }

private:
  void run();
  int init();
  std::shared_ptr<StorageRef> getDB();
  int createTable();

  void handleRemoveFile(std::shared_ptr<Event> evt);
  void handleDownloadFileDone(std::shared_ptr<Event> evt);
  void handleDownloadFileFailed(std::shared_ptr<Event> evt);

  void reportHaveFile(const FileItem &item);
  void reportRemoveFile(const FileItem &item);

  // jobs
  void scanFiles();
  void reportFiles();
  void eliminateFiles();

  // LRU相关内部方法
  uint64_t getCurrentTimestamp();
  uint64_t calculateTotalStorageSize();
  void removeLRUFiles(uint64_t target_size);
  void runLRUThread();            // LRU线程函数
  void loadAccessRecordsFromDB(); // 从数据库加载文件访问记录
  void runFlushThread();          // 刷新访问记录线程函数
  void runReportThread();         // 定时上报文件线程函数

  // db
  std::filesystem::path dbPath() const {
    std::filesystem::path dbFile(mMan->Option().WorkDir);
    dbFile.append("files.db");
    return dbFile;
  }

  std::filesystem::path tmpDir() const {
    std::filesystem::path tmpPath(mMan->Option().WorkDir);
    tmpPath.append("tmp");
    return tmpPath;
  }

private:
  FileManagerOption mOpt;
  std::shared_ptr<StorageRef> mDB;
  util::HttpClient mClient;

  // LRU相关成员变量 - 使用文件ID作为key
  std::mutex mLRUMutex;
  std::unordered_map<uint64_t, LRUNode>
      mLRUCache;                // file_id -> LRUNode，统一管理访问记录
  std::list<uint64_t> mLRUList; // 最近使用的文件ID列表，头部最新

  // 定时器相关
  std::chrono::steady_clock::time_point mLastFlushTime;
  std::chrono::steady_clock::time_point mLastLRUCheckTime;

  // LRU线程控制
  std::atomic<bool> mShouldStop{false};
  std::thread mLRUThread;
  std::condition_variable mLRUCondition;
  std::mutex mLRUConditionMutex;

  // 刷新线程控制
  std::thread mFlushThread;
  std::condition_variable mFlushCondition;
  std::mutex mFlushConditionMutex;

  // 上报线程控制
  std::thread mReportThread;
  std::condition_variable mReportCondition;
  std::mutex mReportConditionMutex;
};

NS_END

#endif
