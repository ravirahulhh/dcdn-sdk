#include "FileManager.h"
#include "Event.h"
#include "MainManager.h"
#include "SqliteOrmHelper.h"
#include <chrono>
#include <filesystem>
#include <sqlite3.h>
#include <sqlite_orm/sqlite_orm.h>

using namespace sqlite_orm;

NS_BEGIN(dcdn)

auto createFileStorage(const std::string &filename) {
  auto storage = make_storage(
      filename,
      make_unique_index("idx_unique", &FileItem::file_hash,
                        &FileItem::file_hash, &FileItem::block_start,
                        &FileItem::block_end),
      make_index("idx_file_start", &FileItem::file_hash,
                 &FileItem::block_start),
      make_index("idx_last_report", &FileItem::last_report),
      make_table(
          "files",
          make_column("id", &FileItem::id, primary_key().autoincrement()),
          make_column("block_hash", &FileItem::block_hash),
          make_column("file_hash", &FileItem::file_hash),
          make_column("file_path", &FileItem::path),
          make_column("status", &FileItem::status),
          make_column("file_size", &FileItem::file_size),
          make_column("block_start", &FileItem::block_start),
          make_column("block_end", &FileItem::block_end),
          make_column("last_access", &FileItem::last_access),
          make_column("last_report", &FileItem::last_report),
          make_column("created_at", &FileItem::created_at,
                      default_value("CURRENT_TIMESTAMP"))));
  return storage;
}

class StorageRef : public StorageRefImpl<createFileStorage> {
public:
  using Base::Base;
};

FileManager::FileManager(MainManager *man, const FileManagerOption &opt)
    : BaseManager(man), mOpt(opt) {
  mLastFlushTime = std::chrono::steady_clock::now();
  mLastLRUCheckTime = std::chrono::steady_clock::now();
  registerHandler(EventType::FileDownloadDone,
                  &FileManager::handleDownloadFileDone);
  registerHandler(EventType::FileDownloadFailed,
                  &FileManager::handleDownloadFileFailed);
  registerHandler(EventType::RemoveFile, &FileManager::handleRemoveFile);

  // 启动LRU线程
  mLRUThread = std::thread(&FileManager::runLRUThread, this);

  // 启动刷新线程
  mFlushThread = std::thread(&FileManager::runFlushThread, this);
  
  // 启动上报线程
  mReportThread = std::thread(&FileManager::runReportThread, this);
}

FileManager::~FileManager() {
  // 停止所有线程
  mShouldStop = true;
  mLRUCondition.notify_all();
  mFlushCondition.notify_all();
  mReportCondition.notify_all();

  if (mLRUThread.joinable()) {
    mLRUThread.join();
  }

  if (mFlushThread.joinable()) {
    mFlushThread.join();
  }
  
  if (mReportThread.joinable()) {
    mReportThread.join();
  }
}

void FileManager::run() {
  logInfo << "FileManager running";
  // check root dir exists
  if (!std::filesystem::exists(mOpt.RootPath)) {
    std::filesystem::create_directories(mOpt.RootPath);
    logInfo << "Created root directory: " << mOpt.RootPath;
  }

  // check if database exists
  if (!std::filesystem::exists(dbPath())) {
    if (createTable() != ErrorCodeOk) {
      logError << "Failed to create files.db table";
      return;
    }
  }

  if (auto db = getDB()) {
    try {
      db->stor.sync_schema();
    } catch (...) {
      // TODO:handle create db failed
      logError << "Failed to sync schema for files.db";
      return;
    }
  }

  // 从数据库加载文件访问记录到LRU缓存
  loadAccessRecordsFromDB();

  while (true) {
    waitAllEvents(std::chrono::milliseconds(1000));
    // 主循环中只处理事件，刷新任务由独立线程处理
  }
  logInfo << "FileManager exit";
}

std::shared_ptr<StorageRef> FileManager::getDB() {
  if (!mDB) {
    try {
      std::filesystem::path dbFile(mMan->Option().WorkDir);
      dbFile.append("files.db");
      mDB = std::make_shared<StorageRef>(dbFile);
    } catch (std::exception &excp) {
      logWarn << "create config.db exception: " << excp.what();
    } catch (...) {
      logWarn << "create config.db unknown exception";
    }
  };
  return mDB;
}

int FileManager::createTable() {
  sqlite3 *db = nullptr;
  int rc = sqlite3_open(dbPath().c_str(), &db);
  if (rc != SQLITE_OK) {
    logWarn << "create files.db fail";
    sqlite3_close(db);
    return ErrorCodeErr;
  }
  char *errMsg = nullptr;
  const char *sql = R"(
        CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        block_hash TEXT NOT NULL,
        file_hash TEXT NOT NULL,
        file_path TEXT NOT NULL,
        file_size INTEGER,
        block_start INTEGER,
        block_end INTEGER,
        last_access INTEGER,
        last_report INTEGER,
        create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(block_hash, file_hash, block_start, block_end)
        );
        CREATE INDEX IF NOT EXISTS idx_file_start ON files(file_hash, block_start);
        CREATE INDEX IF NOT EXISTS idx_last_access ON files(last_access);
        CREATE INDEX IF NOT EXISTS idx_last_report ON files(last_report);
    )";
  rc = sqlite3_exec(db, sql, nullptr, 0, &errMsg);
  if (rc != SQLITE_OK) {
    logWarn << "create table(files) err:" << errMsg;
    sqlite3_free(errMsg);
    sqlite3_close(db);
    return ErrorCodeErr;
  }
  return ErrorCodeOk;
}

std::string FileManager::GetPathByBlockHash(const std::string &block_hash,
                                            bool need_report) {
  auto db = getDB();
  if (!db) {
    return "";
  }

  try {
    // 使用ORM查询
    auto files =
        db->stor.select(columns(&FileItem::id, &FileItem::path,
                                &FileItem::block_end, &FileItem::block_start),
                        where(c(&FileItem::block_hash) == block_hash and
                              c(&FileItem::status) == FileStatus::AVAILABLE),
                        limit(1));

    if (!files.empty()) {
      auto &file = files[0];
      uint64_t file_id = std::get<0>(file);
      std::string file_path = std::get<1>(file);
      uint64_t block_end = std::get<2>(file);
      uint64_t block_start = std::get<3>(file);
      uint64_t file_size = block_end - block_start;

      // 记录文件访问，使用文件ID
      RecordFileAccess(file_id, file_path, file_size);
      return file_path;
    }

    return "";
  } catch (const std::exception &e) {
    logWarn << "Failed to get path by block hash: " << e.what();
    return "";
  }
}

std::string FileManager::NewDownloadPath(const FileDescriptor &file) {
  auto storage_path = tmpDir();
  std::string file_name = FilePath(file);
  storage_path.append(file_name);
  return storage_path.string();
}

// LRU相关方法实现
void FileManager::RecordFileAccess(uint64_t file_id,
                                   const std::string &file_path,
                                   uint64_t file_size) {
  std::lock_guard<std::mutex> lock(mLRUMutex);

  uint64_t current_time = getCurrentTimestamp();

  auto it = mLRUCache.find(file_id);
  if (it != mLRUCache.end()) {
    // 文件已存在，更新访问时间并移到列表头部
    mLRUList.erase(it->second.list_iter);
    mLRUList.push_front(file_id);
    it->second.list_iter = mLRUList.begin();
    it->second.last_access = current_time;
    it->second.access_count++;
    it->second.is_dirty = true; // 标记为需要刷新到数据库

    // 更新文件大小和路径（如果有变化）
    if (file_size > 0) {
      it->second.file_size = file_size;
    }
    it->second.file_path = file_path;
  } else {
    // 新文件，加入缓存
    mLRUList.push_front(file_id);
    LRUNode node;
    node.file_id = file_id;
    node.last_access = current_time;
    node.access_count = 1;
    node.file_size = file_size;
    node.file_path = file_path;
    node.list_iter = mLRUList.begin();
    node.is_dirty = true;
    mLRUCache[file_id] = node;
  }
}

void FileManager::FlushAccessRecords() {
  std::vector<LRUNode> dirty_nodes;

  {
    std::lock_guard<std::mutex> lock(mLRUMutex);

    // 收集所有脏节点
    for (auto &pair : mLRUCache) {
      if (pair.second.is_dirty) {
        dirty_nodes.push_back(pair.second);
        pair.second.is_dirty = false; // 清除脏标记
      }
    }
  }

  if (dirty_nodes.empty()) {
    return;
  }

  auto db = getDB();
  if (!db) {
    logWarn << "Failed to get database connection for flushing access records";
    return;
  }

  try {
    // 批量更新数据库中文件的访问时间
    for (const auto &node : dirty_nodes) {
      db->stor.update_all(set(c(&FileItem::last_access) = node.last_access),
                          where(c(&FileItem::id) == node.file_id));
    }

    logInfo << "Flushed " << dirty_nodes.size()
            << " access records to database";
  } catch (const std::exception &e) {
    logWarn << "Failed to flush access records: " << e.what();

    // 如果刷新失败，重新标记为脏数据
    std::lock_guard<std::mutex> lock(mLRUMutex);
    for (const auto &node : dirty_nodes) {
      auto it = mLRUCache.find(node.file_id);
      if (it != mLRUCache.end()) {
        it->second.is_dirty = true;
      }
    }
  }
}

uint64_t FileManager::getCurrentTimestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

void FileManager::loadAccessRecordsFromDB() {
  logInfo << "Loading file access records from database";

  auto db = getDB();
  if (!db) {
    logWarn << "Failed to get database connection for loading access records";
    return;
  }

  try {
    // 查询所有文件记录，按最后访问时间降序排列
    auto files = db->stor.select(
        columns(&FileItem::id, &FileItem::path, &FileItem::last_access,
                &FileItem::block_end, &FileItem::block_start),
        where(c(&FileItem::status) == FileStatus::AVAILABLE),
        order_by(&FileItem::last_access).desc());

    std::lock_guard<std::mutex> lock(mLRUMutex);

    // 清空现有的LRU缓存和列表
    mLRUCache.clear();
    mLRUList.clear();

    uint64_t loaded_count = 0;
    for (const auto &file : files) {
      uint64_t file_id = std::get<0>(file);
      std::string file_path = std::get<1>(file);
      uint64_t last_access = std::get<2>(file);
      uint64_t block_end = std::get<3>(file);
      uint64_t block_start = std::get<4>(file);
      uint64_t file_size = block_end - block_start;

      // 检查文件是否实际存在于磁盘上
      std::filesystem::path full_path(mOpt.RootPath);
      full_path.append(file_path);

      if (!std::filesystem::exists(full_path)) {
        logDebug << "File not found on disk, skipping: " << file_path;
        // 可以选择从数据库中删除这个记录，但为了安全起见这里只是跳过
        continue;
      }

      // 添加到LRU列表和缓存
      mLRUList.push_back(file_id); // 按访问时间降序添加，最新的在前面

      LRUNode node;
      node.file_id = file_id;
      node.last_access = last_access;
      node.access_count = 1; // 初始化访问次数
      node.file_size = file_size;
      node.file_path = file_path;
      node.list_iter = std::prev(mLRUList.end()); // 指向刚插入的元素
      node.is_dirty = false; // 从数据库加载的记录不需要立即刷新

      mLRUCache[file_id] = node;
      loaded_count++;
    }

    logInfo << "Loaded " << loaded_count
            << " file access records from database";

  } catch (const std::exception &e) {
    logWarn << "Failed to load access records from database: " << e.what();
  }
}

uint64_t FileManager::calculateTotalStorageSize() {
  auto db = getDB();
  if (!db) {
    logWarn << "Failed to get database connection for calculating storage size";
    return 0;
  }

  try {
    // 计算所有文件的总大小
    // 先获取所有文件的block信息，然后计算总和
    auto files =
        db->stor.select(columns(&FileItem::block_end, &FileItem::block_start));

    uint64_t total_size = 0;
    for (const auto &file : files) {
      uint64_t block_end = std::get<0>(file);
      uint64_t block_start = std::get<1>(file);
      total_size += (block_end - block_start);
    }

    logDebug << "Calculated total storage size from database: " << total_size
             << " bytes";
    return total_size;
  } catch (const std::exception &e) {
    logWarn << "Failed to calculate storage size from database: " << e.what();
    return 0;
  }
}

void FileManager::CheckAndEliminateFiles() {
  uint64_t current_size = calculateTotalStorageSize();
  uint64_t upper_bound =
      (mOpt.MaxStorageSize * mOpt.LRUUpperBoundPercent) / 100;

  if (current_size > upper_bound) {
    uint64_t target_size = (mOpt.MaxStorageSize * mOpt.LRUTargetPercent) / 100;
    logInfo << "Storage size (" << current_size << ") exceeds upper bound ("
            << upper_bound
            << "), starting LRU elimination to target size: " << target_size;

    removeLRUFiles(target_size);
  }
}

void FileManager::removeLRUFiles(uint64_t target_size) {
  std::vector<uint64_t> files_to_remove;
  uint64_t current_size = calculateTotalStorageSize();
  uint64_t size_to_free = 0;

  {
    std::lock_guard<std::mutex> lock(mLRUMutex);

    // 从LRU列表尾部开始（最旧的文件），选择要删除的文件
    auto it = mLRUList.rbegin();
    while (it != mLRUList.rend() && current_size > target_size) {
      uint64_t file_id = *it;

      // 获取文件信息
      auto cache_it = mLRUCache.find(file_id);
      if (cache_it != mLRUCache.end()) {
        files_to_remove.push_back(file_id);

        // 使用内存中缓存的文件大小
        uint64_t file_size = cache_it->second.file_size;
        if (file_size > 0) {
          size_to_free += file_size;
          current_size =
              (current_size > file_size) ? current_size - file_size : 0;
        } else {
          // 如果内存中没有大小信息，从数据库获取 block_end - block_start
          try {
            auto db = getDB();
            if (db) {
              auto result = db->stor.select(
                  columns(&FileItem::block_end, &FileItem::block_start),
                  where(c(&FileItem::id) == file_id), limit(1));

              if (!result.empty()) {
                uint64_t block_end = std::get<0>(result[0]);
                uint64_t block_start = std::get<1>(result[0]);
                uint64_t block_size = block_end - block_start;
                size_to_free += block_size;
                current_size =
                    (current_size > block_size) ? current_size - block_size : 0;

                // 更新缓存中的文件大小
                cache_it->second.file_size = block_size;
              }
            }
          } catch (...) {
            // 如果数据库查询失败，使用估算值
            uint64_t estimated_size = 1024 * 1024; // 1MB估算
            size_to_free += estimated_size;
            current_size = (current_size > estimated_size)
                               ? current_size - estimated_size
                               : 0;
          }
        }
      }
      ++it;
    }
  }

  if (files_to_remove.empty()) {
    logInfo << "No files to remove for LRU elimination";
    return;
  }

  logInfo << "Starting LRU elimination: " << files_to_remove.size()
          << " files selected, estimated " << size_to_free << " bytes to free";

  auto db = getDB();
  if (!db) {
    logWarn << "Failed to get database connection for LRU elimination";
    return;
  }

  uint64_t actual_freed = 0;
  uint64_t successful_removals = 0;

  // 删除选中的文件
  for (uint64_t file_id : files_to_remove) {
    try {
      std::string file_path;
      uint64_t file_size = 0;

      // 先从内存缓存获取文件信息
      {
        std::lock_guard<std::mutex> lock(mLRUMutex);
        auto cache_it = mLRUCache.find(file_id);
        if (cache_it != mLRUCache.end()) {
          file_path = cache_it->second.file_path;
          file_size = cache_it->second.file_size;
        }
      }

      // 如果缓存中没有，从数据库获取
      if (file_path.empty()) {
        try {
          auto db = getDB();
          if (db) {
            auto result =
                db->stor.select(columns(&FileItem::path, &FileItem::block_end,
                                        &FileItem::block_start),
                                where(c(&FileItem::id) == file_id), limit(1));

            if (!result.empty()) {
              file_path = std::get<0>(result[0]);
              uint64_t block_end = std::get<1>(result[0]);
              uint64_t block_start = std::get<2>(result[0]);
              file_size = block_end - block_start;
            }
          }
        } catch (const std::exception &e) {
          logWarn << "Failed to query file info for ID " << file_id << ": "
                  << e.what();
          continue;
        }
      }

      if (file_path.empty()) {
        logWarn << "File path not found for ID: " << file_id;
        continue;
      }

      // 删除物理文件
      std::filesystem::path full_path(mOpt.RootPath);
      full_path.append(file_path);

      if (std::filesystem::exists(full_path)) {
        std::filesystem::remove(full_path);
        actual_freed += file_size;
        successful_removals++;
        logInfo << "Removed LRU file: " << full_path << " (size: " << file_size
                << " bytes)";
      }

      // 从数据库删除记录
      try {
        if (db) {
          db->stor.remove<FileItem>(file_id);
        }
      } catch (const std::exception &e) {
        logWarn << "Failed to delete file record for ID " << file_id << ": "
                << e.what();
      }

      // 从LRU缓存和列表中删除
      {
        std::lock_guard<std::mutex> lock(mLRUMutex);
        auto cache_it = mLRUCache.find(file_id);
        if (cache_it != mLRUCache.end()) {
          mLRUList.erase(cache_it->second.list_iter);
          mLRUCache.erase(cache_it);
        }
      }

    } catch (const std::exception &e) {
      logWarn << "Failed to remove file ID " << file_id << ": " << e.what();
    }
  }

  logInfo << "LRU elimination completed: " << successful_removals << "/"
          << files_to_remove.size() << " files removed, " << actual_freed
          << " bytes freed";
}

void FileManager::reportHaveFile(const FileItem &item) {
  if (mOpt.PCDNReportUrl.empty()) {
    return;
  }

  try {
    nlohmann::json j;
    j["action"] = "have_file";
    j["file_hash"] = item.file_hash;
    j["block_hash"] = item.block_hash;
    j["block_start"] = item.block_start;
    j["block_end"] = item.block_end;
    j["file_size"] = item.file_size;
    j["last_access"] = item.last_access;
    
    std::string resp;
    mClient.Post(mOpt.PCDNReportUrl.c_str(), j.dump(), resp, "application/json");
    logDebug << "Successfully reported have file: " << item.file_hash 
             << " (block: " << item.block_start << "-" << item.block_end << ")";
  } catch (const std::exception &e) {
    logWarn << "Failed to report have file: " << e.what();
  }
}

void FileManager::reportRemoveFile(const FileItem &item) {
  if (mOpt.PCDNReportUrl.empty()) {
    return;
  }

  try {
    nlohmann::json j;
    j["file_hash"] = item.file_hash;
    j["block_start"] = item.block_start;
    j["block_end"] = item.block_end;

    std::string resp;
    mClient.Post(mOpt.PCDNReportUrl.c_str(), j.dump(), resp,
                 "application/json");
  } catch (const std::exception &e) {
    logWarn << "Failed to report remove file: " << e.what();
  }
}

void FileManager::handleDownloadFileDone(std::shared_ptr<Event> evt) {
  // TODO: 实现下载完成处理逻辑
  logInfo << "File download completed";
}

void FileManager::handleDownloadFileFailed(std::shared_ptr<Event> evt) {
  // TODO: 实现下载失败处理逻辑
  logWarn << "File download failed";
}

void FileManager::handleRemoveFile(std::shared_ptr<Event> evt) {
  // TODO: 实现文件移除处理逻辑
  logInfo << "File remove request received";
}

void FileManager::runLRUThread() {
  logInfo << "LRU thread started";

  while (!mShouldStop) {
    try {
      // 等待指定的间隔时间，或者被停止信号唤醒
      std::unique_lock<std::mutex> lock(mLRUConditionMutex);
      if (mLRUCondition.wait_for(lock,
                                 std::chrono::seconds(mOpt.LRUCheckInterval),
                                 [this] { return mShouldStop.load(); })) {
        // 被停止信号唤醒
        break;
      }

      // 执行LRU检查和淘汰
      CheckAndEliminateFiles();

    } catch (const std::exception &e) {
      logWarn << "LRU thread exception: " << e.what();
      // 发生异常时等待一段时间再重试，避免快速循环
      std::this_thread::sleep_for(std::chrono::seconds(10));
    } catch (...) {
      logWarn << "LRU thread unknown exception";
      std::this_thread::sleep_for(std::chrono::seconds(10));
    }
  }

  logInfo << "LRU thread stopped";
}

void FileManager::runFlushThread() {
  logInfo << "Flush thread started";

  while (!mShouldStop) {
    try {
      // 等待指定的刷新间隔时间，或者被停止信号唤醒
      std::unique_lock<std::mutex> lock(mFlushConditionMutex);
      if (mFlushCondition.wait_for(
              lock, std::chrono::seconds(mOpt.AccessRecordFlushInterval),
              [this] { return mShouldStop.load(); })) {
        // 被停止信号唤醒
        break;
      }

      // 执行访问记录刷新
      FlushAccessRecords();

    } catch (const std::exception &e) {
      logWarn << "Flush thread exception: " << e.what();
      // 发生异常时等待一段时间再重试，避免快速循环
      std::this_thread::sleep_for(std::chrono::seconds(10));
    } catch (...) {
      logWarn << "Flush thread unknown exception";
      std::this_thread::sleep_for(std::chrono::seconds(10));
    }
  }

  // 线程停止前最后刷新一次，确保数据不丢失
  try {
    FlushAccessRecords();
    logInfo << "Final flush completed before thread exit";
  } catch (const std::exception &e) {
    logWarn << "Final flush failed: " << e.what();
  } catch (...) {
    logWarn << "Final flush unknown exception";
  }

  logInfo << "Flush thread stopped";
}

void FileManager::runReportThread() {
  logInfo << "Report thread started";

  while (!mShouldStop) {
    try {
      // 等待指定的上报间隔时间，或者被停止信号唤醒
      std::unique_lock<std::mutex> lock(mReportConditionMutex);
      if (mReportCondition.wait_for(
              lock, std::chrono::seconds(mOpt.ReportInterval),
              [this] { return mShouldStop.load(); })) {
        // 被停止信号唤醒
        break;
      }

      // 如果没有配置上报URL，跳过上报
      if (mOpt.PCDNReportUrl.empty()) {
        continue;
      }

      // 从数据库查询最久没有上报的文件
      auto db = getDB();
      if (!db) {
        logWarn << "Failed to get database connection for reporting files";
        continue;
      }

      try {
        // 查询最久没有上报的文件，按last_report升序排列
        // 如果last_report为空或0，则优先上报
        auto files = db->stor.select(
            columns(&FileItem::id, &FileItem::file_hash, &FileItem::block_hash,
                    &FileItem::block_start, &FileItem::block_end,
                    &FileItem::file_size, &FileItem::last_access),
            where(c(&FileItem::status) == FileStatus::AVAILABLE),
            order_by(&FileItem::last_report).asc(),
            limit(mOpt.ReportBatchSize));

        if (files.empty()) {
          logDebug << "No files to report";
          continue;
        }

        logInfo << "Reporting " << files.size() << " files to PCDN server";

        uint64_t current_time = getCurrentTimestamp();
        uint64_t successful_reports = 0;

        // 逐个上报文件
        for (const auto &file : files) {
          try {
            FileItem item;
            item.id = std::get<0>(file);
            item.file_hash = std::get<1>(file);
            item.block_hash = std::get<2>(file);
            item.block_start = std::get<3>(file);
            item.block_end = std::get<4>(file);
            item.file_size = std::get<5>(file);
            item.last_access = std::get<6>(file);

            // 上报文件
            reportHaveFile(item);

            // 更新数据库中的last_report时间
            db->stor.update_all(set(c(&FileItem::last_report) = current_time),
                                where(c(&FileItem::id) == item.id));

            successful_reports++;

          } catch (const std::exception &e) {
            logWarn << "Failed to report file ID " << std::get<0>(file) << ": "
                    << e.what();
          }
        }

        logInfo << "Successfully reported " << successful_reports << "/"
                << files.size() << " files";

      } catch (const std::exception &e) {
        logWarn << "Failed to query files for reporting: " << e.what();
      }

    } catch (const std::exception &e) {
      logWarn << "Report thread exception: " << e.what();
      // 发生异常时等待一段时间再重试，避免快速循环
      std::this_thread::sleep_for(std::chrono::seconds(10));
    } catch (...) {
      logWarn << "Report thread unknown exception";
      std::this_thread::sleep_for(std::chrono::seconds(10));
    }
  }

  logInfo << "Report thread stopped";
}

NS_END
