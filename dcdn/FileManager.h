#ifndef _DCDN_SDK_FILE_MANAGER_H_
#define _DCDN_SDK_FILE_MANAGER_H_

#include <nlohmann/json.hpp>

#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "BaseManager.h"
#include "Common.h"
#include "EventLoop.h"
#include "JsonTypes.h"
#include "MainManager.h"
#include "util/HttpClient.h"
#include "util/uuid.h"

NS_BEGIN(dcdn)

enum class FileStatus
{
    DOWNLOADING = 1,
    AVAILABLE = 2
};

struct FileItem
{
    uint64_t id;
    FileStatus status;
    std::string path;
    uint64_t lastAccess;
    uint64_t lastReport;
    std::string createdAt;
    std::string fileHash;
    uint64_t blockStart = 0;
    uint64_t blockEnd = 0;
    std::string blockHash;
};

// LRU cache node - unified management of access records and LRU information
struct LRUNode
{
    uint64_t fileId; // File ID used in database
    uint64_t lastAccess;
    uint64_t accessCount;
    uint64_t fileSize; // File size information for LRU eviction calculation
    std::string filePath; // File path for deletion operations
    std::list<uint64_t>::iterator listIter; // Iterator pointing to position in LRU list
    bool isDirty; // Flag indicating whether flush to database is needed
};

struct FileManagerOption
{
    std::string RootPath;
    uint64_t MaxStorageSize = uint64_t(20) * 1024 * 1024 * 1024; // 20GB
    std::uint8_t LRUUpperBoundPercent = 90; // 90% of max storage size
    std::uint8_t LRUTargetPercent = 70; // 70% of max storage size
    uint32_t AccessRecordFlushInterval = 3; // Access record flush interval to database (seconds)
    uint32_t LRUCheckInterval = 3; // LRU check interval (seconds)
    uint32_t ReportInterval = 5; // File reporting interval, 1 hour
    uint32_t ReportBatchSize = 10; // Number of files per report batch
    uint32_t ScanInterval = 3; // Scan cleanup interval, 1 hour
    uint32_t DownloadTimeout = 30 * 60; // Download timeout, 30 minutes
};

struct FileResourceInfo
{
    std::string path;
    uint64_t start;
};

class StorageRef;
class FileManager: public BaseManager, public EventLoop<FileManager>
{
public:
    using json = nlohmann::json;
    FileManager(MainManager* man);
    ~FileManager();
    int Init(const FileManagerOption& opt);
    const FileManagerOption& Option() const
    {
        return mOpt;
    }

    // Get file path by block hash
    // Returns empty string if file not found
    std::string GetPathByBlockHash(const std::string& blockHash, bool needReport = false);
    std::optional<FileResourceInfo> GetUploadFileResource(const std::string& fileHash, uint64_t minStart);
    std::string NewDownloadPath(uint64_t filesize);

    void FlushAccessRecords();
    void CheckAndEliminateFiles();

private:
    void run();
    std::shared_ptr<StorageRef> getDB();
    int initDB();
    int createTable();

    void handleRemoveFile(std::shared_ptr<Event> evt);
    void handleDownloadFileDone(std::shared_ptr<Event> evt);
    void handleDownloadFileFailed(std::shared_ptr<Event> evt);

    void reportHaveFiles(const std::vector<std::tuple<FileItem, std::string>>& files);
    void reportRemoveFile(const FileItem& item);

    void loadAccessRecordsFromDB(); // Load file access records from database

    // Scheduled tasks
    void runLRUThread(); // LRU eviction thread function
    void runFlushThread(); // Access record flush thread function
    void runReportThread(); // Scheduled file reporting thread function
    void runScanThread(); // Scan and clean files, maintain consistency, delete expired download files
    void scanAndCleanInconsistentFiles(); // Unified filesystem and database consistency check and cleanup

    // Helper method: scan filesystem and database, return file difference information
    void scanFilesystemAndDatabase(
        std::vector<std::string>& filesystemFiles,
        std::unordered_map<std::string, std::tuple<uint64_t, std::string, uint64_t, uint64_t>>& dbFilesMap);

    // Helper method: handle orphan files (files in filesystem but not in database)
    uint64_t cleanOrphanFiles(const std::vector<std::string>& orphanFiles);

    // Helper method: handle missing files (files in database but not in filesystem)
    uint64_t cleanMissingFiles(
        const std::vector<std::tuple<uint64_t, std::string, std::string, uint64_t, uint64_t>>& missingFiles);

    void cleanStaleDownloads(); // Clean expired download files

    uint64_t getCurrentTimestamp()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    };
    // Time since file was last written
    uint64_t durSinceFileLastUpdateTime(std::filesystem::path path)
    {
        std::filesystem::file_time_type ftime = std::filesystem::last_write_time(path);
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch() - ftime.time_since_epoch();
        return std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    }

    // LRU-related internal methods
    uint64_t calculateTotalStorageSize();
    void recordFileAccess(uint64_t fileId, const std::string& filePath, uint64_t fileSize);
    void removeLRUFiles(uint64_t currentSize, uint64_t targetSize);

    // db
    std::filesystem::path dbPath() const
    {
        std::filesystem::path dbFile(mMan->Option().WorkDir);
        dbFile.append("files.db");
        return dbFile;
    }

    std::filesystem::path tmpDir() const
    {
        std::filesystem::path tmpPath(mOpt.RootPath);
        tmpPath.append("tmp");
        return tmpPath;
    }

    std::filesystem::path fileDir() const
    {
        std::filesystem::path filesPath(mOpt.RootPath);
        filesPath.append("files");
        return filesPath;
    }

    std::string newFileName()
    {
        return generate_uuid_v4();
    }

    void handleAsyncApiRequestEvent(std::shared_ptr<Event> evt);

private:
    FileManagerOption mOpt;
    std::shared_ptr<StorageRef> mDB;

    // LRU-related member variables - using file ID as key
    std::mutex mLRUMutex;
    std::unordered_map<uint64_t, LRUNode> mLRUCache; // fileId -> LRUNode, unified access record management
    std::list<uint64_t> mLRUList; // Recently used file ID list, newest at head

    // File operation synchronization lock - prevents race conditions between file create/delete and scan cleanup
    // This lock protects: 1. Orphan file scanning and deletion 2. LRU file deletion 3. Expired download file cleanup
    // Ensures no concurrent file create/delete operations during filesystem state scanning
    std::mutex mFileDBOptMutex;

    // Timer-related
    std::chrono::steady_clock::time_point mLastFlushTime;
    std::chrono::steady_clock::time_point mLastLRUCheckTime;

    // LRU thread control
    std::atomic<bool> mShouldStop{false};
    std::thread mLRUThread;
    std::condition_variable mLRUCondition;
    std::mutex mLRUConditionMutex;

    // Flush thread control
    std::thread mFlushThread;
    std::condition_variable mFlushCondition;
    std::mutex mFlushConditionMutex;

    // Report thread control
    std::thread mReportThread;
    std::condition_variable mReportCondition;
    std::mutex mReportConditionMutex;

    // Scan cleanup thread control
    std::thread mScanThread;
    std::condition_variable mScanCondition;
    std::mutex mScanConditionMutex;
};

NS_END

#endif
