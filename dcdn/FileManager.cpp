#include "FileManager.h"

#include <plog/Initializers/RollingFileInitializer.h>
#include <sqlite3.h>
#include <sqlite_orm/sqlite_orm.h>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <variant>
#include <vector>

#include "ApiClient.h"
#include "Event.h"
#include "MainManager.h"
#include "SqliteOrmHelper.h"

using namespace sqlite_orm;

// Provide sqlite_orm type mapping for FileStatus enum
namespace sqlite_orm {
template<>
struct type_printer<dcdn::FileStatus>: public integer_printer
{
};

template<>
struct statement_binder<dcdn::FileStatus>
{
    int bind(sqlite3_stmt* stmt, int index, const dcdn::FileStatus& value)
    {
        return statement_binder<int>().bind(stmt, index, static_cast<int>(value));
    }
};

template<>
struct field_printer<dcdn::FileStatus>
{
    std::string operator()(const dcdn::FileStatus& t) const
    {
        return std::to_string(static_cast<int>(t));
    }
};

template<>
struct row_extractor<dcdn::FileStatus>
{
    dcdn::FileStatus extract(const char* rowValue)
    {
        return static_cast<dcdn::FileStatus>(std::atoi(rowValue));
    }

    dcdn::FileStatus extract(sqlite3_stmt* stmt, int columnIndex)
    {
        return static_cast<dcdn::FileStatus>(sqlite3_column_int(stmt, columnIndex));
    }
};
} // namespace sqlite_orm

NS_BEGIN(dcdn)

auto createFileStorage(const std::string& filename)
{
    try {
        auto storage = make_storage(
            filename,
            make_index("idx_file_start", &FileItem::fileHash, &FileItem::blockStart),
            make_index("idx_last_report", &FileItem::lastReport),
            make_table(
                "files",
                make_column("id", &FileItem::id, primary_key().autoincrement()),
                make_column("block_hash", &FileItem::blockHash),
                make_column("file_hash", &FileItem::fileHash),
                make_column("file_path", &FileItem::path),
                make_column("status", &FileItem::status),
                make_column("block_start", &FileItem::blockStart),
                make_column("block_end", &FileItem::blockEnd),
                make_column("last_access", &FileItem::lastAccess),
                make_column("last_report", &FileItem::lastReport),
                make_column("created_at", &FileItem::createdAt, default_value("CURRENT_TIMESTAMP"))));
        return storage;
    } catch (const std::exception& e) {
        logWarn << "Failed to create file storage: " << e.what();
    }
}

class StorageRef: public StorageRefImpl<createFileStorage>
{
public:
    using Base::Base;
};

FileManager::FileManager(MainManager* man): BaseManager(man) {}

int FileManager::Init(const FileManagerOption& opt)
{
    mOpt = opt;

    if (mMan->Option().WorkDir.empty()) {
        logError << "Work directory is not set in MainManager";
        return -1;
    }

    if (!std::filesystem::exists(mMan->Option().WorkDir)) {
        std::filesystem::create_directories(mMan->Option().WorkDir);
        logDebug << "Created work directory";
    }

    if (opt.RootPath.empty()) {
        logError << "Root path is not set in FileManagerOption";
        return -1;
    }

    if (!std::filesystem::exists(opt.RootPath)) {
        std::filesystem::create_directories(opt.RootPath);
        logDebug << "Created root directory: " << opt.RootPath;
    }

    // check if database exists
    logDebug << "FileManager init with db path: " << dbPath().string();
    if (!std::filesystem::exists(dbPath())) {
        if (createTable() != ErrorCodeOk) {
            logError << "Failed to create files.db table";
            return -1;
        }
    }

    logDebug << "FileManager init with db path: " << dbPath().string();
    if (initDB() != 0) {
        logError << "Failed to initialize database";
        return -1;
    }
    if (auto db = getDB()) {
        try {
            db->stor.sync_schema();
        } catch (...) {
            logError << "Failed to sync schema for files.db";
            return -1;
        }
    }

    // Load file access records from database to LRU cache
    logDebug << "Loading file access records from database";
    loadAccessRecordsFromDB();

    // Initialize scheduled tasks
    logDebug << "Initializing scheduled tasks";
    initializeTasks();

    logDebug << "Registering event handlers";
    registerHandler(EventType::FileDownloadDone, &FileManager::handleDownloadFileDone);
    registerHandler(EventType::FileDownloadFailed, &FileManager::handleDownloadFileFailed);
    registerHandler(EventType::RemoveFile, &FileManager::handleRemoveFile);
    return 0;
}

FileManager::~FileManager()
{
    try {
        // Stop all threads
        mShouldStop = true;
        mTaskCondition.notify_all();

        if (mTaskWorkerThread.joinable()) {
            mTaskWorkerThread.join();
        }
    } catch (const std::exception& e) {
        logError << "Exception during FileManager destruction: " << e.what();
    } catch (...) {
        logError << "Unknown exception during FileManager destruction";
    }
}

void FileManager::run()
{
    logDebug << "FileManager running";
    // check root dir exists
    if (!std::filesystem::exists(mOpt.RootPath)) {
        std::filesystem::create_directories(mOpt.RootPath);
        logDebug << "Created root directory: " << mOpt.RootPath;
    }

    if (!std::filesystem::exists(tmpDir())) {
        std::filesystem::create_directories(tmpDir());
        logDebug << "Created tmp directory: " << tmpDir();
    }

    if (!std::filesystem::exists(fileDir())) {
        std::filesystem::create_directories(fileDir());
        logDebug << "Created file directory: " << fileDir();
    }

    // Start task worker thread
    logDebug << "Starting task worker thread";
    mTaskWorkerThread = std::thread(&FileManager::runTaskWorkerThread, this);

    while (true) {
        // Main loop only handles events, scheduled tasks are handled by task worker thread
        waitAllEvents(std::chrono::milliseconds(1000));
    }
    logDebug << "FileManager exit";
}

std::shared_ptr<StorageRef> FileManager::getDB()
{
    return mDB;
}

int FileManager::initDB()
{
    if (!mDB) {
        try {
            std::filesystem::path dbFile(mMan->Option().WorkDir);
            dbFile.append("files.db");
            mDB = std::make_shared<StorageRef>(dbFile);
            return 0;
        } catch (std::exception& excp) {
            logWarn << "create config.db exception: " << excp.what();
            return -1;
        } catch (...) {
            logWarn << "create config.db unknown exception";
            return -1;
        }
    };
    return 0;
}

int FileManager::createTable()
{
    sqlite3* db = nullptr;
    int rc = sqlite3_open(dbPath().c_str(), &db);
    if (rc != SQLITE_OK) {
        logWarn << "create files.db fail";
        sqlite3_close(db);
        return ErrorCodeErr;
    }
    char* errMsg = nullptr;
    const char* sql = R"(
        CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        block_hash TEXT NOT NULL,
        file_hash TEXT NOT NULL,
        file_path TEXT NOT NULL,
        status INTEGER NOT NULL DEFAULT 1,
        block_start INTEGER,
        block_end INTEGER,
        last_access INTEGER,
        last_report INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_file_hash ON files(file_hash);
        CREATE INDEX IF NOT EXISTS idx_block_start ON files(block_start);
        CREATE INDEX IF NOT EXISTS idx_block_end ON files(block_end);
        CREATE INDEX IF NOT EXISTS idx_last_access ON files(last_access);
        CREATE INDEX IF NOT EXISTS idx_last_report ON files(last_report);
    )";
    rc = sqlite3_exec(db, sql, nullptr, 0, &errMsg);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("Failed to create files table: " + std::string(errMsg));
        logWarn << "create table(files) err:" << errMsg;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        return ErrorCodeErr;
    }
    return ErrorCodeOk;
}

std::string FileManager::GetPathByBlockHash(const std::string& blockHash, bool needReport)
{
    auto db = getDB();
    if (!db) {
        return "";
    }

    try {
        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
        auto files = db->stor.select(
            columns(&FileItem::id, &FileItem::path, &FileItem::blockEnd, &FileItem::blockStart),
            where(c(&FileItem::blockHash) == blockHash and c(&FileItem::status) == FileStatus::AVAILABLE),
            limit(1));

        if (!files.empty()) {
            auto& file = files[0];
            uint64_t fileId = std::get<0>(file);
            std::string filePath = std::get<1>(file);
            uint64_t blockEnd = std::get<2>(file);
            uint64_t blockStart = std::get<3>(file);
            uint64_t fileSize = blockEnd - blockStart;
            if (needReport) {
                recordFileAccess(fileId, filePath, fileSize);
            }
            // Record file access, using file ID
            return filePath;
        } else {
            if (needReport) {
                logWarn << "File not found for block hash: " << blockHash;
                reportRemoveFile({0, FileStatus::AVAILABLE, "", 0, 0, "", blockHash, 0, 0, ""});
            }
        }

        return "";
    } catch (const std::exception& e) {
        logWarn << "Failed to get path by block hash: " << e.what();
        return "";
    }
}

std::optional<FileResourceInfo> FileManager::GetUploadFileResource(const std::string& fileHash, uint64_t reqStart)
{
    auto db = getDB();
    if (!db) {
        return std::nullopt;
    }
    logDebug << "GetUploadFileResource for fileHash: " << fileHash << ", reqStart: " << reqStart;
    try {
        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
        auto files = db->stor.select(
            columns(&FileItem::id, &FileItem::path, &FileItem::blockStart, &FileItem::blockEnd),
            where(
                c(&FileItem::fileHash) == fileHash and c(&FileItem::blockStart) <= reqStart and
                c(&FileItem::blockEnd) >= reqStart and c(&FileItem::status) == FileStatus::AVAILABLE),
            order_by(&FileItem::blockEnd).desc(),
            limit(1));

        if (!files.empty()) {
            auto& file = files[0];
            uint64_t fileId = std::get<0>(file);
            std::string filePath = std::get<1>(file);
            uint64_t blockStart = std::get<2>(file);
            uint64_t blockEnd = std::get<3>(file);
            uint64_t fileSize = blockEnd - blockStart;
            recordFileAccess(fileId, filePath, fileSize);
            return FileResourceInfo{filePath, blockStart};
        } else {
            // Report file index service remove all files with this fileHash
            // even if node still have some blocks of this file
            reportRemoveFile({0, FileStatus::AVAILABLE, "", 0, 0, "", fileHash, 0, 0, ""});
        }

        return std::nullopt;
    } catch (const std::exception& e) {
        logWarn << "Failed to get file resource: " << e.what();
        return std::nullopt;
    }
}

std::string FileManager::NewDownloadPath(uint64_t fileSize)
{
    auto tmpPath = tmpDir();
    std::string fileName = newFileName();
    tmpPath.append(fileName);
    {
        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
        if (!std::filesystem::exists(tmpPath)) {
            std::ofstream ofs(tmpPath);
            if (!ofs) {
                logWarn << "Failed to create new download file: " << tmpPath;
                return "";
            }
            ofs.close();
        }
        logDebug << "New download file created: " << tmpPath;
        // Write to database
        auto db = getDB();
        if (!db) {
            logWarn << "Failed to get database connection for new download file";
            return "";
        }
        try {
            FileItem item;
            item.path = tmpPath.string();
            item.status = FileStatus::DOWNLOADING;
            item.lastAccess = getCurrentTimestamp();
            item.lastReport = 0;
            db->stor.insert(item);
        } catch (const std::exception& e) {
            logWarn << "Failed to insert new download file into database: " << e.what();
            return "";
        }
    }
    return tmpPath.string();
}

// LRU related method implementations
void FileManager::recordFileAccess(uint64_t fileId, const std::string& filePath, uint64_t fileSize)
{
    std::lock_guard<std::mutex> lock(mLRUMutex);

    uint64_t currentTime = getCurrentTimestamp();

    auto it = mLRUCache.find(fileId);
    if (it != mLRUCache.end()) {
        // File already exists, update access time and move to head of list
        mLRUList.erase(it->second.listIter);
        mLRUList.push_front(fileId);
        it->second.listIter = mLRUList.begin();
        it->second.lastAccess = currentTime;
        it->second.accessCount++;
        it->second.isDirty = true; // Mark as needing database refresh

        // Update file size and path (if changed)
        if (fileSize > 0) {
            it->second.fileSize = fileSize;
        }
        it->second.filePath = filePath;
    } else {
        // New file, add to cache
        mLRUList.push_front(fileId);
        LRUNode node;
        node.fileId = fileId;
        node.lastAccess = currentTime;
        node.accessCount = 1;
        node.fileSize = fileSize;
        node.filePath = filePath;
        node.listIter = mLRUList.begin();
        node.isDirty = true;
        mLRUCache[fileId] = node;
    }
}

void FileManager::FlushAccessRecords()
{
    std::vector<LRUNode> dirtyNodes;

    {
        std::lock_guard<std::mutex> lock(mLRUMutex);

        // Collect all dirty nodes
        for (auto& pair : mLRUCache) {
            if (pair.second.isDirty) {
                dirtyNodes.push_back(pair.second);
                pair.second.isDirty = false; // Clear dirty flag
            }
        }
    }

    if (dirtyNodes.empty()) {
        return;
    }

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for flushing access records";
        return;
    }

    try {
        // Batch update file access times in database
        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
        for (const auto& node : dirtyNodes) {
            db->stor.update_all(
                set(c(&FileItem::lastAccess) = node.lastAccess), where(c(&FileItem::id) == node.fileId));
        }

        logDebug << "Flushed " << dirtyNodes.size() << " access records to database";
    } catch (const std::exception& e) {
        logWarn << "Failed to flush access records: " << e.what();

        // If flush fails, re-mark as dirty data
        std::lock_guard<std::mutex> lock(mLRUMutex);
        for (const auto& node : dirtyNodes) {
            auto it = mLRUCache.find(node.fileId);
            if (it != mLRUCache.end()) {
                it->second.isDirty = true;
            }
        }
    }
}

void FileManager::loadAccessRecordsFromDB()
{
    logDebug << "Loading file access records from database";

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for loading access records";
        return;
    }

    try {
        // Query all file records, sorted by last access time in descending order
        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
        auto files = db->stor.select(
            columns(&FileItem::id, &FileItem::path, &FileItem::lastAccess, &FileItem::blockEnd, &FileItem::blockStart),
            where(c(&FileItem::status) == FileStatus::AVAILABLE),
            order_by(&FileItem::lastAccess).desc());

        std::lock_guard<std::mutex> lock(mLRUMutex);

        // Clear existing LRU cache and list
        mLRUCache.clear();
        mLRUList.clear();

        uint64_t loadedCount = 0;
        for (const auto& file : files) {
            uint64_t fileId = std::get<0>(file);
            std::string filePath = std::get<1>(file);
            uint64_t lastAccess = std::get<2>(file);
            uint64_t blockEnd = std::get<3>(file);
            uint64_t blockStart = std::get<4>(file);
            uint64_t fileSize = blockEnd - blockStart;

            // Add to LRU list and cache
            mLRUList.push_back(fileId); // Add in descending order by access time, newest first

            LRUNode node;
            node.fileId = fileId;
            node.lastAccess = lastAccess;
            node.accessCount = 1; // Initialize access count
            node.fileSize = fileSize;
            node.filePath = filePath;
            node.listIter = std::prev(mLRUList.end()); // Point to just inserted element
            node.isDirty = false; // Records loaded from database don't need immediate refresh

            mLRUCache[fileId] = node;
            loadedCount++;
        }

        logDebug << "Loaded " << loadedCount << " file access records from database";

    } catch (const std::exception& e) {
        logWarn << "Failed to load access records from database: " << e.what();
    }
}

uint64_t FileManager::calculateTotalStorageSize()
{
    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for calculating storage size";
        return 0;
    }

    try {
        // Calculate total size of all files
        // First get all file block information, then calculate sum
        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
        auto files = db->stor.select(columns(&FileItem::blockEnd, &FileItem::blockStart));

        uint64_t totalSize = 0;
        for (const auto& file : files) {
            uint64_t blockEnd = std::get<0>(file);
            uint64_t blockStart = std::get<1>(file);
            totalSize += (blockEnd - blockStart);
        }

        logDebug << "Calculated total storage size from database: " << totalSize << " bytes";
        return totalSize;
    } catch (const std::exception& e) {
        logWarn << "Failed to calculate storage size from database: " << e.what();
        return 0;
    }
}

void FileManager::CheckAndEliminateFiles()
{
    uint64_t currentSize = calculateTotalStorageSize();
    if (currentSize == 0) {
        logDebug << "No files to check for elimination, current size is 0";
        return;
    }
    uint64_t upperBound = (mOpt.MaxStorageSize * mOpt.LRUUpperBoundPercent) / 100;

    if (currentSize > upperBound) {
        uint64_t targetSize = (mOpt.MaxStorageSize * mOpt.LRUTargetPercent) / 100;
        logDebug << "Storage size (" << currentSize << ") exceeds upper bound (" << upperBound
                 << "), starting LRU elimination to target size: " << targetSize;

        removeLRUFiles(currentSize, targetSize);
    }
}

void FileManager::removeLRUFiles(uint64_t currentSize, uint64_t targetSize)
{
    std::vector<uint64_t> filesToRemove;
    uint64_t sizeToFree = 0;

    {
        std::lock_guard<std::mutex> lock(mLRUMutex);

        // Start from tail of LRU list (oldest files), select files to delete
        auto it = mLRUList.rbegin();
        while (it != mLRUList.rend() && currentSize > targetSize) {
            uint64_t fileId = *it;

            // Get file information
            auto cacheIt = mLRUCache.find(fileId);
            if (cacheIt != mLRUCache.end()) {
                filesToRemove.push_back(fileId);

                // Use cached file size from memory
                uint64_t fileSize = cacheIt->second.fileSize;
                if (fileSize > 0) {
                    sizeToFree += fileSize;
                    currentSize = (currentSize > fileSize) ? currentSize - fileSize : 0;
                } else {
                    // If no size info in memory, get blockEnd - blockStart from database
                    try {
                        auto db = getDB();
                        if (db) {
                            std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
                            auto result = db->stor.select(
                                columns(&FileItem::blockEnd, &FileItem::blockStart),
                                where(c(&FileItem::id) == fileId),
                                limit(1));

                            if (!result.empty()) {
                                uint64_t blockEnd = std::get<0>(result[0]);
                                uint64_t blockStart = std::get<1>(result[0]);
                                uint64_t block_size = blockEnd - blockStart;
                                sizeToFree += block_size;
                                currentSize = (currentSize > block_size) ? currentSize - block_size : 0;

                                // Update cached file size
                                cacheIt->second.fileSize = block_size;
                            }
                        }
                    } catch (...) {
                        logWarn << "Failed to query file size for ID " << fileId << ", using estimated size";
                        // If database query fails, use estimated value
                        uint64_t estimatedSize = 1024 * 1024; // 1MB estimate
                        sizeToFree += estimatedSize;
                        currentSize = (currentSize > estimatedSize) ? currentSize - estimatedSize : 0;
                    }
                }
            }
            ++it;
        }
    }

    if (filesToRemove.empty()) {
        logDebug << "No files to remove for LRU elimination";
        return;
    }

    logDebug << "Starting LRU elimination: " << filesToRemove.size() << " files selected, estimated " << sizeToFree
             << " bytes to free";

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for LRU elimination";
        return;
    }

    uint64_t actualFreed = 0;
    uint64_t successfulRemovals = 0;

    // Delete selected files
    for (uint64_t fileId : filesToRemove) {
        try {
            std::string filePath;
            uint64_t fileSize = 0;

            // First get file info from memory cache
            {
                std::lock_guard<std::mutex> lock(mLRUMutex);
                auto cacheIt = mLRUCache.find(fileId);
                if (cacheIt != mLRUCache.end()) {
                    filePath = cacheIt->second.filePath;
                    fileSize = cacheIt->second.fileSize;
                }
            }

            // If not in cache, get from database
            if (filePath.empty()) {
                try {
                    auto db = getDB();
                    if (db) {
                        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
                        auto result = db->stor.select(
                            columns(&FileItem::path, &FileItem::blockEnd, &FileItem::blockStart),
                            where(c(&FileItem::id) == fileId),
                            limit(1));

                        if (!result.empty()) {
                            filePath = std::get<0>(result[0]);
                            uint64_t blockEnd = std::get<1>(result[0]);
                            uint64_t blockStart = std::get<2>(result[0]);
                            fileSize = blockEnd - blockStart;
                        }
                    }
                } catch (const std::exception& e) {
                    logWarn << "Failed to query file info for ID " << fileId << ": " << e.what();
                    continue;
                }
            }

            if (filePath.empty()) {
                logWarn << "File path not found for ID: " << fileId;
                continue;
            }

            // Lock when deleting physical files to prevent conflicts with scan operations
            {
                std::lock_guard<std::mutex> fileLockFOpt(mFileDBOptMutex);
                if (std::filesystem::exists(filePath)) {
                    std::filesystem::remove(filePath);
                    actualFreed += fileSize;
                    successfulRemovals++;
                    logDebug << "Removed LRU file: " << filePath << " (size: " << fileSize << " bytes)";
                }
            }

            // Delete record from database
            try {
                if (db) {
                    std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
                    db->stor.remove<FileItem>(fileId);
                }
            } catch (const std::exception& e) {
                logWarn << "Failed to delete file record for ID " << fileId << ": " << e.what();
            }

            // Remove from LRU cache and list
            {
                std::lock_guard<std::mutex> lock(mLRUMutex);
                auto cacheIt = mLRUCache.find(fileId);
                if (cacheIt != mLRUCache.end()) {
                    mLRUList.erase(cacheIt->second.listIter);
                    mLRUCache.erase(cacheIt);
                }
            }

        } catch (const std::exception& e) {
            logWarn << "Failed to remove file ID " << fileId << ": " << e.what();
        }
    }

    logDebug << "LRU elimination completed: " << successfulRemovals << "/" << filesToRemove.size() << " files removed, "
             << actualFreed << " bytes freed";
}

void FileManager::reportHaveFiles(const std::vector<std::tuple<FileItem, std::string>>& files)
{
    logDebug << "Reporting have files, count: " << files.size();
    JsonReportFileInfo report;
    std::unordered_map<std::string, JsonFileInfo> jsonFiles;
    for (const auto& file : files) {
        const auto& item = std::get<0>(file);
        const auto& url = std::get<1>(file);
        if (item.blockHash.empty() || item.fileHash.empty()) {
            logWarn << "File item has empty block or file hash, skipping report";
            continue;
        }
        if (item.blockStart > item.blockEnd) {
            logWarn << "Invalid block range for file item, skipping report";
            continue;
        }
        auto it = jsonFiles.find(item.fileHash);
        if (it != jsonFiles.end()) {
            // If file already exists, update block information
            it->second.addBlock(item.blockStart, item.blockEnd, item.blockHash);
            if (item.fileHash == item.blockHash) {
                it->second.size = item.blockEnd - item.blockStart;
            }
        } else {
            // Create new file information
            JsonFileInfo fileInfo(item.fileHash, url, 0);
            fileInfo.addBlock(item.blockStart, item.blockEnd, item.blockHash);
            if (item.fileHash == item.blockHash) {
                fileInfo.size = item.blockEnd - item.blockStart;
            }
            jsonFiles[item.fileHash] = fileInfo;
        }
    }

    for (const auto& pair : jsonFiles) {
        report.addFile(pair.second);
    }
    MainManager::json msg;
    msg["changes"] = report.to_json();
    auto succCallBack = [](util::HttpResponse& resp) {
        auto body = resp.Body();
        logDebug << "Have file report success with response: " << body;
    };
    auto failCallBack = [](int code) { logDebug << "Have file report failed with code: " << code; };
    logDebug << "Sending report with changes: " << msg.dump();
    mMan->AsyncApiPostWithToken(nullptr, "/api/v1/update_file_info", msg, this, succCallBack, failCallBack);
}

void FileManager::reportRemoveFile(const FileItem& item)
{
    logDebug << "Reporting remove file for block hash: " << item.blockHash << ", file hash: " << item.fileHash;
    JsonReportFileInfo report;
    JsonFileInfo fileInfo(item.fileHash, "", 0);
    fileInfo.addBlock(item.blockStart, item.blockEnd, item.blockHash);
    if (item.fileHash == item.blockHash) {
        fileInfo.size = item.blockEnd - item.blockStart;
    }
    report.delFile(fileInfo);
    std::string resp;
    MainManager::json msg;
    msg["changes"] = report.to_json();
    auto succCallBack = [](util::HttpResponse& resp) {
        auto body = resp.Body();
        logDebug << "Remove file report success with response: " << body;
    };
    auto failCallBack = [](int code) { logDebug << "Remove file report failed with code: " << code; };
    logDebug << "Sending remove file report with changes: " << msg.dump();
    mMan->AsyncApiPostWithToken(nullptr, "/api/v1/update_file_info", msg, this, succCallBack, failCallBack);
}

void FileManager::handleDownloadFileDone(std::shared_ptr<Event> evt)
{
    auto argEvt = std::static_pointer_cast<ArgEvent<FileDownloadDoneArg>>(evt);
    auto arg = argEvt.get()->Arg();
    logDebug << "Handling download file done for block hash: " << arg.BlockInfo.Hash << ", file path: " << arg.FilePath;
    if (arg.BlockInfo.Hash.empty()) {
        logWarn << "Block hash is empty, cannot handle download file done";
        return;
    }

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for removing download file";
        return;
    }

    {
        // try move file from tmp to file dir
        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
        std::string tmpFilePath = arg.FilePath;
        // handle empty path or file not exists
        if (tmpFilePath.empty() || !std::filesystem::exists(tmpFilePath)) {
            logWarn << "Download file path is empty";
            // delete from db
            try {
                db->stor.remove_all<FileItem>(
                    where(c(&FileItem::path) == tmpFilePath and c(&FileItem::status) == FileStatus::DOWNLOADING));
            } catch (const std::exception& e) {
                logWarn << "Failed to remove download file record: " << e.what();
            } catch (...) {
                logWarn << "Unknown exception occurred while removing download file record";
            }
            return;
        }

        // target filename is blockHash
        auto targetRelativePath = std::filesystem::path(arg.BlockInfo.Hash);
        std::filesystem::path targetPath(fileDir());
        targetPath.append(targetRelativePath.string());

        if (std::filesystem::exists(targetPath)) {
            logWarn << "File already exists at target path: " << targetPath;
            std::filesystem::remove(tmpFilePath); // Clean up temp file
            // delete from db
            try {
                db->stor.remove_all<FileItem>(
                    where(c(&FileItem::path) == tmpFilePath and c(&FileItem::status) == FileStatus::DOWNLOADING));
            } catch (const std::exception& e) {
                logWarn << "Failed to remove download file record: " << e.what();
            } catch (...) {
                logWarn << "Unknown exception occurred while removing download file record";
            }
            return;
        }

        // move file to target path
        try {
            std::filesystem::create_directories(targetPath.parent_path());
            std::filesystem::rename(tmpFilePath, targetPath);
            logDebug << "Moved downloaded file to: " << targetPath;
        } catch (const std::exception& e) {
            logWarn << "Failed to move downloaded file: " << e.what();
            return;
        }

        try {
            logDebug << "File download completed, updating database record for block hash: " << arg.BlockInfo.Hash
                     << ", file hash: " << arg.FileHash << ", block start: " << arg.BlockInfo.Start
                     << ", block end: " << arg.BlockInfo.End;
            // Select and update records based on download path and status=Downloading
            db->stor.update_all(
                set(c(&FileItem::status) = FileStatus::AVAILABLE,
                    c(&FileItem::blockStart) = arg.BlockInfo.Start,
                    c(&FileItem::blockEnd) = arg.BlockInfo.End,
                    c(&FileItem::fileHash) = arg.FileHash,
                    c(&FileItem::blockHash) = arg.BlockInfo.Hash,
                    c(&FileItem::lastAccess) = getCurrentTimestamp(),
                    c(&FileItem::lastReport) = getCurrentTimestamp(),
                    c(&FileItem::createdAt) = getCurrentTimestamp(),
                    c(&FileItem::path) = targetPath.string()),
                where(c(&FileItem::path) == tmpFilePath and c(&FileItem::status) == FileStatus::DOWNLOADING));

            // Get the fileId of the updated record
            auto updatedFiles = db->stor.select(
                &FileItem::id,
                where(c(&FileItem::blockHash) == arg.BlockInfo.Hash and c(&FileItem::status) == FileStatus::AVAILABLE),
                limit(1));

            if (!updatedFiles.empty()) {
                uint64_t updatedFileId = updatedFiles[0];
                recordFileAccess(updatedFileId, targetPath.string(), arg.BlockInfo.End - arg.BlockInfo.Start);
            }
        } catch (const std::exception& e) {
            logWarn << "Failed to update download file record: " << e.what();
            return;
        } catch (...) {
            logWarn << "Unknown exception occurred while updating download file record";
            return;
        }
    }

    // Report file download completion
    try {
        FileItem item;
        item.blockHash = arg.BlockInfo.Hash;
        item.fileHash = arg.FileHash;
        item.blockStart = arg.BlockInfo.Start;
        item.blockEnd = arg.BlockInfo.End;
        reportHaveFiles(std::vector<std::tuple<FileItem, std::string>>{std::make_tuple(item, arg.Url)});
    } catch (const std::exception& e) {
        logWarn << "Failed to report file download completion: " << e.what();
        return;
    } catch (...) {
        logWarn << "Unknown exception occurred while reporting file download";
        return;
    }
    logDebug << "File download completed";
}

void FileManager::handleDownloadFileFailed(std::shared_ptr<Event> evt)
{
    auto e = static_cast<ArgEvent<FileDownloadFailedArg>*>(evt.get());
    if (!e || e->Arg().FilePath.empty()) {
        logWarn << "Invalid download file failed event";
        return;
    }
    std::string tmpPathStr = e->Arg().FilePath;
    std::filesystem::path tmpFilePath(tmpPathStr);
    {
        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
        // Delete failed download file

        if (std::filesystem::exists(tmpFilePath)) {
            std::filesystem::remove(tmpFilePath);
            logDebug << "Removed failed download file: " << tmpFilePath;
        } else {
            logWarn << "File not found for removal: " << tmpFilePath;
        }
        // Delete database record
        auto db = getDB();
        if (!db) {
            logWarn << "Failed to get database connection for removing failed download";
            return;
        }
        try {
            db->stor.remove<FileItem>(
                where(c(&FileItem::path) == tmpPathStr and c(&FileItem::status) == FileStatus::DOWNLOADING));
        } catch (const std::exception& e) {
            logWarn << "Failed to remove download file record: " << e.what();
            return;
        } catch (...) {
            logWarn << "Unknown exception occurred while removing download file record";
            return;
        }
    }

    logWarn << "File download failed";
}

void FileManager::handleRemoveFile(std::shared_ptr<Event> evt)
{
    auto e = static_cast<ArgEvent<RemoveFileArg>*>(evt.get());
    if (!e || !e->Arg().BlockHash.empty()) {
        logWarn << "Invalid remove file event";
        return;
    }
    std::string blockHash = e->Arg().BlockHash;
    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for removing file";
        return;
    }
    try {
        // Query file records to delete
        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
        auto files = db->stor.select(
            columns(&FileItem::id, &FileItem::fileHash, &FileItem::blockHash),
            where(c(&FileItem::blockHash) == blockHash and c(&FileItem::status) == FileStatus::AVAILABLE));

        if (files.empty()) {
            logWarn << "No files found for block hash: " << blockHash;
            return;
        }

        for (const auto& file : files) {
            FileItem item;
            item.id = std::get<0>(file);
            item.fileHash = std::get<1>(file);
            item.blockHash = std::get<2>(file);

            {
                // Delete file
                std::filesystem::path filePath(mOpt.RootPath);
                filePath.append(item.fileHash);
                if (std::filesystem::exists(filePath)) {
                    std::filesystem::remove(filePath);
                    logDebug << "Removed file: " << filePath;
                } else {
                    logWarn << "File not found for removal: " << filePath;
                }
                // Delete record from database
                db->stor.remove<FileItem>(item.id);
            }

            // Report removed file
            try {
                reportRemoveFile(item);
            } catch (const std::exception& e) {
                logWarn << "Failed to report file removal: " << e.what();
            } catch (...) {
                logWarn << "Unknown exception occurred while reporting file removal";
            }

            // Remove from LRU cache and list
            {
                std::lock_guard<std::mutex> lock(mLRUMutex);
                auto it = mLRUCache.find(item.id);
                if (it != mLRUCache.end()) {
                    mLRUList.erase(it->second.listIter);
                    mLRUCache.erase(it);
                }
            }
        }

    } catch (const std::exception& e) {
        logWarn << "Failed to remove file: " << e.what();
    } catch (...) {
        logWarn << "Unknown exception occurred while removing file";
    }
    // Send log message
    logDebug << "File remove request received";
}

void FileManager::initializeTasks()
{
    // Clear any existing tasks
    mScheduledTasks.clear();

    // Initialize Flush Access Records Task
    mScheduledTasks.emplace_back(
        "FlushAccessRecords", std::chrono::seconds(mOpt.AccessRecordFlushInterval), [this]() { FlushAccessRecords(); });

    // Initialize LRU Check Task
    mScheduledTasks.emplace_back(
        "LRUCheck", std::chrono::seconds(mOpt.LRUCheckInterval), [this]() { CheckAndEliminateFiles(); });

    // Initialize File Report Task
    mScheduledTasks.emplace_back(
        "FileReport", std::chrono::seconds(mOpt.ReportInterval), [this]() { executeReportTask(); });

    // Initialize Scan Cleanup Task
    mScheduledTasks.emplace_back("ScanCleanup", std::chrono::seconds(mOpt.ScanInterval), [this]() {
        scanAndCleanInconsistentFiles();
        cleanStaleDownloads();
    });

    logDebug << "Initialized " << mScheduledTasks.size() << " scheduled tasks";
}

std::chrono::steady_clock::time_point FileManager::getNextExecutionTime(const ScheduledTask& task) const
{
    return task.lastExecution + task.interval;
}

void FileManager::executeTask(const ScheduledTask& task)
{
    try {
        logDebug << "Executing task: " << task.name;
        task.execute();
    } catch (const std::exception& e) {
        logWarn << "Task " << task.name << " failed with exception: " << e.what();
    } catch (...) {
        logWarn << "Task " << task.name << " failed with unknown exception";
    }
}

void FileManager::runTaskWorkerThread()
{
    logDebug << "Unified task worker thread started";

    while (!mShouldStop) {
        auto currentTime = std::chrono::steady_clock::now();
        // Find the next task to execute and its execution time
        auto nextTaskTime = std::chrono::steady_clock::time_point::max();
        std::vector<size_t> readyTaskIndices; // Track which tasks are ready

        // Check all tasks for readiness and find the next execution time
        for (size_t i = 0; i < mScheduledTasks.size(); ++i) {
            auto& task = mScheduledTasks[i];
            auto taskNextTime = getNextExecutionTime(task);

            if (currentTime >= taskNextTime) {
                // Task is ready to execute
                readyTaskIndices.push_back(i);
            } else {
                // Update next task time
                nextTaskTime = std::min(nextTaskTime, taskNextTime);
            }
        }

        // Execute all ready tasks and update their last execution time
        for (size_t index : readyTaskIndices) {
            auto& task = mScheduledTasks[index];
            executeTask(task);
            task.lastExecution = std::chrono::steady_clock::now();
        }

        if (readyTaskIndices.empty()) {
            // Calculate wait time until next task or use default wait
            std::chrono::milliseconds waitTime(1000); // Default wait time

            if (nextTaskTime != std::chrono::steady_clock::time_point::max()) {
                auto calculatedWait = std::chrono::duration_cast<std::chrono::milliseconds>(nextTaskTime - currentTime);
                waitTime = std::min(calculatedWait, std::chrono::milliseconds(1000));
            }

            // Wait for the calculated time or stop signal
            std::unique_lock<std::mutex> lock(mTaskConditionMutex);
            if (mTaskCondition.wait_for(lock, waitTime, [this] { return mShouldStop.load(); })) {
                // Awakened by stop signal
                break;
            }
        }
    }

    // Execute final cleanup tasks before thread stops
    try {
        // Execute flush one final time to ensure no data is lost
        for (const auto& task : mScheduledTasks) {
            if (task.name == "FlushAccessRecords") {
                executeTask(task);
                break;
            }
        }
        logDebug << "Final cleanup completed before thread exit";
    } catch (const std::exception& e) {
        logWarn << "Final cleanup failed: " << e.what();
    } catch (...) {
        logWarn << "Final cleanup unknown exception";
    }

    logDebug << "Unified task worker thread stopped";
}

void FileManager::executeReportTask()
{
    // Query files that haven't been reported for the longest time from database
    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for reporting files";
        return;
    }

    // Query files that haven't been reported for the longest time, sorted by lastReport in ascending order
    // If lastReport is empty or 0, prioritize reporting
    std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
    auto files = db->stor.select(
        columns(
            &FileItem::id,
            &FileItem::fileHash,
            &FileItem::blockHash,
            &FileItem::blockStart,
            &FileItem::blockEnd,
            &FileItem::lastAccess),
        where(c(&FileItem::status) == FileStatus::AVAILABLE),
        order_by(&FileItem::lastReport).asc(),
        limit(mOpt.ReportBatchSize));

    if (files.empty()) {
        logDebug << "No files to report";
        return;
    }

    logDebug << "Reporting " << files.size() << " files to PCDN server";
    uint64_t successfulReports = 0;

    // Batch report files
    std::vector<std::tuple<FileItem, std::string>> filesToReport;
    for (const auto& file : files) {
        FileItem item;
        item.id = std::get<0>(file);
        item.fileHash = std::get<1>(file);
        item.blockHash = std::get<2>(file);
        item.blockStart = std::get<3>(file);
        item.blockEnd = std::get<4>(file);
        filesToReport.push_back(std::tuple(item, ""));
    }
    try {
        reportHaveFiles(filesToReport);
    } catch (const std::exception& e) {
        logWarn << "Failed to report files: " << e.what();
        return; // Report failed, skip this iteration
    } catch (...) {
        logWarn << "Unknown exception occurred during file reporting";
        return; // Report failed, skip this iteration
    }

    // Update report time in database
    uint64_t currentTime = getCurrentTimestamp();
    std::vector<uint64_t> fileIds;
    for (const auto& file : files) {
        fileIds.push_back(std::get<0>(file));
    }
    if (!fileIds.empty()) {
        try {
            db->stor.update_all(set(c(&FileItem::lastReport) = currentTime), where(in(&FileItem::id, fileIds)));
            successfulReports += fileIds.size();
        } catch (const std::exception& e) {
            logWarn << "Failed to batch update last report time: " << e.what();
        }
    }
}

void FileManager::scanFilesystemAndDatabase(
    std::vector<std::string>& filesystemFiles,
    std::unordered_map<std::string, std::tuple<uint64_t, std::string, uint64_t, uint64_t>>& dbFilesMap)
{
    if (!std::filesystem::exists(fileDir())) {
        logWarn << "Files path does not exist: " << fileDir();
        return;
    }

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for filesystem and "
                   "database scan";
        return;
    }

    // Lock for consistency scanning to prevent race conditions with file create/delete operations
    std::lock_guard<std::mutex> fileLockFOpt(mFileDBOptMutex);

    logDebug << "Acquiring file operation lock for filesystem and database scan";

    // 1. Scan file system and collect all file paths
    std::filesystem::recursive_directory_iterator dirIter(fileDir());
    for (const auto& entry : dirIter) {
        if (mShouldStop) {
            logDebug << "Filesystem scan interrupted by stop signal";
            return;
        }

        // Only process regular files, skip directories and special files
        if (!entry.is_regular_file()) {
            continue;
        }

        try {
            filesystemFiles.push_back(entry.path().string());
        } catch (const std::exception& e) {
            logWarn << "Error processing file path " << entry.path() << ": " << e.what();
            continue; // Skip files that can't be processed
        }
    }

    // 2. Query database and get all AVAILABLE status file information
    try {
        auto dbFiles = db->stor.select(
            columns(&FileItem::id, &FileItem::path, &FileItem::fileHash, &FileItem::blockStart, &FileItem::blockEnd),
            where(c(&FileItem::status) == FileStatus::AVAILABLE));

        for (const auto& file : dbFiles) {
            uint64_t id = std::get<0>(file);
            std::string path = std::get<1>(file);
            std::string hash = std::get<2>(file);
            uint64_t blockStart = std::get<3>(file);
            uint64_t blockEnd = std::get<4>(file);

            dbFilesMap[path] = std::make_tuple(id, hash, blockStart, blockEnd);
        }

        logDebug << "Filesystem and database scan completed: found " << filesystemFiles.size()
                 << " files in filesystem, " << dbFilesMap.size() << " files in database";

    } catch (const std::exception& e) {
        logWarn << "Failed to query database during scan: " << e.what();
        throw; // Re-throw exception for caller to handle
    }
}

uint64_t FileManager::cleanOrphanFiles(const std::vector<std::string>& orphanFiles)
{
    if (orphanFiles.empty()) {
        return 0;
    }

    logDebug << "Cleaning " << orphanFiles.size() << " orphan files";

    uint64_t orphanDeletedCount = 0;

    for (const auto& orphanFile : orphanFiles) {
        if (mShouldStop) {
            logDebug << "Orphan file cleanup interrupted by stop signal";
            break;
        }

        try {
            std::filesystem::path orphanPath(orphanFile);
            // Check again if file exists (may have been deleted by other processes)
            if (!std::filesystem::exists(orphanPath)) {
                logDebug << "Orphan file no longer exists, skipping: " << orphanFile;
                continue;
            }
            // Delete orphan file
            std::filesystem::remove(orphanPath);
            orphanDeletedCount++;
            logDebug << "Deleted orphan file: " << orphanFile;
        } catch (const std::filesystem::filesystem_error& e) {
            logWarn << "Failed to delete orphan file " << orphanFile << ": " << e.what();
        } catch (const std::exception& e) {
            logWarn << "Error processing orphan file " << orphanFile << ": " << e.what();
        }
    }

    logDebug << "Orphan file cleanup completed: deleted " << orphanDeletedCount << " files";

    return orphanDeletedCount;
}

uint64_t FileManager::cleanMissingFiles(
    const std::vector<std::tuple<uint64_t, std::string, std::string, uint64_t, uint64_t>>& missingFiles)
{
    if (missingFiles.empty()) {
        return 0;
    }

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for cleaning missing files";
        return 0;
    }

    logDebug << "Cleaning " << missingFiles.size() << " missing file records";

    std::vector<uint64_t> missingFileIds;
    std::vector<FileItem> missingFilesForReport;

    // Handle LRU cache cleanup
    for (const auto& missingFile : missingFiles) {
        uint64_t fileId = std::get<0>(missingFile);
        std::string missingFilePath = std::get<1>(missingFile);
        std::string fileHash = std::get<2>(missingFile);
        uint64_t blockStart = std::get<3>(missingFile);
        uint64_t blockEnd = std::get<4>(missingFile);

        missingFileIds.push_back(fileId);

        // Prepare file information for reporting
        FileItem missingItem;
        missingItem.id = fileId;
        missingItem.path = missingFilePath;
        missingItem.fileHash = fileHash;
        missingItem.blockStart = blockStart;
        missingItem.blockEnd = blockEnd;
        missingFilesForReport.push_back(missingItem);

        logDebug << "Found missing file in database: " << missingFilePath << " (ID: " << fileId << ")";

        // Remove this file from LRU cache (if exists)
        {
            std::lock_guard<std::mutex> lock(mLRUMutex);
            auto cacheIt = mLRUCache.find(fileId);
            if (cacheIt != mLRUCache.end()) {
                mLRUList.erase(cacheIt->second.listIter);
                mLRUCache.erase(cacheIt);
                logDebug << "Removed missing file from LRU cache: " << fileId;
            }
        }
    }

    // Batch delete missing file records from database
    uint64_t deletedCount = 0;
    if (!missingFileIds.empty()) {
        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
        try {
            // Use sqlite_orm batch delete: WHERE id IN (?, ?, ?, ...)
            // This only requires one database operation to delete all records
            db->stor.remove_all<FileItem>(where(in(&FileItem::id, missingFileIds)));

            // Since remove_all doesn't return delete count, we assume all deletions succeeded
            // If precise counting is needed, query the number of matching records first
            deletedCount = missingFileIds.size();

            logDebug << "Successfully removed " << deletedCount
                     << " missing file records from database (batch operation)";

        } catch (const std::exception& e) {
            logWarn << "Batch delete failed, falling back to individual deletes: " << e.what();

            // If batch delete fails, fall back to individual deletions
            for (const auto& fileId : missingFileIds) {
                try {
                    db->stor.remove<FileItem>(fileId);
                    deletedCount++;
                } catch (const std::exception& e) {
                    logWarn << "Failed to delete missing file record ID " << fileId << ": " << e.what();
                }
            }

            logDebug << "Successfully removed " << deletedCount
                     << " missing file records from database (individual operations)";
        }
    }

    // Report deleted files (if report URL configured)
    logDebug << "Reporting " << missingFilesForReport.size() << " removed files to PCDN server";
    uint64_t reportedCount = 0;
    for (const auto& missingFile : missingFilesForReport) {
        try {
            reportRemoveFile(missingFile);
            reportedCount++;
        } catch (const std::exception& e) {
            logWarn << "Failed to report removed file " << missingFile.path << ": " << e.what();
        }
    }

    logDebug << "Successfully reported " << reportedCount << "/" << missingFilesForReport.size() << " removed files";

    return deletedCount;
}

void FileManager::scanAndCleanInconsistentFiles()
{
    logDebug << "Starting unified file system and database consistency check";

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for consistency check";
        return;
    }

    try {
        // Data structures for storing scan and check results
        std::vector<std::string> filesystemFiles; // File list in filesystem
        std::unordered_map<std::string, std::tuple<uint64_t, std::string, uint64_t, uint64_t>>
            dbFilesMap; // Database file mapping: path -> (id, hash, blockStart,
                        // blockEnd)

        // Use helper method to scan filesystem and database
        scanFilesystemAndDatabase(filesystemFiles, dbFilesMap);

        // Analyze and clean inconsistent files
        std::vector<std::string> orphanFiles; // Orphan files in filesystem but not in database
        std::vector<std::tuple<uint64_t, std::string, std::string, uint64_t, uint64_t>>
            missingFiles; // Files in database but not in filesystem

        // Find orphan files
        for (const auto& fsFile : filesystemFiles) {
            if (dbFilesMap.find(fsFile) == dbFilesMap.end()) {
                orphanFiles.push_back(fsFile);
            }
        }

        // Find missing files
        for (const auto& [dbPath, fileInfo] : dbFilesMap) {
            bool foundInFilesystem = false;
            for (const auto& fsFile : filesystemFiles) {
                if (fsFile == dbPath) {
                    foundInFilesystem = true;
                    break;
                }
            }

            if (!foundInFilesystem) {
                uint64_t id = std::get<0>(fileInfo);
                std::string hash = std::get<1>(fileInfo);
                uint64_t blockStart = std::get<2>(fileInfo);
                uint64_t blockEnd = std::get<3>(fileInfo);
                missingFiles.push_back(std::make_tuple(id, dbPath, hash, blockStart, blockEnd));
            }
        }

        logDebug << "Found " << orphanFiles.size() << " orphan files and " << missingFiles.size() << " missing files";

        // Handle orphan files (in filesystem but not in database)
        uint64_t orphanDeletedCount = cleanOrphanFiles(orphanFiles);

        // Handle missing files (in database but not in filesystem)
        uint64_t missingDeletedCount = cleanMissingFiles(missingFiles);

        logDebug << "Unified consistency check completed: processed " << filesystemFiles.size() << " filesystem files, "
                 << dbFilesMap.size() << " database files, deleted " << orphanDeletedCount << " orphan files and "
                 << missingDeletedCount << " missing file records";

    } catch (const std::exception& e) {
        logWarn << "Failed to perform unified consistency check: " << e.what();
    }
}

void FileManager::cleanStaleDownloads()
{
    logDebug << "Starting cleanup of stale download files";

    std::filesystem::path tmpDirPath = tmpDir();

    // Check if tmp directory exists
    if (!std::filesystem::exists(tmpDirPath)) {
        logDebug << "Tmp directory does not exist: " << tmpDirPath;
        return;
    }

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for stale download cleanup";
        return;
    }

    try {
        uint64_t currentTime = getCurrentTimestamp();
        uint64_t timeoutSeconds = mOpt.DownloadTimeout;

        // Recursively scan tmp directory
        uint64_t scannedCount = 0;
        uint64_t staleCount = 0;
        uint64_t deletedCount = 0;
        uint64_t totalSizeFreed = 0;
        std::vector<std::string> deletedFilesForDB;

        std::filesystem::recursive_directory_iterator dirIter(tmpDirPath);

        for (const auto& entry : dirIter) {
            if (mShouldStop) {
                logDebug << "Stale download cleanup interrupted by stop signal";
                break;
            }

            // Only process regular files, skip directories and special files
            if (!entry.is_regular_file()) {
                continue;
            }

            scannedCount++;

            try {
                auto durSinceLastWrite = durSinceFileLastUpdateTime(entry.path());
                // Check if file has timed out
                if (durSinceLastWrite > timeoutSeconds) {
                    staleCount++;

                    try {
                        // Get file size
                        uint64_t fileSize = std::filesystem::file_size(entry.path());
                        std::string filePathStr = entry.path().string();
                        std::filesystem::remove(entry.path());
                        deletedCount++;
                        totalSizeFreed += fileSize;

                        // Record files that need to be deleted from database
                        deletedFilesForDB.push_back(filePathStr);

                        uint64_t stale_duration = durSinceLastWrite;
                        logDebug << "Deleted stale download file: " << filePathStr << " (size: " << fileSize
                                 << " bytes, stale for: " << stale_duration << " seconds)";

                    } catch (const std::filesystem::filesystem_error& e) {
                        logWarn << "Failed to delete stale download file " << entry.path() << ": " << e.what();
                    }
                }

                // Output progress every 1000 scanned files
                if (scannedCount % 1000 == 0) {
                    logDebug << "Scanned " << scannedCount << " download files, found " << staleCount << " stale files";
                }

            } catch (const std::exception& e) {
                logWarn << "Error processing download file " << entry.path() << ": " << e.what();
            }
        }

        // Delete corresponding download records from database
        if (!deletedFilesForDB.empty()) {
            try {
                // Find and delete records with DOWNLOADING status and matching paths from database
                uint64_t dbDeletedCount = 0;

                for (const auto& filePath : deletedFilesForDB) {
                    try {
                        std::lock_guard<std::mutex> lockFOpt(mFileDBOptMutex);
                        // Query matching download records
                        auto downloadRecords = db->stor.select(
                            &FileItem::id,
                            where(c(&FileItem::status) == FileStatus::DOWNLOADING and c(&FileItem::path) == filePath));

                        // Delete found records
                        for (const auto& recordId : downloadRecords) {
                            db->stor.remove<FileItem>(recordId);
                            dbDeletedCount++;

                            logDebug << "Removed stale download record from database: " << filePath
                                     << " (ID: " << recordId << ")";
                        }

                    } catch (const std::exception& e) {
                        logWarn << "Failed to remove download record for " << filePath << ": " << e.what();
                    }
                }

                if (dbDeletedCount > 0) {
                    logDebug << "Removed " << dbDeletedCount << " stale download records from database";
                }

            } catch (const std::exception& e) {
                logWarn << "Failed to clean stale download records from database: " << e.what();
            }
        }

        logDebug << "Stale download cleanup completed: scanned " << scannedCount << " files, found " << staleCount
                 << " stale files, "
                 << "deleted " << deletedCount << " files, "
                 << "freed " << totalSizeFreed << " bytes";

    } catch (const std::exception& e) {
        logWarn << "Failed to clean stale download files: " << e.what();
    }
}

NS_END
