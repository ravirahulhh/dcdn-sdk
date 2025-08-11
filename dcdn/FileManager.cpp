#include "FileManager.h"

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
    dcdn::FileStatus extract(const char* row_value)
    {
        return static_cast<dcdn::FileStatus>(std::atoi(row_value));
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
    auto storage = make_storage(
        filename,
        make_unique_index(
            "idx_unique", &FileItem::file_hash, &FileItem::file_hash, &FileItem::block_start, &FileItem::block_end),
        make_index("idx_file_start", &FileItem::file_hash, &FileItem::block_start),
        make_index("idx_last_report", &FileItem::last_report),
        make_table(
            "files",
            make_column("id", &FileItem::id, primary_key().autoincrement()),
            make_column("block_hash", &FileItem::block_hash),
            make_column("file_hash", &FileItem::file_hash),
            make_column("file_path", &FileItem::path),
            make_column("status", &FileItem::status),
            make_column("block_start", &FileItem::block_start),
            make_column("block_end", &FileItem::block_end),
            make_column("last_access", &FileItem::last_access),
            make_column("last_report", &FileItem::last_report),
            make_column("created_at", &FileItem::created_at, default_value("CURRENT_TIMESTAMP"))));
    return storage;
}

class StorageRef: public StorageRefImpl<createFileStorage>
{
public:
    using Base::Base;
};

FileManager::FileManager(MainManager* man, const FileManagerOption& opt): BaseManager(man), mOpt(opt)
{
    mLastFlushTime = std::chrono::steady_clock::now();
    mLastLRUCheckTime = std::chrono::steady_clock::now();

    if (man->Option().WorkDir.empty()) {
        throw std::runtime_error("Work directory is not set in MainManager");
    }

    if (opt.RootPath.empty()) {
        throw std::runtime_error("Root path is not set in FileManagerOption");
    }

    // check if database exists
    if (!std::filesystem::exists(dbPath())) {
        if (createTable() != ErrorCodeOk) {
            logError << "Failed to create files.db table";
            throw std::runtime_error("Failed to create files.db table");
        }
    }

    if (auto db = getDB()) {
        try {
            db->stor.sync_schema();
        } catch (...) {
            logError << "Failed to sync schema for files.db";
            throw std::runtime_error("Failed to sync schema for files.db");
        }
    }

    // Load file access records from database to LRU cache
    loadAccessRecordsFromDB();

    registerHandler(EventType::FileDownloadDone, &FileManager::handleDownloadFileDone);
    registerHandler(EventType::FileDownloadFailed, &FileManager::handleDownloadFileFailed);
    registerHandler(EventType::RemoveFile, &FileManager::handleRemoveFile);

    // Start LRU thread
    mLRUThread = std::thread(&FileManager::runLRUThread, this);

    // Start flush thread
    mFlushThread = std::thread(&FileManager::runFlushThread, this);

    // Start report thread
    mReportThread = std::thread(&FileManager::runReportThread, this);

    // Start scan cleanup thread
    mScanThread = std::thread(&FileManager::runScanThread, this);
}

FileManager::~FileManager()
{
    // Stop all threads
    mShouldStop = true;
    mLRUCondition.notify_all();
    mFlushCondition.notify_all();
    mReportCondition.notify_all();
    mScanCondition.notify_all();

    if (mLRUThread.joinable()) {
        mLRUThread.join();
    }

    if (mFlushThread.joinable()) {
        mFlushThread.join();
    }

    if (mReportThread.joinable()) {
        mReportThread.join();
    }

    if (mScanThread.joinable()) {
        mScanThread.join();
    }
}

void FileManager::run()
{
    logInfo << "FileManager running";
    // check root dir exists
    if (!std::filesystem::exists(mOpt.RootPath)) {
        std::filesystem::create_directories(mOpt.RootPath);
        logInfo << "Created root directory: " << mOpt.RootPath;
    }

    if (!std::filesystem::exists(tmpDir())) {
        std::filesystem::create_directories(tmpDir());
        logInfo << "Created tmp directory: " << tmpDir();
    }

    if (!std::filesystem::exists(fileDir())) {
        std::filesystem::create_directories(fileDir());
        logInfo << "Created file directory: " << fileDir();
    }

    while (true) {
        // Main loop only handles events, flush tasks are handled by separate threads
        waitAllEvents(std::chrono::milliseconds(1000));
    }
    logInfo << "FileManager exit";
}

std::shared_ptr<StorageRef> FileManager::getDB()
{
    if (!mDB) {
        try {
            std::filesystem::path dbFile(mMan->Option().WorkDir);
            dbFile.append("files.db");
            mDB = std::make_shared<StorageRef>(dbFile);
        } catch (std::exception& excp) {
            logWarn << "create config.db exception: " << excp.what();
        } catch (...) {
            logWarn << "create config.db unknown exception";
        }
    };
    return mDB;
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

std::string FileManager::GetPathByBlockHash(const std::string& block_hash, bool need_report)
{
    auto db = getDB();
    if (!db) {
        return "";
    }

    try {
        // Use ORM query
        auto files = db->stor.select(
            columns(&FileItem::id, &FileItem::path, &FileItem::block_end, &FileItem::block_start),
            where(c(&FileItem::block_hash) == block_hash and c(&FileItem::status) == FileStatus::AVAILABLE),
            limit(1));

        if (!files.empty()) {
            auto& file = files[0];
            uint64_t file_id = std::get<0>(file);
            std::string file_path = std::get<1>(file);
            uint64_t block_end = std::get<2>(file);
            uint64_t block_start = std::get<3>(file);
            uint64_t file_size = block_end - block_start;

            // Record file access, using file ID
            recordFileAccess(file_id, file_path, file_size);
            return file_path;
        }

        return "";
    } catch (const std::exception& e) {
        logWarn << "Failed to get path by block hash: " << e.what();
        return "";
    }
}

std::string FileManager::NewDownloadPath(const FileDescriptor& file, bool create)
{
    // check block start <= end
    if (std::holds_alternative<BlockInfo>(file)) {
        const auto& block = std::get<BlockInfo>(file);
        if (block.block_start > block.block_end) {
            logWarn << "Invalid block range for file: " << block.file_hash << " start:" << block.block_start
                    << " end:" << block.block_end;
            return "";
        }
    }
    auto tmp_path = tmpDir();
    std::string file_name = FileName(file);
    tmp_path.append(file_name);
    if (create) {
        {
            std::lock_guard<std::mutex> lock(mFileOperationMutex);
            if (!std::filesystem::exists(tmp_path)) {
                std::ofstream ofs(tmp_path);
                if (!ofs) {
                    logWarn << "Failed to create new download file: " << tmp_path;
                    return "";
                }
                ofs.close();
            }
            logInfo << "New download file created: " << tmp_path;
            // Write to database
            auto db = getDB();
            if (!db) {
                logWarn << "Failed to get database connection for new download file";
                return "";
            }
            try {
                FileItem item;
                item.block_hash = std::holds_alternative<BlockInfo>(file) ? std::get<BlockInfo>(file).block_hash : "";
                item.file_hash = std::holds_alternative<BlockInfo>(file) ? std::get<BlockInfo>(file).file_hash : "";
                item.path = tmp_path.string();
                item.status = FileStatus::DOWNLOADING;
                item.block_start = std::holds_alternative<BlockInfo>(file) ? std::get<BlockInfo>(file).block_start : 0;
                item.block_end = std::holds_alternative<BlockInfo>(file) ? std::get<BlockInfo>(file).block_end : 0;
                item.last_access = getCurrentTimestamp();
                item.last_report = 0;
                if (item.block_start > item.block_end) {
                    logWarn << "Invalid block range for file: " << file_name;
                    return "";
                }
                db->stor.insert(item);
            } catch (const std::exception& e) {
                logWarn << "Failed to insert new download file into database: " << e.what();
                return "";
            }
        }
    }
    return tmp_path.string();
}

// LRU related method implementations
void FileManager::recordFileAccess(uint64_t file_id, const std::string& file_path, uint64_t file_size)
{
    std::lock_guard<std::mutex> lock(mLRUMutex);

    uint64_t current_time = getCurrentTimestamp();

    auto it = mLRUCache.find(file_id);
    if (it != mLRUCache.end()) {
        // File already exists, update access time and move to head of list
        mLRUList.erase(it->second.list_iter);
        mLRUList.push_front(file_id);
        it->second.list_iter = mLRUList.begin();
        it->second.last_access = current_time;
        it->second.access_count++;
        it->second.is_dirty = true; // Mark as needing database refresh

        // Update file size and path (if changed)
        if (file_size > 0) {
            it->second.file_size = file_size;
        }
        it->second.file_path = file_path;
    } else {
        // New file, add to cache
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

void FileManager::FlushAccessRecords()
{
    std::vector<LRUNode> dirty_nodes;

    {
        std::lock_guard<std::mutex> lock(mLRUMutex);

        // Collect all dirty nodes
        for (auto& pair : mLRUCache) {
            if (pair.second.is_dirty) {
                dirty_nodes.push_back(pair.second);
                pair.second.is_dirty = false; // Clear dirty flag
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
        // Batch update file access times in database
        for (const auto& node : dirty_nodes) {
            db->stor.update_all(
                set(c(&FileItem::last_access) = node.last_access), where(c(&FileItem::id) == node.file_id));
        }

        logInfo << "Flushed " << dirty_nodes.size() << " access records to database";
    } catch (const std::exception& e) {
        logWarn << "Failed to flush access records: " << e.what();

        // If flush fails, re-mark as dirty data
        std::lock_guard<std::mutex> lock(mLRUMutex);
        for (const auto& node : dirty_nodes) {
            auto it = mLRUCache.find(node.file_id);
            if (it != mLRUCache.end()) {
                it->second.is_dirty = true;
            }
        }
    }
}

void FileManager::loadAccessRecordsFromDB()
{
    logInfo << "Loading file access records from database";

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for loading access records";
        return;
    }

    try {
        // Query all file records, sorted by last access time in descending order
        auto files = db->stor.select(
            columns(
                &FileItem::id, &FileItem::path, &FileItem::last_access, &FileItem::block_end, &FileItem::block_start),
            where(c(&FileItem::status) == FileStatus::AVAILABLE),
            order_by(&FileItem::last_access).desc());

        std::lock_guard<std::mutex> lock(mLRUMutex);

        // Clear existing LRU cache and list
        mLRUCache.clear();
        mLRUList.clear();

        uint64_t loaded_count = 0;
        for (const auto& file : files) {
            uint64_t file_id = std::get<0>(file);
            std::string file_path = std::get<1>(file);
            uint64_t last_access = std::get<2>(file);
            uint64_t block_end = std::get<3>(file);
            uint64_t block_start = std::get<4>(file);
            uint64_t file_size = block_end - block_start;

            // Add to LRU list and cache
            mLRUList.push_back(file_id); // Add in descending order by access time, newest first

            LRUNode node;
            node.file_id = file_id;
            node.last_access = last_access;
            node.access_count = 1; // Initialize access count
            node.file_size = file_size;
            node.file_path = file_path;
            node.list_iter = std::prev(mLRUList.end()); // Point to just inserted element
            node.is_dirty = false; // Records loaded from database don't need immediate refresh

            mLRUCache[file_id] = node;
            loaded_count++;
        }

        logInfo << "Loaded " << loaded_count << " file access records from database";

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
        auto files = db->stor.select(columns(&FileItem::block_end, &FileItem::block_start));

        uint64_t total_size = 0;
        for (const auto& file : files) {
            uint64_t block_end = std::get<0>(file);
            uint64_t block_start = std::get<1>(file);
            total_size += (block_end - block_start);
        }

        logDebug << "Calculated total storage size from database: " << total_size << " bytes";
        return total_size;
    } catch (const std::exception& e) {
        logWarn << "Failed to calculate storage size from database: " << e.what();
        return 0;
    }
}

void FileManager::CheckAndEliminateFiles()
{
    uint64_t current_size = calculateTotalStorageSize();
    if (current_size == 0) {
        logInfo << "No files to check for elimination, current size is 0";
        return;
    }
    uint64_t upper_bound = (mOpt.MaxStorageSize * mOpt.LRUUpperBoundPercent) / 100;

    if (current_size > upper_bound) {
        uint64_t target_size = (mOpt.MaxStorageSize * mOpt.LRUTargetPercent) / 100;
        logInfo << "Storage size (" << current_size << ") exceeds upper bound (" << upper_bound
                << "), starting LRU elimination to target size: " << target_size;

        removeLRUFiles(current_size, target_size);
    }
}

void FileManager::removeLRUFiles(uint64_t current_size, uint64_t target_size)
{
    std::vector<uint64_t> files_to_remove;
    uint64_t size_to_free = 0;

    {
        std::lock_guard<std::mutex> lock(mLRUMutex);

        // Start from tail of LRU list (oldest files), select files to delete
        auto it = mLRUList.rbegin();
        while (it != mLRUList.rend() && current_size > target_size) {
            uint64_t file_id = *it;

            // Get file information
            auto cache_it = mLRUCache.find(file_id);
            if (cache_it != mLRUCache.end()) {
                files_to_remove.push_back(file_id);

                // Use cached file size from memory
                uint64_t file_size = cache_it->second.file_size;
                if (file_size > 0) {
                    size_to_free += file_size;
                    current_size = (current_size > file_size) ? current_size - file_size : 0;
                } else {
                    // If no size info in memory, get block_end - block_start from database
                    try {
                        auto db = getDB();
                        if (db) {
                            auto result = db->stor.select(
                                columns(&FileItem::block_end, &FileItem::block_start),
                                where(c(&FileItem::id) == file_id),
                                limit(1));

                            if (!result.empty()) {
                                uint64_t block_end = std::get<0>(result[0]);
                                uint64_t block_start = std::get<1>(result[0]);
                                uint64_t block_size = block_end - block_start;
                                size_to_free += block_size;
                                current_size = (current_size > block_size) ? current_size - block_size : 0;

                                // Update cached file size
                                cache_it->second.file_size = block_size;
                            }
                        }
                    } catch (...) {
                        logWarn << "Failed to query file size for ID " << file_id << ", using estimated size";
                        // If database query fails, use estimated value
                        uint64_t estimated_size = 1024 * 1024; // 1MB estimate
                        size_to_free += estimated_size;
                        current_size = (current_size > estimated_size) ? current_size - estimated_size : 0;
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

    logInfo << "Starting LRU elimination: " << files_to_remove.size() << " files selected, estimated " << size_to_free
            << " bytes to free";

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for LRU elimination";
        return;
    }

    uint64_t actual_freed = 0;
    uint64_t successful_removals = 0;

    // Delete selected files
    for (uint64_t file_id : files_to_remove) {
        try {
            std::string file_path;
            uint64_t file_size = 0;

            // First get file info from memory cache
            {
                std::lock_guard<std::mutex> lock(mLRUMutex);
                auto cache_it = mLRUCache.find(file_id);
                if (cache_it != mLRUCache.end()) {
                    file_path = cache_it->second.file_path;
                    file_size = cache_it->second.file_size;
                }
            }

            // If not in cache, get from database
            if (file_path.empty()) {
                try {
                    auto db = getDB();
                    if (db) {
                        auto result = db->stor.select(
                            columns(&FileItem::path, &FileItem::block_end, &FileItem::block_start),
                            where(c(&FileItem::id) == file_id),
                            limit(1));

                        if (!result.empty()) {
                            file_path = std::get<0>(result[0]);
                            uint64_t block_end = std::get<1>(result[0]);
                            uint64_t block_start = std::get<2>(result[0]);
                            file_size = block_end - block_start;
                        }
                    }
                } catch (const std::exception& e) {
                    logWarn << "Failed to query file info for ID " << file_id << ": " << e.what();
                    continue;
                }
            }

            if (file_path.empty()) {
                logWarn << "File path not found for ID: " << file_id;
                continue;
            }

            // Lock when deleting physical files to prevent conflicts with scan operations
            {
                std::lock_guard<std::mutex> file_lock(mFileOperationMutex);
                if (std::filesystem::exists(file_path)) {
                    std::filesystem::remove(file_path);
                    actual_freed += file_size;
                    successful_removals++;
                    logInfo << "Removed LRU file: " << file_path << " (size: " << file_size << " bytes)";
                }
            }

            // Delete record from database
            try {
                if (db) {
                    db->stor.remove<FileItem>(file_id);
                }
            } catch (const std::exception& e) {
                logWarn << "Failed to delete file record for ID " << file_id << ": " << e.what();
            }

            // Remove from LRU cache and list
            {
                std::lock_guard<std::mutex> lock(mLRUMutex);
                auto cache_it = mLRUCache.find(file_id);
                if (cache_it != mLRUCache.end()) {
                    mLRUList.erase(cache_it->second.list_iter);
                    mLRUCache.erase(cache_it);
                }
            }

        } catch (const std::exception& e) {
            logWarn << "Failed to remove file ID " << file_id << ": " << e.what();
        }
    }

    logInfo << "LRU elimination completed: " << successful_removals << "/" << files_to_remove.size()
            << " files removed, " << actual_freed << " bytes freed";
}

void FileManager::reportHaveFiles(const std::vector<std::tuple<FileItem, std::string>>& files)
{
    if (mOpt.PCDNReportUrl.empty()) {
        return;
    }

    JsonReportFileInfo report;
    std::unordered_map<std::string, JsonFileInfo> json_files;
    for (const auto& file : files) {
        const auto& item = std::get<0>(file);
        const auto& url = std::get<1>(file);
        if (item.block_hash.empty() || item.file_hash.empty()) {
            logWarn << "File item has empty block or file hash, skipping report";
            continue;
        }
        auto it = json_files.find(item.file_hash);
        if (it != json_files.end()) {
            // If file already exists, update block information
            it->second.addBlock(item.block_start, item.block_end, item.block_hash);
        } else {
            // Create new file information
            JsonFileInfo file_info(item.file_hash, url, 0);
            file_info.addBlock(item.block_start, item.block_end, item.block_hash);
            if (item.file_hash == item.block_hash) {
                file_info.size = item.block_end - item.block_start;
            }
            json_files[item.file_hash] = file_info;
        }
    }

    for (const auto& pair : json_files) {
        report.addFile(pair.second);
    }
    std::string resp;
    mClient.Post(mOpt.PCDNReportUrl.c_str(), report.to_json_string(), resp, "application/json");
}

void FileManager::reportRemoveFile(const FileItem& item)
{
    if (mOpt.PCDNReportUrl.empty()) {
        return;
    }
    JsonReportFileInfo report;
    JsonFileInfo file_info(item.file_hash, "", 0);
    file_info.addBlock(item.block_start, item.block_end, item.block_hash);
    if (item.file_hash == item.block_hash) {
        file_info.size = item.block_end - item.block_start;
    }
    report.delFile(file_info);
    std::string resp;
    mClient.Post(mOpt.PCDNReportUrl.c_str(), report.to_json_string(), resp, "application/json");
}

void FileManager::handleDownloadFileDone(std::shared_ptr<Event> evt)
{
    auto e = static_cast<ArgEvent<FileDownloadDoneArg>*>(evt.get());
    if (!e) {
        logWarn << "Invalid download file done event";
        return;
    }
    auto& arg = e->Arg();

    if (arg.block_hash.empty()) {
        logWarn << "Block hash is empty, cannot handle download file done";
        return;
    }

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for removing download file";
        return;
    }

    FileItem item;
    {
        // try move file from tmp to file dir
        std::lock_guard<std::mutex> lock(mFileOperationMutex);
        std::string tmp_file_path = arg.file_path;
        // handle empty path or file not exists
        if (tmp_file_path.empty() || !std::filesystem::exists(tmp_file_path)) {
            logWarn << "Download file path is empty";
            // delete from db
            try {
                db->stor.remove<FileItem>(
                    where(c(&FileItem::path) == tmp_file_path and c(&FileItem::status) == FileStatus::DOWNLOADING));
            } catch (const std::exception& e) {
                logWarn << "Failed to remove download file record: " << e.what();
            } catch (...) {
                logWarn << "Unknown exception occurred while removing download file record";
            }
            return;
        }

        // target filename is block_hash
        auto tmp_relative_path = std::filesystem::relative(tmp_file_path, tmpDir());
        auto target_relative_path = std::filesystem::path(arg.block_hash);
        std::filesystem::path target_path(fileDir());
        target_path.append(target_relative_path.string());

        if (std::filesystem::exists(target_path)) {
            logWarn << "File already exists at target path: " << target_path;
            return;
        }

        // move file to target path
        try {
            std::filesystem::create_directories(target_path.parent_path());
            std::filesystem::rename(tmp_file_path, target_path);
            logInfo << "Moved downloaded file to: " << target_path;
        } catch (const std::exception& e) {
            logWarn << "Failed to move downloaded file: " << e.what();
            return;
        }

        try {
            item.path = target_path.string();
            item.status = FileStatus::AVAILABLE;
            item.block_start = arg.block_start;
            item.block_end = arg.block_end;
            item.file_hash = arg.file_hash;
            item.block_hash = arg.block_hash;
            item.last_access = getCurrentTimestamp();
            item.last_report = item.last_access;

            // Select and update records based on download path and status=Downloading
            db->stor.update_all(
                set(c(&FileItem::status) = item.status,
                    c(&FileItem::block_start) = item.block_start,
                    c(&FileItem::block_end) = item.block_end,
                    c(&FileItem::file_hash) = item.file_hash,
                    c(&FileItem::block_hash) = item.block_hash,
                    c(&FileItem::last_access) = item.last_access,
                    c(&FileItem::last_report) = item.last_report),
                where(c(&FileItem::path) == tmp_file_path and c(&FileItem::status) == FileStatus::DOWNLOADING));

        } catch (const std::exception& e) {
            logWarn << "Failed to update download file record: " << e.what();
            return;
        } catch (...) {
            logWarn << "Unknown exception occurred while updating download file record";
            return;
        }
    }

    // Report file download completion
    if (item.block_hash.empty()) {
        logWarn << "Block hash is empty, cannot report file download completion";
        return;
    }
    try {
        reportHaveFiles(std::vector<std::tuple<FileItem, std::string>>{std::make_tuple(item, arg.url)});
    } catch (const std::exception& e) {
        logWarn << "Failed to report file download completion: " << e.what();
        return;
    } catch (...) {
        logWarn << "Unknown exception occurred while reporting file download";
        return;
    }
    logInfo << "File download completed";
}

void FileManager::handleDownloadFileFailed(std::shared_ptr<Event> evt)
{
    auto e = static_cast<ArgEvent<FileDownloadFailedArg>*>(evt.get());
    if (!e) {
        logWarn << "Invalid download file failed event";
        return;
    }
    auto descriptor = e->Arg().file;
    std::string tmp_path_str = NewDownloadPath(descriptor, false);
    if (tmp_path_str.empty()) {
        logWarn << "Failed to create download path for file";
        return;
    }
    std::filesystem::path tmp_file_path(tmp_path_str);
    {
        std::lock_guard<std::mutex> lock(mFileOperationMutex);
        // Delete failed download file

        if (std::filesystem::exists(tmp_file_path)) {
            std::filesystem::remove(tmp_file_path);
            logInfo << "Removed failed download file: " << tmp_file_path;
        } else {
            logWarn << "File not found for removal: " << tmp_file_path;
        }
        // Delete database record
        auto db = getDB();
        if (!db) {
            logWarn << "Failed to get database connection for removing failed download";
            return;
        }
        try {
            db->stor.remove<FileItem>(
                where(c(&FileItem::path) == tmp_path_str and c(&FileItem::status) == FileStatus::DOWNLOADING));
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
    if (!e || !e->Arg().block_hash.empty()) {
        logWarn << "Invalid remove file event";
        return;
    }
    std::string blockHash = e->Arg().block_hash;
    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for removing file";
        return;
    }
    try {
        // Query file records to delete
        auto files = db->stor.select(
            columns(&FileItem::id, &FileItem::file_hash, &FileItem::block_hash),
            where(c(&FileItem::block_hash) == blockHash and c(&FileItem::status) == FileStatus::AVAILABLE));

        if (files.empty()) {
            logWarn << "No files found for block hash: " << blockHash;
            return;
        }

        for (const auto& file : files) {
            FileItem item;
            item.id = std::get<0>(file);
            item.file_hash = std::get<1>(file);
            item.block_hash = std::get<2>(file);

            {
                std::lock_guard<std::mutex> lock(mFileOperationMutex);
                // Delete file
                std::filesystem::path file_path(mOpt.RootPath);
                file_path.append(item.file_hash);
                if (std::filesystem::exists(file_path)) {
                    std::filesystem::remove(file_path);
                    logInfo << "Removed file: " << file_path;
                } else {
                    logWarn << "File not found for removal: " << file_path;
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
                    mLRUList.erase(it->second.list_iter);
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
    logInfo << "File remove request received";
}

void FileManager::runLRUThread()
{
    logInfo << "LRU thread started";

    while (!mShouldStop) {
        try {
            // Wait for specified interval or be awakened by stop signal
            std::unique_lock<std::mutex> lock(mLRUConditionMutex);
            if (mLRUCondition.wait_for(
                    lock, std::chrono::seconds(mOpt.LRUCheckInterval), [this] { return mShouldStop.load(); })) {
                // Awakened by stop signal
                break;
            }

            // Execute LRU check and elimination
            CheckAndEliminateFiles();

        } catch (const std::exception& e) {
            logWarn << "LRU thread exception: " << e.what();
            // Wait for a period when exception occurs to avoid rapid loops
            std::this_thread::sleep_for(std::chrono::seconds(10));
        } catch (...) {
            logWarn << "LRU thread unknown exception";
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }

    logInfo << "LRU thread stopped";
}

void FileManager::runFlushThread()
{
    logInfo << "Flush thread started";

    while (!mShouldStop) {
        try {
            // Wait for specified flush interval or be awakened by stop signal
            std::unique_lock<std::mutex> lock(mFlushConditionMutex);
            if (mFlushCondition.wait_for(lock, std::chrono::seconds(mOpt.AccessRecordFlushInterval), [this] {
                    return mShouldStop.load();
                })) {
                // Awakened by stop signal
                break;
            }

            // Execute access record flush
            FlushAccessRecords();

        } catch (const std::exception& e) {
            logWarn << "Flush thread exception: " << e.what();
            // Wait for a period when exception occurs to avoid rapid loops
            std::this_thread::sleep_for(std::chrono::seconds(10));
        } catch (...) {
            logWarn << "Flush thread unknown exception";
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }

    // Final flush before thread stops to ensure no data is lost
    try {
        FlushAccessRecords();
        logInfo << "Final flush completed before thread exit";
    } catch (const std::exception& e) {
        logWarn << "Final flush failed: " << e.what();
    } catch (...) {
        logWarn << "Final flush unknown exception";
    }

    logInfo << "Flush thread stopped";
}

void FileManager::runReportThread()
{
    logInfo << "Report thread started";

    while (!mShouldStop) {
        try {
            // Wait for specified report interval or be awakened by stop signal
            std::unique_lock<std::mutex> lock(mReportConditionMutex);
            if (mReportCondition.wait_for(
                    lock, std::chrono::seconds(mOpt.ReportInterval), [this] { return mShouldStop.load(); })) {
                // Awakened by stop signal
                break;
            }

            // Skip reporting if no report URL configured
            if (mOpt.PCDNReportUrl.empty()) {
                continue;
            }

            // Query files that haven't been reported for the longest time from database
            auto db = getDB();
            if (!db) {
                logWarn << "Failed to get database connection for reporting files";
                continue;
            }

            // Query files that haven't been reported for the longest time, sorted by last_report in ascending order
            // If last_report is empty or 0, prioritize reporting
            auto files = db->stor.select(
                columns(
                    &FileItem::id,
                    &FileItem::file_hash,
                    &FileItem::block_hash,
                    &FileItem::block_start,
                    &FileItem::block_end,
                    &FileItem::last_access),
                where(c(&FileItem::status) == FileStatus::AVAILABLE),
                order_by(&FileItem::last_report).asc(),
                limit(mOpt.ReportBatchSize));

            if (files.empty()) {
                logDebug << "No files to report";
                continue;
            }

            logInfo << "Reporting " << files.size() << " files to PCDN server";
            uint64_t successful_reports = 0;

            // Batch report files
            std::vector<std::tuple<FileItem, std::string>> files_to_report;
            for (const auto& file : files) {
                FileItem item;
                item.id = std::get<0>(file);
                item.file_hash = std::get<1>(file);
                item.block_hash = std::get<2>(file);
                item.block_start = std::get<3>(file);
                item.block_end = std::get<4>(file);
                files_to_report.push_back(std::tuple(item, ""));
            }
            try {
                reportHaveFiles(files_to_report);
            } catch (const std::exception& e) {
                logWarn << "Failed to report files: " << e.what();
                continue; // Report failed, skip this iteration
            } catch (...) {
                logWarn << "Unknown exception occurred during file reporting";
                continue; // Report failed, skip this iteration
            }

            // Update report time in database
            uint64_t current_time = getCurrentTimestamp();
            std::vector<uint64_t> file_ids;
            for (const auto& file : files) {
                file_ids.push_back(std::get<0>(file));
            }
            if (!file_ids.empty()) {
                try {
                    db->stor.update_all(
                        set(c(&FileItem::last_report) = current_time), where(in(&FileItem::id, file_ids)));
                    successful_reports += file_ids.size();
                } catch (const std::exception& e) {
                    logWarn << "Failed to batch update last report time: " << e.what();
                }
            }
        } catch (const std::exception& e) {
            logWarn << "Report thread exception: " << e.what();
            // Wait for a period when exception occurs to avoid rapid loops
            std::this_thread::sleep_for(std::chrono::seconds(10));
        } catch (...) {
            logWarn << "Report thread unknown exception";
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
    logInfo << "Report thread stopped";
}

void FileManager::runScanThread()
{
    logInfo << "Scan thread started";

    while (!mShouldStop) {
        try {
            // Wait for specified scan interval or be awakened by stop signal
            std::unique_lock<std::mutex> lock(mScanConditionMutex);
            if (mScanCondition.wait_for(
                    lock, std::chrono::seconds(mOpt.ScanInterval), [this] { return mShouldStop.load(); })) {
                // Awakened by stop signal
                break;
            }

            // Execute unified file system and database consistency check and cleanup
            scanAndCleanInconsistentFiles();

            // Clean expired download files
            cleanStaleDownloads();
        } catch (const std::exception& e) {
            logWarn << "Scan thread exception: " << e.what();
            // Wait for a period when exception occurs to avoid rapid loops
            std::this_thread::sleep_for(std::chrono::seconds(30));
        } catch (...) {
            logWarn << "Scan thread unknown exception";
            std::this_thread::sleep_for(std::chrono::seconds(30));
        }
    }

    logInfo << "Scan thread stopped";
}

void FileManager::scanFilesystemAndDatabase(
    std::vector<std::string>& filesystem_files,
    std::unordered_map<std::string, std::tuple<uint64_t, std::string, uint64_t, uint64_t>>& db_files_map)
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
    std::lock_guard<std::mutex> file_lock(mFileOperationMutex);

    logDebug << "Acquiring file operation lock for filesystem and database scan";

    // 1. Scan file system and collect all file paths
    std::filesystem::recursive_directory_iterator dir_iter(fileDir());
    for (const auto& entry : dir_iter) {
        if (mShouldStop) {
            logInfo << "Filesystem scan interrupted by stop signal";
            return;
        }

        // Only process regular files, skip directories and special files
        if (!entry.is_regular_file()) {
            continue;
        }

        try {
            filesystem_files.push_back(entry.path().string());
        } catch (const std::exception& e) {
            logWarn << "Error processing file path " << entry.path() << ": " << e.what();
            continue; // Skip files that can't be processed
        }
    }

    // 2. Query database and get all AVAILABLE status file information
    try {
        auto db_files = db->stor.select(
            columns(&FileItem::id, &FileItem::path, &FileItem::file_hash, &FileItem::block_start, &FileItem::block_end),
            where(c(&FileItem::status) == FileStatus::AVAILABLE));

        for (const auto& file : db_files) {
            uint64_t id = std::get<0>(file);
            std::string path = std::get<1>(file);
            std::string hash = std::get<2>(file);
            uint64_t block_start = std::get<3>(file);
            uint64_t block_end = std::get<4>(file);

            db_files_map[path] = std::make_tuple(id, hash, block_start, block_end);
        }

        logInfo << "Filesystem and database scan completed: found " << filesystem_files.size()
                << " files in filesystem, " << db_files_map.size() << " files in database";

    } catch (const std::exception& e) {
        logWarn << "Failed to query database during scan: " << e.what();
        throw; // Re-throw exception for caller to handle
    }
}

uint64_t FileManager::cleanOrphanFiles(const std::vector<std::string>& orphan_files)
{
    if (orphan_files.empty()) {
        return 0;
    }

    logInfo << "Cleaning " << orphan_files.size() << " orphan files";

    uint64_t orphan_deleted_count = 0;

    for (const auto& orphan_file : orphan_files) {
        if (mShouldStop) {
            logInfo << "Orphan file cleanup interrupted by stop signal";
            break;
        }

        try {
            std::filesystem::path orphan_path(orphan_file);
            // Check again if file exists (may have been deleted by other processes)
            if (!std::filesystem::exists(orphan_path)) {
                logDebug << "Orphan file no longer exists, skipping: " << orphan_file;
                continue;
            }
            // Delete orphan file
            std::filesystem::remove(orphan_path);
            orphan_deleted_count++;
            logInfo << "Deleted orphan file: " << orphan_file;
        } catch (const std::filesystem::filesystem_error& e) {
            logWarn << "Failed to delete orphan file " << orphan_file << ": " << e.what();
        } catch (const std::exception& e) {
            logWarn << "Error processing orphan file " << orphan_file << ": " << e.what();
        }
    }

    logInfo << "Orphan file cleanup completed: deleted " << orphan_deleted_count << " files";

    return orphan_deleted_count;
}

uint64_t FileManager::cleanMissingFiles(
    const std::vector<std::tuple<uint64_t, std::string, std::string, uint64_t, uint64_t>>& missing_files)
{
    if (missing_files.empty()) {
        return 0;
    }

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for cleaning missing files";
        return 0;
    }

    logInfo << "Cleaning " << missing_files.size() << " missing file records";

    std::vector<uint64_t> missing_file_ids;
    std::vector<FileItem> missing_files_for_report;

    // Handle LRU cache cleanup
    for (const auto& missing_file : missing_files) {
        uint64_t file_id = std::get<0>(missing_file);
        std::string missing_file_path = std::get<1>(missing_file);
        std::string file_hash = std::get<2>(missing_file);
        uint64_t block_start = std::get<3>(missing_file);
        uint64_t block_end = std::get<4>(missing_file);

        missing_file_ids.push_back(file_id);

        // Prepare file information for reporting
        FileItem missing_item;
        missing_item.id = file_id;
        missing_item.path = missing_file_path;
        missing_item.file_hash = file_hash;
        missing_item.block_start = block_start;
        missing_item.block_end = block_end;
        missing_files_for_report.push_back(missing_item);

        logDebug << "Found missing file in database: " << missing_file_path << " (ID: " << file_id << ")";

        // Remove this file from LRU cache (if exists)
        {
            std::lock_guard<std::mutex> lock(mLRUMutex);
            auto cache_it = mLRUCache.find(file_id);
            if (cache_it != mLRUCache.end()) {
                mLRUList.erase(cache_it->second.list_iter);
                mLRUCache.erase(cache_it);
                logDebug << "Removed missing file from LRU cache: " << file_id;
            }
        }
    }

    // Batch delete missing file records from database
    uint64_t deleted_count = 0;
    if (!missing_file_ids.empty()) {
        try {
            // Use sqlite_orm batch delete: WHERE id IN (?, ?, ?, ...)
            // This only requires one database operation to delete all records
            db->stor.remove_all<FileItem>(where(in(&FileItem::id, missing_file_ids)));

            // Since remove_all doesn't return delete count, we assume all deletions succeeded
            // If precise counting is needed, query the number of matching records first
            deleted_count = missing_file_ids.size();

            logInfo << "Successfully removed " << deleted_count
                    << " missing file records from database (batch operation)";

        } catch (const std::exception& e) {
            logWarn << "Batch delete failed, falling back to individual deletes: " << e.what();

            // If batch delete fails, fall back to individual deletions
            for (const auto& file_id : missing_file_ids) {
                try {
                    db->stor.remove<FileItem>(file_id);
                    deleted_count++;
                } catch (const std::exception& e) {
                    logWarn << "Failed to delete missing file record ID " << file_id << ": " << e.what();
                }
            }

            logInfo << "Successfully removed " << deleted_count
                    << " missing file records from database (individual operations)";
        }
    }

    // Report deleted files (if report URL configured)
    if (!mOpt.PCDNReportUrl.empty()) {
        logInfo << "Reporting " << missing_files_for_report.size() << " removed files to PCDN server";

        uint64_t reported_count = 0;
        for (const auto& missing_file : missing_files_for_report) {
            try {
                reportRemoveFile(missing_file);
                reported_count++;
            } catch (const std::exception& e) {
                logWarn << "Failed to report removed file " << missing_file.path << ": " << e.what();
            }
        }

        logInfo << "Successfully reported " << reported_count << "/" << missing_files_for_report.size()
                << " removed files";
    }

    return deleted_count;
}

void FileManager::scanAndCleanInconsistentFiles()
{
    logInfo << "Starting unified file system and database consistency check";

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for consistency check";
        return;
    }

    try {
        // Data structures for storing scan and check results
        std::vector<std::string> filesystem_files; // File list in filesystem
        std::unordered_map<std::string, std::tuple<uint64_t, std::string, uint64_t, uint64_t>>
            db_files_map; // Database file mapping: path -> (id, hash, block_start,
                          // block_end)

        // Use helper method to scan filesystem and database
        scanFilesystemAndDatabase(filesystem_files, db_files_map);

        // Analyze and clean inconsistent files
        std::vector<std::string> orphan_files; // Orphan files in filesystem but not in database
        std::vector<std::tuple<uint64_t, std::string, std::string, uint64_t, uint64_t>>
            missing_files; // Files in database but not in filesystem

        // Find orphan files
        for (const auto& fs_file : filesystem_files) {
            if (db_files_map.find(fs_file) == db_files_map.end()) {
                orphan_files.push_back(fs_file);
            }
        }

        // Find missing files
        for (const auto& [db_path, file_info] : db_files_map) {
            bool found_in_filesystem = false;
            for (const auto& fs_file : filesystem_files) {
                if (fs_file == db_path) {
                    found_in_filesystem = true;
                    break;
                }
            }

            if (!found_in_filesystem) {
                uint64_t id = std::get<0>(file_info);
                std::string hash = std::get<1>(file_info);
                uint64_t block_start = std::get<2>(file_info);
                uint64_t block_end = std::get<3>(file_info);
                missing_files.push_back(std::make_tuple(id, db_path, hash, block_start, block_end));
            }
        }

        logInfo << "Found " << orphan_files.size() << " orphan files and " << missing_files.size() << " missing files";

        // Handle orphan files (in filesystem but not in database)
        uint64_t orphan_deleted_count = cleanOrphanFiles(orphan_files);

        // Handle missing files (in database but not in filesystem)
        uint64_t missing_deleted_count = cleanMissingFiles(missing_files);

        logInfo << "Unified consistency check completed: processed " << filesystem_files.size() << " filesystem files, "
                << db_files_map.size() << " database files, deleted " << orphan_deleted_count << " orphan files and "
                << missing_deleted_count << " missing file records";

    } catch (const std::exception& e) {
        logWarn << "Failed to perform unified consistency check: " << e.what();
    }
}

void FileManager::cleanStaleDownloads()
{
    logInfo << "Starting cleanup of stale download files";

    std::filesystem::path tmp_dir = tmpDir();

    // Check if tmp directory exists
    if (!std::filesystem::exists(tmp_dir)) {
        logDebug << "Tmp directory does not exist: " << tmp_dir;
        return;
    }

    auto db = getDB();
    if (!db) {
        logWarn << "Failed to get database connection for stale download cleanup";
        return;
    }

    try {
        uint64_t current_time = getCurrentTimestamp();
        uint64_t timeout_seconds = mOpt.DownloadTimeout;

        // Recursively scan tmp directory
        uint64_t scanned_count = 0;
        uint64_t stale_count = 0;
        uint64_t deleted_count = 0;
        uint64_t total_size_freed = 0;
        std::vector<std::string> deleted_files_for_db;

        std::filesystem::recursive_directory_iterator dir_iter(tmp_dir);

        for (const auto& entry : dir_iter) {
            if (mShouldStop) {
                logInfo << "Stale download cleanup interrupted by stop signal";
                break;
            }

            // Only process regular files, skip directories and special files
            if (!entry.is_regular_file()) {
                continue;
            }

            scanned_count++;

            try {
                // Get file's last write time
                auto last_write_time = std::filesystem::last_write_time(entry.path());
                auto durSinceLastWrite = durSinceFileLastUpdateTime(entry.path());
                // Check if file has timed out
                if (durSinceLastWrite > timeout_seconds) {
                    stale_count++;

                    try {
                        // Get file size
                        uint64_t file_size = std::filesystem::file_size(entry.path());
                        std::string file_path_str = entry.path().string();
                        std::filesystem::remove(entry.path());
                        deleted_count++;
                        total_size_freed += file_size;

                        // Record files that need to be deleted from database
                        deleted_files_for_db.push_back(file_path_str);

                        uint64_t stale_duration = durSinceLastWrite;
                        logInfo << "Deleted stale download file: " << file_path_str << " (size: " << file_size
                                << " bytes, stale for: " << stale_duration << " seconds)";

                    } catch (const std::filesystem::filesystem_error& e) {
                        logWarn << "Failed to delete stale download file " << entry.path() << ": " << e.what();
                    }
                }

                // Output progress every 1000 scanned files
                if (scanned_count % 1000 == 0) {
                    logDebug << "Scanned " << scanned_count << " download files, found " << stale_count
                             << " stale files";
                }

            } catch (const std::exception& e) {
                logWarn << "Error processing download file " << entry.path() << ": " << e.what();
            }
        }

        // Delete corresponding download records from database
        if (!deleted_files_for_db.empty()) {
            try {
                // Find and delete records with DOWNLOADING status and matching paths from database
                uint64_t db_deleted_count = 0;

                for (const auto& file_path : deleted_files_for_db) {
                    try {
                        // Query matching download records
                        auto download_records = db->stor.select(
                            &FileItem::id,
                            where(c(&FileItem::status) == FileStatus::DOWNLOADING and c(&FileItem::path) == file_path));

                        // Delete found records
                        for (const auto& record_id : download_records) {
                            db->stor.remove<FileItem>(record_id);
                            db_deleted_count++;

                            logDebug << "Removed stale download record from database: " << file_path
                                     << " (ID: " << record_id << ")";
                        }

                    } catch (const std::exception& e) {
                        logWarn << "Failed to remove download record for " << file_path << ": " << e.what();
                    }
                }

                if (db_deleted_count > 0) {
                    logInfo << "Removed " << db_deleted_count << " stale download records from database";
                }

            } catch (const std::exception& e) {
                logWarn << "Failed to clean stale download records from database: " << e.what();
            }
        }

        logInfo << "Stale download cleanup completed: scanned " << scanned_count << " files, found " << stale_count
                << " stale files, "
                << "deleted " << deleted_count << " files, "
                << "freed " << total_size_freed << " bytes";

    } catch (const std::exception& e) {
        logWarn << "Failed to clean stale download files: " << e.what();
    }
}

NS_END
