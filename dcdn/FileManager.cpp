#include "FileManager.h"
#include "MainManager.h"
#include "Event.h"
#include <sqlite3.h>
#include <sqlite_orm/sqlite_orm.h>
#include "SqliteOrmHelper.h"
#include <filesystem>
#include <chrono>

NS_BEGIN(dcdn)

auto createFileStorage(const std::string &filename)
{
    using namespace sqlite_orm;
    return make_storage(filename,
                        make_unique_index("idx_unique", &FileItem::file_hash, &FileItem::file_hash, &FileItem::block_start, &FileItem::block_end),
                        make_index("idx_file_start", &FileItem::file_hash, &FileItem::block_start),
                        make_index("idx_last_report", &FileItem::last_report),
                        make_table("files",
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
                                   make_column("created_at", &FileItem::created_at, default_value("CURRENT_TIMESTAMP"))));
}

class StorageRef : public StorageRefImpl<createFileStorage>
{
public:
    using Base::Base;
};

FileManager::FileManager(MainManager *man, const FileManagerOption &opt) : BaseManager(man), mOpt(opt)
{
    mLastFlushTime = std::chrono::steady_clock::now();
    mLastLRUCheckTime = std::chrono::steady_clock::now();
}

FileManager::~FileManager()
{
}

void FileManager::run()
{
    logInfo << "FileManager running";
#if 1
    createTable();
#else
    if (auto db = getDB())
    {
        try
        {
            db->stor.sync_schema();
        }
        catch (...)
        {
        }
    }
#endif

    while (true)
    {
        waitAllEvents(std::chrono::milliseconds(1000));
        
        // 检查是否需要刷新访问记录到数据库
        auto now = std::chrono::steady_clock::now();
        auto flush_duration = std::chrono::duration_cast<std::chrono::seconds>(now - mLastFlushTime);
        if (flush_duration.count() >= mOpt.AccessRecordFlushInterval)
        {
            FlushAccessRecords();
            mLastFlushTime = now;
        }
        
        // 检查是否需要进行LRU淘汰
        auto lru_duration = std::chrono::duration_cast<std::chrono::seconds>(now - mLastLRUCheckTime);
        if (lru_duration.count() >= mOpt.LRUCheckInterval)
        {
            CheckAndEliminateFiles();
            mLastLRUCheckTime = now;
        }
    }
    logInfo << "FileManager exit";
}

std::shared_ptr<StorageRef> FileManager::getDB()
{
    if (!mDB)
    {
        try
        {
            std::filesystem::path dbFile(mMan->Option().WorkDir);
            dbFile.append("files.db");
            mDB = std::make_shared<StorageRef>(dbFile);
        }
        catch (std::exception &excp)
        {
            logWarn << "create config.db exception: " << excp.what();
        }
        catch (...)
        {
            logWarn << "create config.db unknown exception";
        }
    }
    return mDB;
}

int FileManager::createTable()
{
    sqlite3 *db = nullptr;
    std::filesystem::path dbFile(mMan->Option().WorkDir);
    dbFile.append("files.db");
    int rc = sqlite3_open(dbFile.c_str(), &db);
    if (rc != SQLITE_OK)
    {
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
    if (rc != SQLITE_OK)
    {
        logWarn << "create table(files) err:" << errMsg;
        sqlite3_free(errMsg);
        sqlite3_close(db);
        return ErrorCodeErr;
    }
    return ErrorCodeOk;
}

void FileManager::handleEvent(std::shared_ptr<Event> evt)
{
    switch (evt->Type())
    {
    case EventType::AddFile:
        break;
    default:
        break;
    }
}

void FileManager::handleAddFile(std::shared_ptr<Event> evt)
{
    auto e = static_cast<ArgEvent<AddFileArg> *>(evt.get());
    auto &arg = e->Arg();
}

std::string FileManager::GetPathByBlockHash(const std::string &block_hash, bool need_report)
{
    auto db = getDB();
    if (!db)
    {
        return "";
    }
    
    try
    {
        sqlite3 *sqlite_db = nullptr;
        std::filesystem::path dbFile(mMan->Option().WorkDir);
        dbFile.append("files.db");
        
        int rc = sqlite3_open(dbFile.c_str(), &sqlite_db);
        if (rc != SQLITE_OK)
        {
            return "";
        }
        
        const char *sql = "SELECT id, file_path, block_end - block_start FROM files WHERE block_hash = ? AND status = ? LIMIT 1";
        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(sqlite_db, sql, -1, &stmt, nullptr);
        
        if (rc != SQLITE_OK)
        {
            sqlite3_close(sqlite_db);
            return "";
        }
        
        sqlite3_bind_text(stmt, 1, block_hash.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 2, static_cast<int>(FileStatus::AVAILABLE));
        
        std::string file_path;
        if (sqlite3_step(stmt) == SQLITE_ROW)
        {
            uint64_t file_id = sqlite3_column_int64(stmt, 0);
            file_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
            uint64_t file_size = sqlite3_column_int64(stmt, 2);  // 使用 block_end - block_start
            
            // 记录文件访问，使用文件ID
            RecordFileAccess(file_id, file_path, file_size);
        }
        
        sqlite3_finalize(stmt);
        sqlite3_close(sqlite_db);
        
        return file_path;
    }
    catch (const std::exception &e)
    {
        logWarn << "Failed to get path by block hash: " << e.what();
        return "";
    }
}

std::string FileManager::NewDownloadPath(const FileDescriptor &file)
{
    std::filesystem::path storage_path(mOpt.RootPath);
    std::string file_name = FilePath(file);
    storage_path.append(file_name);
    return storage_path.string();
}

// LRU相关方法实现

void FileManager::RecordFileAccess(uint64_t file_id, const std::string &file_path, uint64_t file_size)
{
    std::lock_guard<std::mutex> lock(mLRUMutex);
    
    uint64_t current_time = getCurrentTimestamp();
    
    auto it = mLRUCache.find(file_id);
    if (it != mLRUCache.end())
    {
        // 文件已存在，更新访问时间并移到列表头部
        mLRUList.erase(it->second.list_iter);
        mLRUList.push_front(file_id);
        it->second.list_iter = mLRUList.begin();
        it->second.last_access = current_time;
        it->second.access_count++;
        it->second.is_dirty = true;  // 标记为需要刷新到数据库
        
        // 更新文件大小和路径（如果有变化）
        if (file_size > 0)
        {
            it->second.file_size = file_size;
        }
        it->second.file_path = file_path;
    }
    else
    {
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

void FileManager::FlushAccessRecords()
{
    std::vector<LRUNode> dirty_nodes;
    
    {
        std::lock_guard<std::mutex> lock(mLRUMutex);
        
        // 收集所有脏节点
        for (auto &pair : mLRUCache)
        {
            if (pair.second.is_dirty)
            {
                dirty_nodes.push_back(pair.second);
                pair.second.is_dirty = false;  // 清除脏标记
            }
        }
    }
    
    if (dirty_nodes.empty())
    {
        return;
    }
    
    auto db = getDB();
    if (!db)
    {
        logWarn << "Failed to get database connection for flushing access records";
        return;
    }
    
    try
    {
        // 批量更新数据库中文件的访问时间
        sqlite3 *sqlite_db = nullptr;
        std::filesystem::path dbFile(mMan->Option().WorkDir);
        dbFile.append("files.db");
        
        int rc = sqlite3_open(dbFile.c_str(), &sqlite_db);
        if (rc != SQLITE_OK)
        {
            logWarn << "Failed to open database for flushing access records";
            return;
        }
        
        const char *sql = "UPDATE files SET last_access = ? WHERE id = ?";
        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(sqlite_db, sql, -1, &stmt, nullptr);
        
        if (rc == SQLITE_OK)
        {
            for (const auto &node : dirty_nodes)
            {
                sqlite3_bind_int64(stmt, 1, node.last_access);
                sqlite3_bind_int64(stmt, 2, node.file_id);  // 使用文件ID更新
                sqlite3_step(stmt);
                sqlite3_reset(stmt);
            }
            sqlite3_finalize(stmt);
        }
        sqlite3_close(sqlite_db);
        
        logInfo << "Flushed " << dirty_nodes.size() << " access records to database";
    }
    catch (const std::exception &e)
    {
        logWarn << "Failed to flush access records: " << e.what();
        
        // 如果刷新失败，重新标记为脏数据
        std::lock_guard<std::mutex> lock(mLRUMutex);
        for (const auto &node : dirty_nodes)
        {
            auto it = mLRUCache.find(node.file_id);
            if (it != mLRUCache.end())
            {
                it->second.is_dirty = true;
            }
        }
    }
}

uint64_t FileManager::getCurrentTimestamp()
{
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

uint64_t FileManager::calculateTotalStorageSize()
{
    uint64_t total_size = 0;
    
    try
    {
        sqlite3 *sqlite_db = nullptr;
        std::filesystem::path dbFile(mMan->Option().WorkDir);
        dbFile.append("files.db");
        
        int rc = sqlite3_open(dbFile.c_str(), &sqlite_db);
        if (rc != SQLITE_OK)
        {
            logWarn << "Failed to open database for calculating storage size";
            return 0;
        }
        
        // 计算所有文件的总大小
        // 使用 block_end - block_start 来计算每个块的实际大小
        const char *sql = "SELECT SUM(block_end - block_start) FROM files";
        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(sqlite_db, sql, -1, &stmt, nullptr);
        
        if (rc != SQLITE_OK)
        {
            logWarn << "Failed to prepare SQL for calculating storage size";
            sqlite3_close(sqlite_db);
            return 0;
        }
        
        if (sqlite3_step(stmt) == SQLITE_ROW)
        {
            total_size = sqlite3_column_int64(stmt, 0);
        }
        
        sqlite3_finalize(stmt);
        sqlite3_close(sqlite_db);
        
        logDebug << "Calculated total storage size from database: " << total_size << " bytes";
    }
    catch (const std::exception &e)
    {
        logWarn << "Failed to calculate storage size from database: " << e.what();
        return 0;
    }
    
    return total_size;
}

void FileManager::CheckAndEliminateFiles()
{
    uint64_t current_size = calculateTotalStorageSize();
    uint64_t upper_bound = (mOpt.MaxStorageSize * mOpt.LRUUpperBoundPercent) / 100;
    
    if (current_size > upper_bound)
    {
        uint64_t target_size = (mOpt.MaxStorageSize * mOpt.LRUTargetPercent) / 100;
        logInfo << "Storage size (" << current_size << ") exceeds upper bound (" << upper_bound 
                << "), starting LRU elimination to target size: " << target_size;
        
        removeLRUFiles(target_size);
    }
}

void FileManager::removeLRUFiles(uint64_t target_size)
{
    std::vector<uint64_t> files_to_remove;
    uint64_t current_size = calculateTotalStorageSize();
    uint64_t size_to_free = 0;
    
    {
        std::lock_guard<std::mutex> lock(mLRUMutex);
        
        // 从LRU列表尾部开始（最旧的文件），选择要删除的文件
        auto it = mLRUList.rbegin();
        while (it != mLRUList.rend() && current_size > target_size)
        {
            uint64_t file_id = *it;
            
            // 获取文件信息
            auto cache_it = mLRUCache.find(file_id);
            if (cache_it != mLRUCache.end())
            {
                files_to_remove.push_back(file_id);
                
                // 使用内存中缓存的文件大小
                uint64_t file_size = cache_it->second.file_size;
                if (file_size > 0)
                {
                    size_to_free += file_size;
                    current_size = (current_size > file_size) ? current_size - file_size : 0;
                }
                else
                {
                    // 如果内存中没有大小信息，从数据库获取 block_end - block_start
                    try
                    {
                        sqlite3 *temp_db = nullptr;
                        std::filesystem::path dbFile(mMan->Option().WorkDir);
                        dbFile.append("files.db");
                        
                        int rc = sqlite3_open(dbFile.c_str(), &temp_db);
                        if (rc == SQLITE_OK)
                        {
                            const char *size_sql = "SELECT block_end - block_start FROM files WHERE id = ?";
                            sqlite3_stmt *size_stmt;
                            rc = sqlite3_prepare_v2(temp_db, size_sql, -1, &size_stmt, nullptr);
                            if (rc == SQLITE_OK)
                            {
                                sqlite3_bind_int64(size_stmt, 1, file_id);
                                if (sqlite3_step(size_stmt) == SQLITE_ROW)
                                {
                                    uint64_t block_size = sqlite3_column_int64(size_stmt, 0);
                                    size_to_free += block_size;
                                    current_size = (current_size > block_size) ? current_size - block_size : 0;
                                    
                                    // 更新缓存中的文件大小
                                    cache_it->second.file_size = block_size;
                                }
                                sqlite3_finalize(size_stmt);
                            }
                            sqlite3_close(temp_db);
                        }
                    }
                    catch (...)
                    {
                        // 如果数据库查询失败，使用估算值
                        uint64_t estimated_size = 1024 * 1024; // 1MB估算
                        size_to_free += estimated_size;
                        current_size = (current_size > estimated_size) ? current_size - estimated_size : 0;
                    }
                }
            }
            ++it;
        }
    }
    
    if (files_to_remove.empty())
    {
        logInfo << "No files to remove for LRU elimination";
        return;
    }
    
    logInfo << "Starting LRU elimination: " << files_to_remove.size() 
            << " files selected, estimated " << size_to_free << " bytes to free";
    
    auto db = getDB();
    if (!db)
    {
        logWarn << "Failed to get database connection for LRU elimination";
        return;
    }
    
    uint64_t actual_freed = 0;
    uint64_t successful_removals = 0;
    
    // 删除选中的文件
    for (uint64_t file_id : files_to_remove)
    {
        try
        {
            std::string file_path;
            uint64_t file_size = 0;
            
            // 先从内存缓存获取文件信息
            {
                std::lock_guard<std::mutex> lock(mLRUMutex);
                auto cache_it = mLRUCache.find(file_id);
                if (cache_it != mLRUCache.end())
                {
                    file_path = cache_it->second.file_path;
                    file_size = cache_it->second.file_size;
                }
            }
            
            // 如果缓存中没有，从数据库获取
            if (file_path.empty())
            {
                sqlite3 *sqlite_db = nullptr;
                std::filesystem::path dbFile(mMan->Option().WorkDir);
                dbFile.append("files.db");
                
                int rc = sqlite3_open(dbFile.c_str(), &sqlite_db);
                if (rc != SQLITE_OK)
                {
                    continue;
                }
                
                const char *sql = "SELECT file_path, block_end - block_start FROM files WHERE id = ? LIMIT 1";
                sqlite3_stmt *stmt;
                rc = sqlite3_prepare_v2(sqlite_db, sql, -1, &stmt, nullptr);
                
                if (rc != SQLITE_OK)
                {
                    sqlite3_close(sqlite_db);
                    continue;
                }
                
                sqlite3_bind_int64(stmt, 1, file_id);
                
                if (sqlite3_step(stmt) == SQLITE_ROW)
                {
                    file_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                    file_size = sqlite3_column_int64(stmt, 1);  // 使用 block_end - block_start
                }
                
                sqlite3_finalize(stmt);
                sqlite3_close(sqlite_db);
            }
            
            if (file_path.empty())
            {
                logWarn << "File path not found for ID: " << file_id;
                continue;
            }
            
            // 删除物理文件
            std::filesystem::path full_path(mOpt.RootPath);
            full_path.append(file_path);
            
            if (std::filesystem::exists(full_path))
            {
                std::filesystem::remove(full_path);
                actual_freed += file_size;
                successful_removals++;
                logInfo << "Removed LRU file: " << full_path << " (size: " << file_size << " bytes)";
            }
            
            // 从数据库删除记录
            sqlite3 *sqlite_db = nullptr;
            std::filesystem::path dbFile(mMan->Option().WorkDir);
            dbFile.append("files.db");
            
            int rc = sqlite3_open(dbFile.c_str(), &sqlite_db);
            if (rc == SQLITE_OK)
            {
                const char *delete_sql = "DELETE FROM files WHERE id = ?";
                sqlite3_stmt *stmt;
                rc = sqlite3_prepare_v2(sqlite_db, delete_sql, -1, &stmt, nullptr);
                if (rc == SQLITE_OK)
                {
                    sqlite3_bind_int64(stmt, 1, file_id);
                    sqlite3_step(stmt);
                    sqlite3_finalize(stmt);
                }
                sqlite3_close(sqlite_db);
            }
            
            // 从LRU缓存和列表中删除
            {
                std::lock_guard<std::mutex> lock(mLRUMutex);
                auto cache_it = mLRUCache.find(file_id);
                if (cache_it != mLRUCache.end())
                {
                    mLRUList.erase(cache_it->second.list_iter);
                    mLRUCache.erase(cache_it);
                }
            }
            
        }
        catch (const std::exception &e)
        {
            logWarn << "Failed to remove file ID " << file_id << ": " << e.what();
        }
    }
    
    logInfo << "LRU elimination completed: " << successful_removals << "/" << files_to_remove.size() 
            << " files removed, " << actual_freed << " bytes freed";
}

NS_END
