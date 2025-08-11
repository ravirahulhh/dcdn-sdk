#include "FileManager.h"

#include <sqlite3.h>
#include <sqlite_orm/sqlite_orm.h>

#include "MainManager.h"
#include "SqliteOrmHelper.h"

NS_BEGIN(dcdn)

auto createFileStorage(const std::string& filename)
{
    using namespace sqlite_orm;
    return make_storage(
        filename,
        make_unique_index(
            "idx_unique", &FileItem::block_hash, &FileItem::file_hash, &FileItem::block_start, &FileItem::block_end),
        make_index("idx_file_start", &FileItem::file_hash, &FileItem::block_start),
        make_index("idx_last_report", &FileItem::last_report),
        make_table(
            "files",
            make_column("id", &FileItem::id, primary_key().autoincrement()),
            make_column("block_hash", &FileItem::block_hash),
            make_column("file_hash", &FileItem::block_hash),
            make_column("file_path", &FileItem::file_path),
            make_column("file_size", &FileItem::file_size),
            make_column("block_start", &FileItem::block_start),
            make_column("block_end", &FileItem::block_end),
            make_column("last_report", &FileItem::last_report),
            make_column("created_at", &FileItem::created_at, default_value("CURRENT_TIMESTAMP"))));
}

class StorageRef: public StorageRefImpl<createFileStorage>
{
public:
    using Base::Base;
};

FileManager::FileManager(MainManager* man): BaseManager(man) {}

FileManager::~FileManager() {}

void FileManager::run()
{
    logInfo << "FileManager running";
#if 1
    createTable();
#else
    if (auto db = getDB()) {
        try {
            db->stor.sync_schema();
        } catch (...) {
        }
    }
#endif

    while (true) {
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
    }
    return mDB;
}

int FileManager::createTable()
{
    sqlite3* db = nullptr;
    std::filesystem::path dbFile(mMan->Option().WorkDir);
    dbFile.append("files.db");
    int rc = sqlite3_open(dbFile.c_str(), &db);
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
        file_size INTEGER,
        block_start INTEGER,
        block_end INTEGER,
        last_report INTEGER,
        create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(block_hash, file_hash, block_start, block_end)
        );
        CREATE INDEX IF NOT EXISTS idx_file_start ON files(file_hash, block_start);
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

void FileManager::handleEvent(std::shared_ptr<Event> evt)
{
    switch (evt->Type()) {
        case EventType::AddFile:
            break;
        default:
            break;
    }
}

void FileManager::handleAddFile(std::shared_ptr<Event> evt)
{
    auto e = static_cast<ArgEvent<AddFileArg>*>(evt.get());
    auto& arg = e->Arg();
}

NS_END
