#include "Config.h"

#include <sqlite_orm/sqlite_orm.h>

#include <filesystem>

#include "SqliteOrmHelper.h"

NS_BEGIN(dcdn)

auto createConfigStorage(const std::string& filename)
{
    using namespace sqlite_orm;
    return make_storage(
        filename,
        make_table(
            "configs",
            make_column("id", &ConfigItem::id, primary_key().autoincrement()),
            make_column("key", &ConfigItem::key, unique()),
            make_column("val", &ConfigItem::val)));
}

class StorageRef: public StorageRefImpl<createConfigStorage>
{
public:
    using Base::Base;
};

Config::Config()
{
    mApiRootUrl = "https://pcdn.capell.io";
    mWebSktUrl = "ws://pcdn.capell.io";
    mStunServers.emplace_back("39.106.141.70:8347");
    mStunServers.emplace_back("stun1.l.google.com:3478");
}

int Config::CreateTable(const std::string& workDir)
{
    std::filesystem::path cfgFile(workDir);
    cfgFile.append("config.db");
    mDBFile = cfgFile;
    if (auto db = getDB()) {
        try {
            db->stor.sync_schema();
        } catch (std::exception& excp) {
            return ErrorCodeErr;
        } catch (...) {
            return ErrorCodeErr;
        }
    } else {
        return ErrorCodeErr;
    }
    return ErrorCodeOk;
}

std::shared_ptr<StorageRef> Config::getDB()
{
    try {
        auto db = std::make_shared<StorageRef>(mDBFile);
        return db;
    } catch (std::exception& excp) {
        logWarn << "open config.db exception: " << excp.what();
    } catch (...) {
        logWarn << "open config.db unknown exception";
    }
    return nullptr;
}

int Config::LoadFromDB()
{
    try {
        auto db = getDB();
        if (!db) {
            return ErrorCodeErr;
        }
        auto items = db->stor.get_all<ConfigItem>();
        for (auto& item : items) {
            if (item.key == "peerId") {
                SetPeerId(item.val);
            } else if (item.key == "token") {
                SetToken(item.val);
            }
            mKv.erase(item.key);
        }
    } catch (std::exception& excp) {
        logWarn << "load config exception: " << excp.what();
        return ErrorCodeErr;
    } catch (...) {
        logWarn << "load config exception unknown";
        return ErrorCodeErr;
    }
    return ErrorCodeOk;
}

int Config::SaveToDB()
{
    do {
        std::unique_lock lck(mMtx);
        if (mKv.empty()) {
            return ErrorCodeOk;
        }
    } while (0);
    int ret = ErrorCodeOk;
    std::unordered_map<std::string, std::string> kvs;
    try {
        auto db = getDB();
        if (!db) {
            return ErrorCodeErr;
        }

        mMtx.lock();
        kvs.swap(mKv);
        mMtx.unlock();

        db->stor.transaction([&]() {
            for (auto& it : kvs) {
                db->stor.replace(ConfigItem{.id = 0, .key = it.first, .val = it.second});
            }
            return true;
        });
    } catch (std::exception& excp) {
        ret = ErrorCodeErr;
    } catch (...) {
        ret = ErrorCodeErr;
    }

    if (ret != ErrorCodeOk && !kvs.empty()) {
        std::unique_lock lck(mMtx);
        for (auto& it : mKv) {
            kvs[it.first] = it.second;
        }
        mKv.swap(kvs);
    }
    return ret;
}

NS_END
