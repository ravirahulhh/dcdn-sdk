#ifndef _DCDN_SDK_CONFIG_H_
#define _DCDN_SDK_CONFIG_H_

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/Common.h"

NS_BEGIN(dcdn)

struct ConfigItem
{
    uint64_t id;
    std::string key;
    std::string val;
};

class StorageRef;
class Config
{
public:
    Config();
    int CreateTable(const std::string& workDir);
    int LoadFromDB();
    int SaveToDB();

    std::string ApiRootUrl() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        auto val = mApiRootUrl;
        return val;
    }
    std::string WebSktUrl() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        auto val = mWebSktUrl;
        return val;
    }
    std::string PeerId() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        auto val = mPeerId;
        return val;
    }
    std::string Token() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        auto val = mToken;
        return val;
    }
    std::vector<std::string> StunServers() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        auto val = mStunServers;
        return val;
    }

    void SetPeerId(const std::string& peerId)
    {
        std::unique_lock<std::mutex> lck(mMtx);
        if (peerId != mPeerId) {
            mPeerId = peerId;
            mKv["peerId"] = peerId;
        }
    }
    void SetToken(const std::string& token)
    {
        std::unique_lock<std::mutex> lck(mMtx);
        if (token != mToken) {
            mToken = token;
            mKv["token"] = token;
        }
    }
    unsigned WebRtcGatherPeriod() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        unsigned val = mWebRtcGatherPeriod;
        return val;
    }
    unsigned WebSktConnectTimeout() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        unsigned val = mWebSktConnectTimeout;
        return val;
    }
    bool WebSktDisableTlsVerification() const
    {
        std::unique_lock<std::mutex> lck(mMtx);
        bool val = mWebSktDisableTlsVerification;
        return val;
    }

private:
    std::shared_ptr<StorageRef> getDB();

private:
    mutable std::mutex mMtx;
    std::string mDBFile;

    std::string mApiRootUrl;
    std::string mWebSktUrl;
    std::string mPeerId;
    std::string mToken;
    std::vector<std::string> mStunServers;

    unsigned mWebRtcGatherPeriod = 60; // seconds

    unsigned mWebSktConnectTimeout = 60; // seconds
    bool mWebSktDisableTlsVerification = true;

    std::unordered_map<std::string, std::string> mKv;
};

NS_END

#endif
