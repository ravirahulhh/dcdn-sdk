#ifndef _DCDN_WEBRTC_MANAGER_H_
#define _DCDN_WEBRTC_MANAGER_H_

#include <nlohmann/json.hpp>
#include <rtc/rtc.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "BaseManager.h"
#include "Cert.h"
#include "util/HttpClient.h"

NS_BEGIN(dcdn)

class WebRtcManager: public BaseManager
{
public:
    using json = nlohmann::json;

public:
    WebRtcManager(MainManager* man);

private:
    void run();
    void runGather();
    void gather();
    void gatherDone();
    void report();
    enum GatherStatus
    {
        GatherIdle,
        GatherRunning,
        GatherDone,
    };

private:
    std::mutex mMtx;
    std::condition_variable mCv;

    dcdn::util::HttpClient mClient;
    CertificatePair mCert;
    std::chrono::time_point<std::chrono::steady_clock> mLastGatherTime;
    std::atomic<GatherStatus> mGatherStatus = GatherIdle;
    std::shared_ptr<rtc::PeerConnection> mPc;
    std::string mSdp;
};

NS_END

#endif
