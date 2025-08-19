#include "WebRtcManager.h"

#include <chrono>

#include "MainManager.h"

NS_BEGIN(dcdn)

static const std::string webRtcConnProtocol = "ProtocolWebRTC";

WebRtcManager::WebRtcManager(MainManager* man): BaseManager(man)
{
    mCert = generate_ecdsa_certificate();
    mLastGatherTime = std::chrono::steady_clock::now() - std::chrono::hours(24);
}

void WebRtcManager::run()
{
    logInfo << "WebRtc running";
    while (true) {
        runGather();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    logInfo << "WebRtc exit";
}

void WebRtcManager::runGather()
{
    switch (mGatherStatus) {
        case GatherIdle: {
            auto now = std::chrono::steady_clock::now();
            unsigned period = mMan->Cfg().WebRtcGatherPeriod();
            if (mLastGatherTime + std::chrono::seconds(period) <= now) {
                gather();
            }
        } break;
        case GatherRunning:
            break;
        case GatherDone:
            mGatherStatus = GatherIdle;
            mLastGatherTime = std::chrono::steady_clock::now();
            gatherDone();
            break;
    }
}

void WebRtcManager::gather()
{
    try {
        mGatherStatus = GatherRunning;
        rtc::Configuration config;
        config.enableIceUdpMux = true;
        config.certificatePemFile = mCert.certPem;
        config.keyPemFile = mCert.keyPem;
        auto ss = mMan->Cfg().StunServers();
        for (auto s : ss) {
            auto idx = s.find(':');
            if (idx == std::string::npos) {
                continue;
            }
            std::string host = s.substr(0, idx);
            int port = atoi(s.c_str() + idx + 1);
            if (port > 0 && port < 65536) {
                rtc::IceServer serv(host, port);
                serv.type = rtc::IceServer::Type::Stun;
                config.iceServers.push_back(serv);
            }
        }
        auto pc = std::make_shared<rtc::PeerConnection>(config);
        pc->onGatheringStateChange([&](rtc::PeerConnection::GatheringState state) {
            if (state == rtc::PeerConnection::GatheringState::Complete) {
                mGatherStatus = GatherDone;
            }
        });
        auto dc = pc->createDataChannel("detect"); // start gathering
        mPc = pc;
    } catch (std::exception& excp) {
        logWarn << "webrtc gather excpetion: " << excp.what();
    } catch (...) {
        logWarn << "webrtc gather unknown excpetion";
    }
}

void WebRtcManager::gatherDone()
{
    try {
        auto dsOpt = mPc->localDescription();
        if (dsOpt) {
            std::string sdp(dsOpt.value());
            mSdp = sdp;
#ifdef DEBUG_LOCAL_P2P
            std::cout << mSdp << std::endl;
#endif
            // TODO:
            // 解析出candidates并保存，合并已保存的candidates到sdp中，因为有时stun
            // server无法到达从而会导致本次sdp缺失外网candidate
        }
        report();
    } catch (std::exception& excp) {
        logWarn << "webrtc gatherDone exception: " << excp.what();
    } catch (...) {
        logWarn << "webrtc gatherDone unknown exception";
    }
}

void WebRtcManager::report()
{
    try {
        json msg;
        msg["infos"] = json::array();
        json info;
        info["protocol"] = webRtcConnProtocol;
        info["connMeta"] = mSdp;
        msg["infos"].push_back(info);
        mMan->AsyncApiPostWithToken(nullptr, "/api/v1/report_net_info", msg, this, nullptr, nullptr);
    } catch (std::exception& excp) {
        logWarn << "webrtc report exception: " << excp.what();
    } catch (...) {
        logWarn << "webrtc report unknown exception";
    }
}

NS_END
