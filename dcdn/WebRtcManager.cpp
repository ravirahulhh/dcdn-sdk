#include "WebRtcManager.h"

#include <chrono>

#include "MainManager.h"

NS_BEGIN(dcdn)

static const std::string webRtcConnProtocol = "ProtocolWebRTC";

static const std::string candidatePlaceholder = "CANDIDATE_PLACEHOLDER";
static const std::string candidatePrefix = "a=candidate:";

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
            parseCandidates(sdp);
#ifdef DEBUG_LOCAL_P2P
            std::cout << mSdp << std::endl;
#endif
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

void WebRtcManager::parseCandidates(const std::string& sdp)
{
    logDebug << "parseCandidates, sdp=" << sdp;
    std::vector<rtc::Candidate> candidates;
    std::istringstream sdpStream(sdp);
    std::string line;
    bool isPlaceholderInResult{false};
    std::ostringstream result;
    while (std::getline(sdpStream, line)) {
        if (line.empty()) {
            continue;
        }
        if (line.compare(0, candidatePrefix.size(), candidatePrefix) == 0) {
            logDebug << "candidate: " << line;
            candidates.push_back(rtc::Candidate(line));
            if (!isPlaceholderInResult) {
                result << candidatePlaceholder;
                isPlaceholderInResult = true;
            }
        } else {
            result << line << '\n';
        }
    }

    for (auto candidate : candidates) {
        auto it = std::find(mCandidates.begin(), mCandidates.end(), candidate);
        if (it != mCandidates.end()) {
            logDebug << "update candidate: " << candidate;
            // 比较了主要部分，如果剩余部分有变化，也会更新
            *it = candidate;
        } else {
            logDebug << "add candidate: " << candidate;
            mCandidates.push_back(candidate);
        }
    }

    std::ostringstream candidatesStr;
    for (const auto& candidate : mCandidates) {
        candidatesStr << candidate << "\r\n";
    }
    logDebug << "local candidate collect:" << candidatesStr.str();

    std::string resultStr = result.str();
    size_t placeholderPos = resultStr.find(candidatePlaceholder);
    if (placeholderPos != std::string::npos) {
        resultStr.replace(placeholderPos, candidatePlaceholder.size(), candidatesStr.str());
    }
    logDebug << "local placed sdp:" << resultStr;

    mSdp = resultStr;
}

NS_END
