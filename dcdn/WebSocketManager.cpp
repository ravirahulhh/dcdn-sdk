#include "WebSocketManager.h"

#include <chrono>
#include <cstdlib>
#include <iostream>

#include "MainManager.h"

using namespace std::chrono_literals;
using json = nlohmann::json;

NS_BEGIN(dcdn)

struct WebSktCmdType
{
    enum Type
    {
        None = 0,
        Ping = 1,
        Pong = 2,
        QueryClientInfo = 5,
        ReportClientInfo = 6,

        PushConnMeta = 11,

        DispatchConfig = 50,
        DeployFile = 51,
        DeleteFile = 52,

        Ack = 103,
    };
};

WebSocketManager::WebSocketManager(MainManager* man): BaseManager(man)
{
    registerHandler(EventType::AckMsg, &WebSocketManager::handleAckMsgEvent);
}

void WebSocketManager::run()
{
    logInfo << "WebSocket running";
    auto connectTime = std::chrono::steady_clock::now();
    while (true) {
        switch (mStatus) {
            case Idle:
                connectTime = std::chrono::steady_clock::now();
                connect();
                break;
            case Connecting:
                if (std::chrono::steady_clock::now() - connectTime >=
                    std::chrono::seconds(mMan->Cfg().WebSktConnectTimeout())) {
                    mStatus = Closed; // timeout
                } else {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                break;
            case Connected:
                if (!mWebSkt || !mWebSkt->isOpen()) {
                    mStatus = Closed;
                } else {
                    waitEvent(std::chrono::milliseconds(1000));
                }
                break;
            case Closed: {
                mWebSkt = nullptr;
                mStatus = Idle;
                int secs = rand() % 30 + 1;
                std::this_thread::sleep_for(std::chrono::seconds(secs));
            } break;
        }
    }
    logInfo << "WebSocket exit";
}

void WebSocketManager::connect()
{
    try {
        rtc::WebSocket::Configuration config;
        config.disableTlsVerification = mMan->Cfg().WebSktDisableTlsVerification();
        auto ws = std::make_shared<rtc::WebSocket>(std::move(config));
        ws->onOpen([&]() {
            logInfo << "websocket opened";
            mStatus = Connected;
        });
        ws->onError([&](std::string error) {
            logInfo << "websocket error: " << error;
            mStatus = Closed;
        });
        ws->onClosed([&]() {
            logInfo << "websocket closed";
            mStatus = Closed;
        });
        ws->onMessage([&](std::variant<rtc::binary, std::string> message) { handleRecvMsg(message); });
        mStatus = Connecting;
        mWebSkt = ws;
        std::string url = mMan->Cfg().WebSktUrl();
        url += "/ws";
        logInfo << "websocket try to connect: " << url;
        mWebSkt->open(url);
    } catch (std::exception& excp) {
        logWarn << "websocket exception: " << excp.what();
        mStatus = Closed;
    } catch (...) {
        logWarn << "websocket unknown exception";
        mStatus = Closed;
    }
}

void WebSocketManager::handleRecvMsg(std::variant<rtc::binary, std::string>& message)
{
    try {
        if (!std::holds_alternative<std::string>(message)) {
            return;
        }
        std::string str = std::move(std::get<std::string>(message));
        json msg = json::parse(str);
        int msgType = msg["type"];
        int etype = EventType::None;
        switch (msgType) {
            case WebSktCmdType::PushConnMeta:
                etype = EventType::UploadMsg;
                break;
            case WebSktCmdType::DeployFile:
                etype = EventType::DeployMsg;
                break;
            default:
                break;
        }
        if (etype != EventType::None) {
            auto evt = std::make_shared<ArgEvent<json>>(etype, std::move(msg["payload"]));
            mMan->PostEvent(evt);
        }
        try {
            auto it = msg.find("cmd_id");
            if (it != msg.end()) {
                std::string cmdid = *it;
                if (!cmdid.empty()) {
                    auto evt = std::make_shared<ArgEvent<std::string>>(EventType::AckMsg, std::move(cmdid));
                    PostEvent(evt);
                }
            }
        } catch (...) {
        }
    } catch (std::exception& excp) {
        logWarn << "websocket handle msg exception: " << excp.what();
    } catch (...) {
        logWarn << "websocket handle msg unknown exception";
    }
}

void WebSocketManager::handleAckMsgEvent(std::shared_ptr<Event> evt)
{
    if (!mWebSkt) {
        return;
    }
    try {
        auto e = static_cast<ArgEvent<std::string>*>(evt.get());
        json msg;
        msg["type"] = int(WebSktCmdType::Ack);
        msg["cmd_id"] = e->Arg();
        mWebSkt->send(msg.dump());
    } catch (...) {
    }
}

NS_END
