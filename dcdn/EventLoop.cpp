#include "EventLoop.h"

#include <unordered_map>

NS_BEGIN(dcdn)

static std::unordered_map<int, std::pair<BaseEventLoop::GlobalHandler, void*>> globalHandlers;

void BaseEventLoop::RegisterGlobalHandler(int etype, GlobalHandler hdlr, void* userData)
{
    globalHandlers[etype] = {hdlr, userData};
}

void BaseEventLoop::globalHandle(std::shared_ptr<Event> evt)
{
    auto it = globalHandlers.find(evt->Type());
    if (it != globalHandlers.end()) {
        try {
            it->second.first(evt, it->second.second);
        } catch (std::exception& excp) {
            logWarn << "global handle event:" << evt->Type() << " exception:" << excp.what();
        } catch (...) {
            logWarn << "global handle event:" << evt->Type() << " unknown exception";
        }
    }
}

NS_END
