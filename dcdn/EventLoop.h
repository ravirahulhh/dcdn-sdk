#ifndef _DCDN_SDK_EVENT_LOOP_H_
#define _DCDN_SDK_EVENT_LOOP_H_

#include <chrono>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "Event.h"

NS_BEGIN(dcdn)

class BaseEventLoop
{
public:
    typedef void (*GlobalHandler)(std::shared_ptr<Event> evt, void* userData);
    void RegisterGlobalHandler(int etype, GlobalHandler hdlr, void* userData);

protected:
    void globalHandle(std::shared_ptr<Event> evt);
};

template<class T>
class EventLoop: public BaseEventLoop
{
public:
    typedef void (T::*Handler)(std::shared_ptr<Event> evt);
    EventLoop() {}
    void PostEvent(std::shared_ptr<Event> evt)
    {
        std::unique_lock lck(mMtx);
        mEvents.push_back(evt);
        mCv.notify_all();
    }

protected:
    void registerHandler(int etype, Handler hdlr)
    {
        mHandlers[etype] = hdlr;
    }
    void waitEvent(std::chrono::milliseconds duration)
    {
        auto evt = takeEvent(duration);
        if (evt) {
            handle(evt);
        }
    }
    void waitAllEvents(std::chrono::milliseconds duration)
    {
        auto evts = takeAllEvents(duration);
        while (!evts.empty()) {
            auto evt = evts.front();
            evts.pop_front();
            handle(evt);
        }
    }
    std::shared_ptr<Event> takeEvent(std::chrono::milliseconds duration)
    {
        std::unique_lock lck(mMtx);
        if (duration.count() == 0) {
            while (mEvents.empty()) {
                mCv.wait(lck);
            }
        } else {
            mCv.wait_for(lck, duration);
        }
        if (mEvents.empty()) {
            return nullptr;
        }
        auto evt = mEvents.front();
        mEvents.pop_front();
        return evt;
    }
    std::list<std::shared_ptr<Event>> takeAllEvents(std::chrono::milliseconds duration)
    {
        std::unique_lock lck(mMtx);
        if (duration.count() == 0) {
            while (mEvents.empty()) {
                mCv.wait(lck);
            }
        } else {
            mCv.wait_for(lck, duration);
        }
        auto evts = std::move(mEvents);
        return evts;
    }

    void handle(std::shared_ptr<Event> evt)
    {
        auto it = mHandlers.find(evt->Type());
        if (it != mHandlers.end()) {
            try {
                (static_cast<T*>(this)->*(it->second))(evt);
            } catch (std::exception& excp) {
                logWarn << "handle event:" << evt->Type() << " exception:" << excp.what();
            } catch (...) {
                logWarn << "handle event:" << evt->Type() << " unknown exception";
            }
        } else {
            globalHandle(evt);
        }
    }

private:
    std::mutex mMtx;
    std::condition_variable mCv;
    std::list<std::shared_ptr<Event>> mEvents;

    std::unordered_map<int, Handler> mHandlers;
};

NS_END

#endif
