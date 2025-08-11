#ifndef _DCDN_SDK_BASE_MANAGER_H_
#define _DCDN_SDK_BASE_MANAGER_H_

#include <memory>
#include <thread>

#include "common/Common.h"

NS_BEGIN(dcdn)

class MainManager;

class BaseManager
{
public:
    BaseManager(MainManager* man): mMan(man) {}
    virtual ~BaseManager() {}
    void Start(bool detach = true)
    {
        mThread = std::make_shared<std::thread>([&]() { run(); });
        if (detach) {
            mThread->detach();
        }
    }
    std::shared_ptr<std::thread> Thread()
    {
        return mThread;
    }

protected:
    virtual void run() = 0;

protected:
    std::shared_ptr<std::thread> mThread;
    MainManager* mMan;
};

NS_END

#endif
