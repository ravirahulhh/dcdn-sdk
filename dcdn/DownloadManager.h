#ifndef _DCDN_SDK_DOWNLOAD_MANAGER_H_
#define _DCDN_SDK_DOWNLOAD_MANAGER_H_

#include "BaseManager.h"
#include "EventLoop.h"

NS_BEGIN(dcdn)

class DownloadManager: public BaseManager, public EventLoop<DownloadManager>
{
public:
    DownloadManager(MainManager* man);

private:
    void run();
    void handleDeployMsgEvent(std::shared_ptr<Event> evt);
};

NS_END

#endif
