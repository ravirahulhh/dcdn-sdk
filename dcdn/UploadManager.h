#ifndef _DCDN_SDK_UPLOAD_MANAGER_H_
#define _DCDN_SDK_UPLOAD_MANAGER_H_

#include "BaseManager.h"
#include "EventLoop.h"

NS_BEGIN(dcdn)

class UploadManager: public BaseManager, public EventLoop<UploadManager>
{
public:
    UploadManager(MainManager* man);

private:
    void run();
    void handleUploadMsgEvent(std::shared_ptr<Event> evt);
};

NS_END

#endif
