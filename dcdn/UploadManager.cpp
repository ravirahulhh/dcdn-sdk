#include "UploadManager.h"

#include "MainManager.h"

NS_BEGIN(dcdn)

UploadManager::UploadManager(MainManager* man): BaseManager(man)
{
    registerHandler(EventType::UploadMsg, &UploadManager::handleUploadMsgEvent);
}

void UploadManager::run()
{
    logInfo << "Upload running";
    while (true) {
        waitAllEvents(std::chrono::milliseconds(1000));
    }
    logInfo << "Upload exit";
}

void UploadManager::handleUploadMsgEvent(std::shared_ptr<Event> evt) {}

NS_END
