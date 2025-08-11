#include "DownloadManager.h"

#include "MainManager.h"

NS_BEGIN(dcdn)

DownloadManager::DownloadManager(MainManager* man): BaseManager(man)
{
    registerHandler(EventType::DeployMsg, &DownloadManager::handleDeployMsgEvent);
}

void DownloadManager::run()
{
    logInfo << "Download running";
    while (true) {
        waitAllEvents(std::chrono::milliseconds(1000));
    }
    logInfo << "Download exit";
}

void DownloadManager::handleDeployMsgEvent(std::shared_ptr<Event> evt) {}

NS_END
