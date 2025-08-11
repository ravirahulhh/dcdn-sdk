#include "DownloadTask.h"

NS_BEGIN(dcdn)

DownloadTask::DownloadTask(MainManager* man): BaseManager(man) {}

int DownloadTask::Init(const DownloadOption& opt)
{
    mOpt = opt;
    return ErrorCodeOk;
}

void DownloadTask::run()
{
    while (true) {
    }
}

NS_END
