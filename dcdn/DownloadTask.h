#ifndef _DCDN_SDK_DOWNLOAD_TASK_H_
#define _DCDN_SDK_DOWNLOAD_TASK_H_

#include <util/HttpClient.h>

#include "BaseManager.h"

NS_BEGIN(dcdn)

struct DownloadOption
{
    std::string Url;
    std::string FileHash;
    uint64_t Start;
};

class DownloadTask: public BaseManager
{
public:
    DownloadTask(MainManager* man);
    int Init(const DownloadOption& opt);

private:
    void run();

private:
    DownloadOption mOpt;

    util::HttpClient mClient;
};

NS_END
#endif
