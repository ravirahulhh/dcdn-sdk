#ifndef _DCDN_SDK_EVENT_H_
#define _DCDN_SDK_EVENT_H_

#include <string>

#include "common/Common.h"

NS_BEGIN(dcdn)

struct EventType
{
    enum Type
    {
        None = 0,

        AsyncApiRequest = 1000,

        // FileManager
        FileDownloadDone = 10000,
        FileDownloadFailed = 10001,
        RemoveFile = 10002,

        // UploadManager
        UploadMsg = 20000,

        // DeployManager
        DeployMsg = 30000,

        // WebSocketManager
        AckMsg = 40000,
    };
};

class Event
{
public:
    Event(int etype = EventType::None): mType(etype) {}
    virtual ~Event() {}
    int Type() const
    {
        return mType;
    }

private:
    int mType;
};

template<class ArgType>
class ArgEvent: public Event
{
public:
    ArgEvent(int etype, ArgType&& arg): Event(etype), mArg(arg) {}
    ArgType& Arg()
    {
        return mArg;
    }
    const ArgType& Arg() const
    {
        return mArg;
    }

private:
    ArgType mArg;
};

struct FileDownloadDoneArg: BlockInfo
{
    std::string url;
    std::string file_path;
};

struct FileDownloadFailedArg
{
    std::string file_path;
};

struct RemoveFileArg
{
    std::string block_hash;
};

struct DeployMsgArg : BlockInfo
{
    std::string job_id;  // 部署任务唯一标识
    std::string url;     // 下载URL
};
NS_END

#endif
