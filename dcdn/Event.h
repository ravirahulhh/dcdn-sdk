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
        AddFile = 10000,

        // UploadManager
        UploadMsg = 20000,

        // DownloadManager
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

struct AddFileArg
{
    std::string block_hash;
    std::string file_hash;
    std::string file_path;
    size_t file_size;
    size_t block_start;
    size_t block_end;
};

NS_END

#endif
