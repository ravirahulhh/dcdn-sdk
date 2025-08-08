#ifndef _DCDN_SDK_EVENT_H_
#define _DCDN_SDK_EVENT_H_

#include "Common.h"
#include "common/Common.h"
#include <string>
#include <variant>

NS_BEGIN(dcdn)

struct EventType {
  enum Type {
    None = 0,

    // FileManager
    AddFile = 10000,
    RemoveFile = 10001,
    FileDownloadDone = 10002,
    FileDownloadFailed = 10003,

    // UploadManager
    UploadMsg = 20000,

    // DownloadManager
    DeployMsg = 30000,

    // WebSocketManager
    AckMsg = 40000,
  };
};

class Event {
public:
  Event(int etype = EventType::None) : mType(etype) {}
  virtual ~Event() {}
  int Type() const { return mType; }

private:
  int mType;
};

template <class ArgType> class ArgEvent : public Event {
public:
  ArgEvent(int etype, ArgType &&arg) : Event(etype), mArg(arg) {}
  ArgType &Arg() { return mArg; }
  const ArgType &Arg() const { return mArg; }

private:
  ArgType mArg;
};

struct AddFileArg {
  std::string block_hash;
  std::string file_hash;
  std::string file_path;
  size_t file_size;
  size_t block_start;
  size_t block_end;
};

struct FileDownloadDoneArg : BlockInfo {
  std::string url;
  std::string file_path;
};

struct FileDownloadFailedArg {
  FileDescriptor file;
};

struct RemoveFileArg {
  std::string block_hash;
};

NS_END

#endif
