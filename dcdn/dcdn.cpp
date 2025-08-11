#include "dcdn.h"

#include "MainManager.h"

extern "C" {

int DcdnInit(const DcdnInitOption* opt)
{
    using namespace dcdn;
    MainManagerOption mopt;
    int ret = MainManager::Init(mopt);
    return ret;
}

void DcdnUpdateNetworkInfo(const DcdnNetworkInfo* info)
{
    // TODO
}

int DcdnCreateDownloadTask(uint64_t* taskId, const DcdnDownloadTaskOption* opt)
{
    // TODO
    return DcdnErrorCodeErr;
}

int DcdnCreateStreamTask(uint64_t* taskId, const DcdnStreamTaskOption* opt)
{
    // TODO
    return DcdnErrorCodeErr;
}

int DcdnTaskStatusIsEnd(DcdnTaskStatus st)
{
    return st < DcdnTaskIdle || st >= DcdnTaskCompleted;
}

int DcdnCancelTask(uint64_t taskId)
{
    // TODO
    return DcdnErrorCodeErr;
}

int DcdnPauseTask(uint64_t taskId)
{
    // TODO
    return DcdnErrorCodeErr;
}

int DcdnResumeTask(uint64_t taskId)
{
    // TODO
    return DcdnErrorCodeErr;
}

int DcdnRemoveTask(uint64_t taskId)
{
    // TODO
    return DcdnErrorCodeErr;
}

int DcdnGetTaskInfo(DcdnTaskInfo* info)
{
    // TODO
    return DcdnErrorCodeErr;
}

int DcdnReadTaskData(size_t taskId, DcdnReadTaskDataHandle handle, void* userData)
{
    // TODO
    return DcdnErrorCodeErr;
}

int DcdnUpdateUploadDir(const DcdnUploadDir* d)
{
    // TODO
    return DcdnErrorCodeErr;
}

int DcdnRemoveUploadDir(const char* path)
{
    // TODO
    return DcdnErrorCodeErr;
}

int DcdnSetMaxUploadSpeed(uint64_t bytesPerSec)
{
    // TODO
    return DcdnErrorCodeErr;
}
}
