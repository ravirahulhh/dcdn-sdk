#ifndef _DCDN_SDK_H_
#define _DCDN_SDK_H_

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum _DcdnErrorCode
{
    DcdnErrorCodeUnknownTask = -2,
    DcdnErrorCodeErr = -1,
    DcdnErrorCodeUnknown = 0,
    DcdnErrorCodeOk = 1,
} DcdnErrorCode;

typedef struct _DcdnUploadDir
{
    const char* Path;
    size_t Capacity;
} DcdnUploadDir;

typedef struct _DcdnUploadOption
{
    DcdnUploadDir* UploadDirs; // NULL means disable
    unsigned UploadDirSize;
    size_t MaxUploadSpeed; // BytesPerSecond, 0:unlimit
} DcdnUploadOption;

typedef struct _DcdnDeviceInfo
{
    const char* Id;
    const char* CPU;
    uint64_t Memory;
    const char* OS;
} DcdnDeviceInfo;

typedef struct _DcdnInitOption
{
    const char* WorkDir;
    const char* ApiKey;
    DcdnDeviceInfo Device;
    DcdnUploadOption Upload;
} DcdnInitOption;

typedef struct _DcdnNetworkInfo
{
    const char* Ssid;
    double Longitude;
    double Latitude;
} DcdnNetworkInfo;

/********************************************
 *
 *
 *
 * ******************************************/
////////////// Global //////////////////////
int DcdnInit(const DcdnInitOption* opt);

void DcdnUpdateNetworkInfo(const DcdnNetworkInfo* info);

////////////// Downlaod //////////////////////
const char* DcdnGetHttpServer();

typedef struct _DcdnDownloadTaskOption
{
    const char* Url;
    const char* SavePath;
    size_t Offset;
    int DisableP2P;
    void (*Callback)(uint64_t taskId, void* userData);
    void* UserData;
} DcdnDownloadTaskOption;

typedef struct _DcdnStreamTaskOption
{
    const char* Url;
    size_t Offset;
    size_t MaxBuf;
    int DisableP2P;
    void (*Callback)(uint64_t taskId, void* userData);
    void* UserData;
} DcdnStreamTaskOption;

typedef enum _DcdnTaskStatus
{
    DcdnTaskCancelled = -2,
    DcdnTaskFail = -1,
    DcdnTaskIdle = 0,
    DcdnTaskRunning = 1,
    DcdnTaskPaused = 2,
    DcdnTaskCompleted = 3,
} DcdnTaskStatus;

int DcdnTaskStatusIsEnd(DcdnTaskStatus st);

typedef struct _DcdnTaskInfo
{
    uint64_t TaskId;
    DcdnTaskStatus Status;
    size_t ReceivedBytes;
    size_t ContentLength;
} DcdnTaskInfo;

int DcdnCreateDownloadTask(uint64_t* taskId, const DcdnDownloadTaskOption* opt);

int DcdnCreateStreamTask(uint64_t* taskId, const DcdnStreamTaskOption* opt);

int DcdnCancelTask(uint64_t taskId);

int DcdnPauseTask(uint64_t taskId);

int DcdnResumeTask(uint64_t taskId);

int DcdnRemoveTask(uint64_t taskId);

int DcdnGetTaskInfo(DcdnTaskInfo* info);

typedef void (*DcdnReadTaskDataHandle)(size_t offset, const unsigned char* dat, size_t len, void* userData);

// only for stream task
int DcdnReadTaskData(size_t taskId, DcdnReadTaskDataHandle handle, void* userData);

////////////// Uplaod //////////////////////
int DcdnUpdateUploadDir(const DcdnUploadDir* d);

int DcdnRemoveUploadDir(const char* path);

int DcdnSetMaxUploadSpeed(uint64_t bytesPerSec);

#ifdef __cplusplus
}
#endif

#endif
