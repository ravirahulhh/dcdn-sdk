#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include "dcdn.h"

void help()
{
    fprintf(stderr, "download <url>\n");
    fprintf(stderr, "info <taskId>\n");
    fprintf(stderr, "cancel <taskId>\n");
    fprintf(stderr, "pause <taskId>\n");
    fprintf(stderr, "resume <taskId>\n");
}

#define HasPrefix(s, prefix) (strncmp(s, prefix, strlen(prefix)) == 0)

void trimTail(char* s)
{
    char* p = s;
    while (p && *p) {
        ++p;
    }
    if (p != s) {
        --p;
        while (s <= p && isspace(*p)) {
            *p-- = 0;
        }
    }
}

void downloadCB(uint64_t taskId, void* userData)
{
}

int main(int argc, char* argv[])
{
    DcdnInitOption opt;
    int ret = DcdnInit(&opt);
    if (ret != DcdnErrorCodeOk) {
        fprintf(stderr, "DcdnInit fail\n");
        return 1;
    }
    char line[4096];
    puts("input help then enter for help");
    putchar('>');
    int fid = 1;
    while (1) {
        if (!fgets(line, sizeof(line), stdin)) {
            break;
        }
        line[sizeof(line)-1] = 0;
        trimTail(line);
        if (strcmp(line, "help") == 0) {
            help();
        } else if (HasPrefix(line, "download ")) {
            DcdnDownloadTaskOption dopt;
            dopt.Url = line + strlen("download ");
            char path[128];
            snprintf(path, sizeof(path), "f%04d.dat", fid++);
            dopt.Offset = 0;
            dopt.DisableP2P = 0;
            dopt.Callback = downloadCB;
            dopt.UserData = NULL;
            uint64_t taskId = 0;
            int ret = DcdnCreateDownloadTask(&taskId, &dopt);
            if (ret == DcdnErrorCodeOk) {
                printf("create download task:%llu\n", (unsigned long long)taskId);
            } else {
                printf("create download task error:%d\n", ret);
            }
        } else if (HasPrefix(line, "info ")) {
            DcdnTaskInfo info;
            info.TaskId = atoll(line + strlen("info "));
            int ret = DcdnGetTaskInfo(&info);
            if (ret == DcdnErrorCodeOk) {
                if (DcdnTaskStatusIsEnd(info.Status)) {
                    DcdnRemoveTask(info.TaskId);
                }
            } else {
            }
        } else {
            printf("unknown command:%s\n", line);
        }

        putchar('>');
    }
    return 0;
}
