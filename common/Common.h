#ifndef _DCDN_COMMON_COMMON_H_
#define _DCDN_COMMON_COMMON_H_

#include "Config.h"
#include "Logger.h"
#include "Types.h"
#include "Version.h"

struct BlockInfo
{
    uint64_t start;
    uint64_t end;
    std::string hash;
};

#endif
