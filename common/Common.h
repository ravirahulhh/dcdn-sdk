#ifndef _DCDN_COMMON_COMMON_H_
#define _DCDN_COMMON_COMMON_H_

#include "Config.h"
#include "Logger.h"
#include "Types.h"
#include "Version.h"

struct BlockInfo
{
    std::string file_hash;
    uint64_t block_start;
    uint64_t block_end;
    std::string block_hash;
};
using Url = std::string;
using FileDescriptor = std::variant<BlockInfo, Url>;

#endif
