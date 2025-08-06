#include <string>
#ifndef _DCDN_SDK_COMMON_H_
#define _DCDN_SDK_COMMON_H_

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
