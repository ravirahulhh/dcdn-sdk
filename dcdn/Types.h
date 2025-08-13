#ifndef _DCDN_SDK_TYPES_H_
#define _DCDN_SDK_TYPES_H_

#include <string>

#include "common/Common.h"

NS_BEGIN(dcdn)

struct BlockInfo
{
    uint64_t start;
    uint64_t end;
    std::string hash;
};

NS_END

#endif