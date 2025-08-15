#ifndef _DCDN_SDK_TYPES_H_
#define _DCDN_SDK_TYPES_H_

#include <string>

#include "common/Common.h"

NS_BEGIN(dcdn)

struct BlockInfo
{
    uint64_t Start;
    uint64_t End;
    std::string Hash;
};

NS_END

#endif