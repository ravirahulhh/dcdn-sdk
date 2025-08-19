#ifndef _DCDN_UTIL_HEX_H_
#define _DCDN_UTIL_HEX_H_

#include <string>
#include "common/Common.h"

NS_BEGIN(dcdn)
NS_BEGIN(util)

inline std::string Bytes2Hex(const void* dat, size_t len)
{
    std::string s;
    s.resize(len<<1);
    const unsigned char* p = static_cast<const unsigned char*>(dat);
    for (size_t i = 0; i < len; ++i, ++p) {
        s[i<<1]     = "0123456789abcdef"[(*p)>>4];
        s[(i<<1)+1] = "0123456789abcdef"[(*p)&0xf];
    }
    return s;
}

NS_END
NS_END

#endif
