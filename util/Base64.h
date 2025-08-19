#ifndef _DCDN_UTIL_BASE64_H_
#define _DCDN_UTIL_BASE64_H_

#include <string>
#include "common/Common.h"

NS_BEGIN(dcdn)
NS_BEGIN(util)

class Base64
{
public:
    static std::string Encode(const void* dat, size_t len)
    {
        const unsigned char* p = static_cast<const unsigned char*>(dat);
        std::string s;
        s.reserve((len * 8 / 6) + ((len % 3) == 0 ? 0 : 1));
        s.resize(0);
        unsigned v = 0;
        unsigned bits = 0;
        for (size_t i = 0; i < len; ++i, ++p) {
            v = (v<<8) | *p;
            bits += 8;
            while (bits >= 6) {
                s += charSet[(v>>(bits-6)) & 0x3f];
                bits -= 6;
            }
        }
        if (bits > 0) {
            s += charSet[(v<<(6-bits)) & 0x3f];
        }
        return s;
    }
    static int Decode(std::vector<unsigned char>& dat, const std::string& s)
    {
        dat.reserve(s.size() * 6 / 8);
        dat.resize(0);
        int v = 0;
        unsigned bits = 0;
        for (auto& c : s) {
            int idx = charIdx[unsigned(c)];
            if (idx < 0) {
                return -1;
            }
            v = (v<<6) | idx;
            bits += 6;
            if (bits >= 8) {
                dat.push_back(v>>(bits-8));
                bits -= 8;
            }
        }
        return 1;
    }
private:
    constexpr static char charSet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    constexpr static int  charIdx[] = {
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
        -1,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, 63,
        -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
};

NS_END
NS_END

#endif
