#ifndef _DCDN_UTIL_BLAKE2S_HASH_H
#define _DCDN_UTIL_BLAKE2S_HASH_H

#include <vector>
#include <openssl/evp.h>
#include "common/Common.h"

NS_BEGIN(dcdn)
NS_BEGIN(util)

class Blake2sHash
{
public:
    Blake2sHash():mCtx(nullptr)
    {
        init();
    }

    Blake2sHash(Blake2sHash&& oth):mCtx(oth.mCtx)
    {
        oth.mCtx = nullptr;
    }

    Blake2sHash& operator=(Blake2sHash&& oth)
    {
        Clear();
        mCtx = oth.mCtx;
        oth.mCtx = nullptr;
        return *this;
    }

    Blake2sHash(const Blake2sHash& oth) = delete;
    Blake2sHash& operator=(const Blake2sHash&) = delete;

    ~Blake2sHash()
    {
        Clear();
    }

    void Clear()
    {
        if (mCtx) {
            EVP_MD_CTX_free(mCtx);
            mCtx = nullptr;
        }
    }

    void Reset()
    {
        int ret = 0;
        if (mCtx) {
            int ret = EVP_MD_CTX_reset(mCtx);
            if (ret == 1) {
                ret = init();
            }
        }
        if (ret != 1) {
            Clear();
            init();
        }
    }

    int Update(const void* data, size_t len)
    {
        if (mCtx) {
            int ret = EVP_DigestUpdate(mCtx, data, len);
            if (ret != 1) {
                Clear();
                return -1;
            }
            return 1;
        }
        return 0;
    }

    int Final(std::vector<unsigned char>& hash) 
    {
        if (mCtx) {
            hash.resize(32);
            unsigned int hashLen = 0;
            int ret = EVP_DigestFinal_ex(mCtx, hash.data(), &hashLen);
            if (ret != 1) {
                Clear();
                hash.resize(0);
                return -1;
            }
            return 1;
        }
        hash.resize(0);
        return 0;
    }

    static int Calc(std::vector<unsigned char>& hash, const void* data, size_t len)
    {
        Blake2sHash h;
        int ret = h.Update(data, len);
        if (ret == 1) {
            ret = h.Final(hash);
        }
        return ret;
    }
private:
    int init()
    {
        if (!mCtx) {
            mCtx = EVP_MD_CTX_new();
        }
        if (mCtx) {
            int ret = EVP_DigestInit_ex(mCtx, EVP_blake2s256(), nullptr);
            if (ret != 1) {
                EVP_MD_CTX_free(mCtx);
                mCtx = nullptr;
                return -1;
            }
            return 1;
        }
        return 0;
    }
private:
    EVP_MD_CTX* mCtx;
};

NS_END
NS_END

#endif
