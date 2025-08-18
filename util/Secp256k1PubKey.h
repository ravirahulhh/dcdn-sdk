#ifndef _DCDN_UTIL_SECP256k1_PUB_KEY_H_
#define _DCDN_UTIL_SECP256k1_PUB_KEY_H_

#include <openssl/bn.h>
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/obj_mac.h>

#include <cstring>
#include <vector>

class Secp256k1PubKey
{
public:
    Secp256k1PubKey(): mKey(nullptr) {}
    Secp256k1PubKey(Secp256k1PubKey&& oth): mKey(oth.mKey)
    {
        oth.mKey = nullptr;
    }
    Secp256k1PubKey& operator=(Secp256k1PubKey&& oth)
    {
        Clear();
        mKey = oth.mKey;
        oth.mKey = nullptr;
    }
    ~Secp256k1PubKey()
    {
        Clear();
    }
    void Clear()
    {
        if (mKey) {
            EC_KEY_free(mKey);
        }
    }
    int Set(const unsigned char seckey[32])
    {
        getKey();
        if (!mKey) {
            return -1;
        }
        BIGNUM* priv = BN_bin2bn(seckey, 32, nullptr);
        if (!priv) {
            return -1;
        }

        if (EC_KEY_set_private_key(mKey, priv) != 1) {
            BN_free(priv);
            return -1;
        }

        const EC_GROUP* group = EC_KEY_get0_group(mKey);
        EC_POINT* pub = EC_POINT_new(group);
        if (!pub) {
            BN_free(priv);
            return -1;
        }
        if (EC_POINT_mul(group, pub, priv, nullptr, nullptr, nullptr) != 1) {
            BN_free(priv);
            EC_POINT_free(pub);
            return -1;
        }
        if (EC_KEY_set_public_key(mKey, pub) != 1) {
            BN_free(priv);
            EC_POINT_free(pub);
            return -1;
        }

        BN_free(priv);
        EC_POINT_free(pub);
        return 1;
    }

    int Generate(unsigned char seckey[32])
    {
        getKey();
        if (!mKey) {
            return -1;
        }
        if (EC_KEY_generate_key(mKey) != 1) {
            Clear();
            return -1;
        }

        const BIGNUM* priv = EC_KEY_get0_private_key(mKey);
        if (!priv) {
            return -1;
        }

        int bnLen = BN_num_bytes(priv);
        if (bnLen > 32) {
            return -1;
        }
        memset(seckey, 0, 32);
        BN_bn2bin(priv, seckey + (32 - bnLen));
        return 1;
    }

    int Serialize(std::vector<unsigned char>& pkey, bool compress)
    {
        if (!mKey) {
            return -1;
        }
        int len = i2o_ECPublicKey(mKey, nullptr);
        if (len <= 0) {
            return -1;
        }

        pkey.resize(len);
        unsigned char* p = pkey.data();
        if (i2o_ECPublicKey(mKey, &p) != len) {
            return -1;
        }
        return 1;
    }

    int Parse(const std::vector<unsigned char>& pkey)
    {
        getKey();
        if (!mKey) {
            return -1;
        }
        const unsigned char* p = pkey.data();
        EC_KEY* newKey = o2i_ECPublicKey(&mKey, &p, pkey.size());
        if (!newKey) {
            return -1;
        }
        return 1;
    }

    // r||s
    static int Sign(std::vector<unsigned char>& sig, const unsigned char seckey[32], const unsigned char msgHash[32])
    {
        EC_KEY* key = EC_KEY_new_by_curve_name(NID_secp256k1);
        if (!key) {
            return -1;
        }

        BIGNUM* priv = BN_bin2bn(seckey, 32, nullptr);
        if (!priv) {
            EC_KEY_free(key);
            return -1;
        }
        if (EC_KEY_set_private_key(key, priv) != 1) {
            BN_free(priv);
            EC_KEY_free(key);
            return -1;
        }

        ECDSA_SIG* signature = ECDSA_do_sign(msgHash, 32, key);
        if (!signature) {
            BN_free(priv);
            EC_KEY_free(key);
            return -1;
        }

        const BIGNUM* r;
        const BIGNUM* s;
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
        ECDSA_SIG_get0(signature, &r, &s);
#else
        r = signature->r;
        s = signature->s;
#endif

        sig.resize(64);
        BNToFixedBytes(r, sig.data());
        BNToFixedBytes(s, sig.data() + 32);

        ECDSA_SIG_free(signature);
        BN_free(priv);
        EC_KEY_free(key);
        return 1;
    }

    int Verify(const std::vector<unsigned char>& sig, const unsigned char msgHash[32])
    {
        if (!mKey || sig.size() != 64)
            return -1;

        BIGNUM* r = BytesToBN(sig.data());
        BIGNUM* s = BytesToBN(sig.data() + 32);
        ECDSA_SIG* signature = ECDSA_SIG_new();
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
        ECDSA_SIG_set0(signature, r, s);
#else
        signature->r = r;
        signature->s = s;
#endif

        int ret = ECDSA_do_verify(msgHash, 32, signature, mKey);
        ECDSA_SIG_free(signature);
        return (ret == 1) ? 1 : -1;
    }

    static void BNToFixedBytes(const BIGNUM* bn, unsigned char out[32])
    {
        memset(out, 0, 32);
        int bnLen = BN_num_bytes(bn);
        if (bnLen > 32) {
            return;
        }
        BN_bn2bin(bn, out + (32 - bnLen));
    }

    static BIGNUM* BytesToBN(const unsigned char in[32])
    {
        return BN_bin2bn(in, 32, nullptr);
    }

private:
    EC_KEY* getKey()
    {
        if (!mKey) {
            mKey = EC_KEY_new_by_curve_name(NID_secp256k1);
        }
        return mKey;
    }

private:
    EC_KEY* mKey;
};

#endif
