#include "Secp256k1PubKey.h"

#include <iostream>
#include <sstream>

template<class Iter>
std::string bytes2hex(Iter begin, Iter end)
{
    std::ostringstream oss;
    for (auto c = begin; c != end; ++c) {
        char buf[4];
        snprintf(buf, sizeof(buf), "%02x", *c);
        oss << buf;
    }
    return oss.str();
}

int main(int argc, char* argv[])
{
    Secp256k1PubKey pkey;
    unsigned char skey[32];
    memset(skey, 0, 32);
    skey[31] = 1;
    int ret = pkey.Set(skey);
    std::cout << "SetSkey ret:" << ret << std::endl;
    std::vector<unsigned char> data;
    ret = pkey.Serialize(data, true);
    std::cout << "Serialize ret:" << ret << std::endl;
    std::cout << bytes2hex(data.begin(), data.end()) << std::endl;
    ret = pkey.Parse(data);
    std::cout << "Parse ret:" << ret << std::endl;
    std::cout << "Generate" << std::endl;
    ret = pkey.Generate(skey);
    if (ret != 1) {
        std::cout << "Generate fail ret:" << ret << std::endl;
        return 1;
    }
    std::cout << "skey: 0x" << bytes2hex(skey, skey + 32) << std::endl;
    ret = pkey.Serialize(data, true);
    if (ret != 1) {
        std::cout << "Serialize ret:" << ret << std::endl;
        return 1;
    }
    std::cout << bytes2hex(data.begin(), data.end()) << std::endl;
    std::cout << "Sign" << std::endl;
    std::vector<unsigned char> sig;
    ret = Secp256k1PubKey::Sign(sig, skey, skey);
    if (ret != 1) {
        std::cout << "Sign fail ret:" << ret << std::endl;
        return 1;
    }
    std::cout << bytes2hex(sig.begin(), sig.end()) << std::endl;
    std::cout << "Verify" << std::endl;
    ret = pkey.Verify(sig, skey);
    if (ret != 1) {
        std::cout << "Verify fail ret:" << ret << std::endl;
        return 1;
    }
    std::cout << "Verify succ" << std::endl;
    return 0;
}
