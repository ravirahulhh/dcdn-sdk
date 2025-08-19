#include <iostream>
#include "Blake2sHash.h"
#include "Hex.h"

int main(int argc, char* argv[])
{
    using namespace dcdn::util;
    const char* s = (argc > 1 ? argv[1] : "");
    std::vector<unsigned char> hash;
    int ret = Blake2sHash::Calc(hash, s, strlen(s));
    if (ret != 1) {
        std::cout << "Blake2sHash Calc fail ret:" << ret << std::endl;
        return 1;
    }
    std::cout << s << " hash:" << Bytes2Hex(hash.data(), hash.size()) << std::endl;
    if (strlen(s) == 0) {
        if ((Bytes2Hex(hash.data(), hash.size()) == "69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9")) {
            std::cout << "Correct" << std::endl;
        } else {
            std::cout << "Incorrect" << std::endl;
        }
    }
    return 0;
}
