#include <iostream>
#include "Base64.h"


int main(int argc, char* argv[])
{
    using namespace dcdn::util;
    for (int i = 1; i < argc; ++i) {
        auto e = Base64::Encode(argv[i], strlen(argv[i]));
        std::cout << argv[i] << "\nEncode: " << e << std::endl;
        std::vector<unsigned char> d;
        int ret = Base64::Decode(d, e);
        if (ret != 1) {
            std::cout << "Deocde Error" << std::endl;
        } else {
            std::vector<unsigned char> s;
            s.assign(argv[i], argv[i] + strlen(argv[i]));
            std::cout << "Decode " << (d == s ? "Succ" : "Fail") << std::endl;
        }
    }
    return 0;
}
