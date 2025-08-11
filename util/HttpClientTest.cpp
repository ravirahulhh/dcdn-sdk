#include "HttpClient.h"

#include <iostream>

int main(int argc, char* argv[])
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
    dcdn::util::HttpClient cli;
    std::string resp;
    std::string body;
    const char* url = "https://www.baidu.com";
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--follow") == 0) {
            long v = atol(argv[++i]);
            cli.SetFollowLocation(v);
        } else if (strcmp(argv[i], "--ua") == 0) {
            cli.SetUserAgent(argv[++i]);
        } else if (strcmp(argv[i], "--json") == 0) {
            body = argv[++i];
        } else {
            url = argv[i];
        }
    }
    long code = 0;
    if (body.empty()) {
        code = cli.Get(url, resp);
    } else {
        code = cli.Post(url, body, resp, "application/json");
    }
    std::cout << "resp:" << resp << std::endl;
    std::cout << "code:" << code << std::endl;
    std::cout << "leng:" << resp.size() << std::endl;
    return 0;
}
