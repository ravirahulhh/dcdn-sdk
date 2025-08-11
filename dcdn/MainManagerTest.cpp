#include "MainManager.h"

#include <iostream>

int main(int argc, char* argv[])
{
    dcdn::MainManagerOption opt;
    opt.WorkDir = "data";
    int ret = dcdn::MainManager::Init(opt);
    if (ret != 1) {
        logError << "Init MainManager fail";
        return 1;
    }
    dcdn::MainManager* m = dcdn::MainManager::Singlet();
    logInfo << "Start MainManager";
    m->Start();
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return 0;
}
