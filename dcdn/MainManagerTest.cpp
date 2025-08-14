#include "MainManager.h"

#include <iostream>

#include "DeployManager.h"
#include "dcdn.h"

int main(int argc, char* argv[])
{
    dcdn::MainManagerOption opt;
    opt.WorkDir = "./data";
    opt.ApiKey = "123456";
    opt.DeviceId = "device123";
    opt.DeviceInfo = DcdnDeviceInfo{
        const_cast<char*>("device123"), const_cast<char*>("Linux"), const_cast<char*>("Linux"), 8, 16384};
    opt.DiskInfo = DcdnDiskInfo{1000000000, 800000000, 200000000};
    int ret = dcdn::MainManager::Init(opt);
    if (ret != 1) {
        logError << "Init MainManager fail";
        return 1;
    }
    dcdn::MainManager* m = dcdn::MainManager::Singlet();
    logInfo << "Start MainManager";
    m->Start();

    // deploy test
    std::shared_ptr<dcdn::DeployManager> deployMgr =
        std::dynamic_pointer_cast<dcdn::DeployManager>(m->GetDeployManager());
    auto deployArg = dcdn::DeployMsgArg{
        .jobId = "1234",
        .url = "https://d1.xia12345.com/video/202310/6524242c37926f1bd8c374d8/hd.mp4",
    };

    auto evt = std::make_shared<dcdn::ArgEvent<dcdn::DeployMsgArg>>(dcdn::EventType::DeployMsg, std::move(deployArg));
    deployMgr->PostEvent(evt);

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return 0;
}
