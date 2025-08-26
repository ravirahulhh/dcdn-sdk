#include <plog/Appenders/ConsoleAppender.h>
#include <plog/Initializers/RollingFileInitializer.h>
#include <plog/Log.h>

#include <chrono>
#include <iostream>
#include <thread>

#include "DownloadManager.h"
#include "dcdn/MainManager.h"
#include "dcdn/WebRtcManager.h"

int main()
{
    static plog::ConsoleAppender<plog::TxtFormatter> consoleAppender;
    plog::Severity lvl = plog::debug;
    plog::init<DCDN_LOGGER_ID>(lvl, &consoleAppender);

    dcdn::MainManagerOption mOpts;
    mOpts.WorkDir = "./data";
    mOpts.ApiKey = "123456";
    mOpts.DeviceId = "device123";
    mOpts.DeviceInfo = DcdnDeviceInfo{
        const_cast<char*>("device123"), const_cast<char*>("Linux"), const_cast<char*>("Linux"), 8, 16384};
    mOpts.DiskInfo = DcdnDiskInfo{1000000000, 800000000, 200000000};
    mOpts.ServerCfg = DcdnServerCfg{
        "https://api-pcdn.capell.io", // api
        "https://api-pcdn.capell.io", // evt
        "wss://cmd-pcdn.capell.io" // cmd
    };
    int ret = dcdn::MainManager::Init(mOpts);
    if (ret != 1) {
        logError << "Init MainManager fail";
        return 1;
    }

    dcdn::MainManager* m = dcdn::MainManager::Singlet();
    logInfo << "Start MainManager";
    m->Start();
    std::this_thread::sleep_for(std::chrono::seconds(5)); // wait login

    const auto cp = static_cast<dcdn::WebRtcManager*>(dcdn::MainManager::Singlet()->GetWebRtcManager().get())->Cert();
    std::cout << "Main" << "keyPemFile" << cp.keyPem << "certPemFile" << cp.certPem << std::endl;
    auto mgr = std::dynamic_pointer_cast<dcdn::DownloadManager>(m->GetDownloadManager());

    mgr->SetMaxConcurrentDownloads(8);

    std::string url = "https://testfileorg.netwet.net/500MB-CZIPtestfile.org.zip";
    dcdn::FileDownloadOptions opt = dcdn::MakeDefaultOptions(dcdn::DownloadStrategy::Stream);
    opt.RangeStart = 0;

    auto sid = mgr->Subscribe([](const dcdn::DMEvent& ev) {
        if (auto* e = dynamic_cast<const dcdn::ETaskStatusChanged*>(&ev)) {
            std::cout << "ETaskStatusChanged from:" << static_cast<int>(e->from) << " to:" << static_cast<int>(e->to)
                      << "Task Id :" << e->id << std::endl;
        } 
    });
   
    opt.taskStateChangeEventCallback = [](const dcdn::DMEvent& ev) {
        if (auto* p = dynamic_cast<const dcdn::ETaskStatusChanged*>(&ev)) {
            std::cout << "task id :" << p->id << "status changed from:" << static_cast<int>(p->from) << " to:" << static_cast<int>(p->to) << std::endl;
            if (p->to == dcdn::TaskStatus::Completed) {
                std::cout << "下载完成" << std::endl;
            }
        }
    };
    opt.StreamReadyCb = [](dcdn::TaskId taskId, void* ){
        // std::cout << "stream cb [" << offset << "," << offset + size - 1 << "]" << std::endl;
    };

    auto taskId = mgr->AddDownloadTask(url, "", opt);
    std::cout << "任务已创建，taskId = " << taskId << std::endl;

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    std::cout << "Main exit" << std::endl;
    return 0;
}
