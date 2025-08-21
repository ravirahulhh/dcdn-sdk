#include <plog/Appenders/ConsoleAppender.h>
#include <plog/Initializers/RollingFileInitializer.h>
#include <plog/Log.h>

#include <chrono>
#include <iostream>
#include <thread>

#include "DownloadManagerRefactor.h"
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
    // std::string url = "https://hil-speed.hetzner.com/100MB.bin";
    // std::string url = "http://localhost:8080/100MB.bin";
    // std::string url = "https://ash-speed.hetzner.com/1GB.bin";
    // std::string url = "https://d1.xia12345.com/video/202310/6524242c37926f1bd8c374d8/hd.mp4";
    dcdn::FileDownloadOptions opt;
    opt.OutputPath = "chunk.bin";
    opt.HasRange = true;

    // opt.RangeStart = 0;
    // opt.RangeEnd   = 262143999ULL;

    opt.RangeStart = 262144000;
    opt.RangeEnd   = 524287999ULL;
    opt.ChunkSize = 10 * 1024 * 1024;
    opt.WriteRangeToSeparateFile = true; // 默认即为 true

    // 订阅模式：状态/进度更新通知(注意要在添加任务前订阅，否则可能会丢失部分通知)
    auto sid = mgr->Subscribe([](const dcdn::DMEvent& ev) {
        if (auto* e = dynamic_cast<const dcdn::ETaskStatusChanged*>(&ev)) {
            // e->id, e->from, e->to
            std::cout << "ETaskStatusChanged from:" << static_cast<int>(e->from) << " to:" << static_cast<int>(e->to)
                      << "Task Id :" << e->id << std::endl;
        } else if (auto* p = dynamic_cast<const dcdn::ETaskProgress*>(&ev)) {
            // p->id, p->downloaded, p->total
            // std::cout << "ETaskProgress downloaded:" << p->downloaded << " total:" << p->total 
            //           << "Task Id :" << p->id << std::endl;
            if (p->downloaded == p->total) {
                std::cout << "下载完成" << std::endl;
            }
        }
    });
    //  取消订阅
    // std::this_thread::sleep_for(std::chrono::seconds(6));
    // std::cout << "cancel subscribe" << std::endl;
    // mgr->Unsubscribe(sid);

    auto taskId = mgr->AddDownloadTask(url, "", opt);
    std::cout << "任务已创建，taskId = " << taskId << std::endl;

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    // 轮训模式
    // bool firstPause = true;
    // while (true) {
    //     auto task = mgr->GetTaskStatus(taskId);

    //     double percent = 0.0;
    //     if (task.TotalSize > 0) {
    //         percent = (100.0 * task.Downloaded) / task.TotalSize;
    //     }

    //     std::cout << "进度: " << percent << "% "
    //               << "已下载: " << task.Downloaded << "/" << task.TotalSize << " bytes "
    //               << "速度: " << task.Speed / 1024.0 << " KB/s "
    //               << "状态: " << static_cast<int>(task.Status) << std::endl;

    //     if (task.Status == dcdn::TaskStatus::Completed || task.Status == dcdn::TaskStatus::Failed ||
    //         task.Status == dcdn::TaskStatus::Cancelled) {
    //         std::cout << "main: 任务结束" << std::endl;
    //         break;
    //     }

    //     // 取消示例
    //     // if (percent > 1){
    //     //    auto success = mgr.cancelDownloadTask(taskId);
    //     //    if (success) std::cout << "main: 取消任务成功" << std::endl;
    //     //    else std::cout << "main: 取消任务失败" << std::endl;
    //     // }
    //     //
    //     // 暂停/继续示例
    //     // if (percent > 5 && firstPause) {
    //     //    auto success = mgr.PauseDownloadTask(taskId);
    //     //    if (success) std::cout << "main: 暂停任务成功" << std::endl;
    //     //    else std::cout << "main: 暂停任务失败" << std::endl;
    //     //    std::this_thread::sleep_for(std::chrono::seconds(1));
    //     //    std::cout << "main: 继续任务" << std::endl;
    //     //    success = mgr.ResumeDownloadTask(taskId);
    //     //    if (success) std::cout << "main: 继续任务成功" << std::endl;
    //     //    else std::cout << "main: 继续任务失败" << std::endl;
    //     //    firstPause = false;
    //     // }

    //     std::this_thread::sleep_for(std::chrono::seconds(1));
    // }
    std::cout << "Main exit" << std::endl;
    return 0;
}
