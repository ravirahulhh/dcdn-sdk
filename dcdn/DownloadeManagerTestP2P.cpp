#include <plog/Appenders/ConsoleAppender.h>
#include <plog/Initializers/RollingFileInitializer.h>
#include <plog/Log.h>

#include <chrono>
#include <iostream>
#include <thread>

#include "DownloadManager.h"
#include "MainManager.h"
#include "WebRtcManager.h"

int main()
{
    static plog::ConsoleAppender<plog::TxtFormatter> consoleAppender;
    // plog::Severity lvl = plog::debug;
    // static plog::ConsoleAppender<plog::TxtFormatter> consoleAppender;
    plog::Severity lvl = plog::debug;
    // plog::init<DCDN_LOGGER_ID>(lvl, consoleAppender);
    plog::init<DCDN_LOGGER_ID>(lvl, &consoleAppender);
    dcdn::MainManagerOption opt;
    opt.WorkDir = "./data3";
    opt.ApiKey = "123456";
    opt.DeviceId = "device31221";
    opt.DeviceInfo = DcdnDeviceInfo{
        const_cast<char*>("device31221"), const_cast<char*>("Linux"), const_cast<char*>("Linux"), 8, 16384};
    opt.DiskInfo = DcdnDiskInfo{1000000000, 800000000, 200000000};
    opt.ServerCfg = DcdnServerCfg{
        "https://api-pcdn.capell.io", // api
        "https://api-pcdn.capell.io", // evt
        "wss://cmd-pcdn.capell.io" // cmd
    };
    int ret = dcdn::MainManager::Init(opt);
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

    // 配置为 HTTP_ONLY 策略，最大并发 4
    // mgr.SetStrategy(dcdn::DownloadStrategy::P2P_ONLY);
    mgr->SetMaxConcurrentDownloads(10);

    // 想下载的 HTTP 文件 URL
    // std::string url = "https://testfileorg.netwet.net/500MB-CZIPtestfile.org.zip";
    // std::string url = "http://localhost:8080/100MB.bin";
    // std::string url = "https://ash-speed.hetzner.com/1GB.bin";
    std::string url = "https://d1.xia12345.com/video/202310/6524242c37926f1bd8c374d8/hd.mp4";

    // 下载区间 [start, end]，并把结果写成一个小文件（相对偏移）：
    dcdn::FileDownloadOptions opts;
    opts.OutputPath = "chunk.bin";
    opts.HasRange = true;
    opts.RangeStart = 0;
    // opts.RangeEnd = 4164379-1;
    // opts.RangeEnd = 2097152 - 1;
    // opts.RangeEnd = 4164379-1;
    opts.WriteRangeToSeparateFile = true; // 默认即为 true
    opts.Strategy = dcdn::DownloadStrategy::P2P_ONLY;
    auto sid = mgr->Subscribe([](const dcdn::DMEvent& ev) {
        if (auto* e = dynamic_cast<const dcdn::ETaskStatusChanged*>(&ev)) {
            // e->id, e->from, e->to
            std::cout << "ETaskStatusChanged from:" << static_cast<int>(e->from) << " to:" << static_cast<int>(e->to)
                      << "Task Id :" << e->id << std::endl;
        } else if (auto* p = dynamic_cast<const dcdn::ETaskProgress*>(&ev)) {
            // p->id, p->downloaded, p->total
            std::cout << "ETaskProgress downloaded:" << p->downloaded << " total:" << p->total << "Task Id :" << p->id
                      << std::endl;
            if (p->downloaded == p->total) {
                std::cout << "下载完成" << std::endl;
            }
        }
    });
#ifdef DEBUG_LOCAL_P2P
    auto taskId = mgr->AddDownloadTask("", "cd4a7faf4ed9cd3486cb08ff1dcfd040", opts);
#else
    auto taskId = mgr->AddDownloadTask("", "AbjECUo74vz26ZAQdZkRYrXwYIT9", opts);
#endif

    // auto taskId = mgr->AddDownloadTask("", "AXKXNz-vQ9V2YRLZTZLen8aO1CgB", opts);
    // 添加下载任务
    std::cout << "任务已创建，taskId = " << taskId << std::endl;

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

    //         if (task.Status == dcdn::TaskStatus::Failed) {
    //             std::cout << "任务失败" << std::endl;
    //         }else if (task.Status == dcdn::TaskStatus::Cancelled) {
    //             std::cout << "任务取消" << std::endl;
    //         }else if (task.Status == dcdn::TaskStatus::Completed) {
    //             std::cout << "任务完成" << std::endl;
    //         }
    //         break;
    //     }

    // 取消示例
    // if (percent > 1){
    //    auto success = mgr.cancelDownloadTask(taskId);
    //    if (success) std::cout << "main: 取消任务成功" << std::endl;
    //    else std::cout << "main: 取消任务失败" << std::endl;
    // }
    //
    // 暂停/继续示例
    // if (percent > 5 && firstPause) {
    //    auto success = mgr.PauseDownloadTask(taskId);
    //    if (success) std::cout << "main: 暂停任务成功" << std::endl;
    //    else std::cout << "main: 暂停任务失败" << std::endl;
    //    std::this_thread::sleep_for(std::chrono::seconds(1));
    //    std::cout << "main: 继续任务" << std::endl;
    //    success = mgr.ResumeDownloadTask(taskId);
    //    if (success) std::cout << "main: 继续任务成功" << std::endl;
    //    else std::cout << "main: 继续任务失败" << std::endl;
    //    firstPause = false;
    // }

    // std::this_thread::sleep_for(std::chrono::seconds(1));
    // }
    std::cout << "Main exit" << std::endl;
    std::mutex lock;
    std::condition_variable cv;
    std::unique_lock<std::mutex> lck(lock); // <1>
    while (true) {
        cv.wait(lck);
    }
    return 0;
}
