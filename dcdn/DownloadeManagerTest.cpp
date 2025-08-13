#include <plog/Appenders/ConsoleAppender.h>
#include <plog/Initializers/RollingFileInitializer.h>
#include <plog/Log.h>

#include <chrono>
#include <iostream>
#include <thread>

#include "DownloadManager.h"

int main()
{
    static plog::ConsoleAppender<plog::TxtFormatter> consoleAppender;
    plog::Severity lvl = plog::debug;
    plog::init<DCDN_LOGGER_ID>(lvl, &consoleAppender);

    dcdn::DownloadManager mgr;

    // 配置为 HTTP_ONLY 策略，最大并发 4
    mgr.setStrategy(dcdn::DownloadStrategy::HTTP_ONLY);
    mgr.setMaxConcurrentDownloads(5);

    // 想下载的 HTTP 文件 URL
    // std::string url = "https://testfileorg.netwet.net/500MB-CZIPtestfile.org.zip";
    std::string url = "https://hil-speed.hetzner.com/1GB.bin";

    // 输出到本地 test.bin，内部并发分片大小 10MB (Preferred Chunk Size, 不一定严格遵守)
    // dcdn::FileDownloadOptions opts;
    // opts.outputPath = "chunk.bin";
    // opts.chunkSize = 10 *1024 * 1024;

    // 其他示例
    // 下载整个文件
    // dcdn::FileDownloadOptions opt;
    // opt.outputPath = "full.bin";
    // auto taskId = mgr.addDownloadTask(url, "", opt);

    // 下载区间 [start, end]，并把结果写成一个小文件（相对偏移）：
    dcdn::FileDownloadOptions opt;
    opt.outputPath = "chunk.bin";
    opt.hasRange = true;
    // opt.rangeStart = 0ULL;
    opt.rangeStart = 73741824ULL;
    opt.rangeEnd = 1073741823ULL;
    // opt.rangeEnd   = 73741823ULL;
    opt.chunkSize = 10 * 1024 * 1024;
    opt.writeRangeToSeparateFile = true; // 默认即为 true
    auto taskId = mgr.addDownloadTask(url, "", opt);

    // 下载区间 [start, EOF]（end 未知）：
    // FileDownloadOptions opt;
    // opt.outputPath = "tail.bin";
    // opt.hasRange = true;
    // opt.rangeStart = 100 * 1024 * 1024ULL;
    // opt.rangeEnd   = SIZE_MAX; // 未知
    // mgr.addDownloadTask(url, "", opt);

    // 添加下载任务
    std::cout << "任务已创建，taskId = " << taskId << std::endl;

    // 每秒打印一次任务状态，直到完成/失败
    bool firstPause = true;
    while (true) {
        auto task = mgr.getTaskStatus(taskId);

        double percent = 0.0;
        if (task.totalSize > 0) {
            percent = (100.0 * task.downloaded) / task.totalSize;
        }

        std::cout << "进度: " << percent << "% "
                  << "已下载: " << task.downloaded << "/" << task.totalSize << " bytes "
                  << "速度: " << task.speed / 1024.0 << " KB/s "
                  << "状态: " << static_cast<int>(task.status) << std::endl;

        if (task.status == dcdn::TaskStatus::Completed || task.status == dcdn::TaskStatus::Failed ||
            task.status == dcdn::TaskStatus::Cancelled) {
            std::cout << "main: 任务结束" << std::endl;
            break;
        }

        // 取消示例
        // if (percent > 1){
        //    auto success = mgr.cancelDownloadTask(taskId);
        //    if (success) std::cout << "main: 取消任务成功" << std::endl;
        //    else std::cout << "main: 取消任务失败" << std::endl;
        // }

        // 暂停/继续示例
        if (percent > 0.5 && firstPause) {
            auto success = mgr.pauseDownloadTask(taskId);
            if (success)
                std::cout << "main: 暂停任务成功" << std::endl;
            else
                std::cout << "main: 暂停任务失败" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            std::cout << "main: 继续任务" << std::endl;
            success = mgr.resumeDownloadTask(taskId);
            if (success)
                std::cout << "main: 继续任务成功" << std::endl;
            else
                std::cout << "main: 继续任务失败" << std::endl;
            firstPause = false;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    std::cout << "Main exit" << std::endl;
    return 0;
}
