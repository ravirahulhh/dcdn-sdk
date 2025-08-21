#include "MainManager.h"

#include <signal.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "DeployManager.h"
#include "dcdn.h"

#ifdef HAVE_GPERFTOOLS
#include <gperftools/profiler.h>
#endif

void signal_handler(int signal)
{
#ifdef HAVE_GPERFTOOLS
    ProfilerStop();
    std::cout << "CPU profiler stopped due to signal" << std::endl;
#endif
    exit(signal);
}

int main(int argc, char* argv[])
{
    // 注册信号处理器
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

#ifdef HAVE_GPERFTOOLS
    // 启动CPU profiler
    ProfilerStart("cpu.prof");
    std::cout << "CPU profiler started, output to cpu.prof" << std::endl;
#endif

    // 先执行CPU密集型操作，避免SSL初始化的干扰
    std::cout << "Starting CPU intensive operations..." << std::endl;

    // 运行一段时间并执行CPU密集型操作
    volatile long long sum = 0;
    volatile long long factorial = 1;

    for (int round = 0; round < 5; round++) {
        std::cout << "Round " << (round + 1) << "/5" << std::endl;

        // 密集计算1：大量乘法运算
        for (int i = 1; i < 100000; i++) {
            sum += i * i * i;
            if (i % 1000 == 0) {
                factorial = factorial * (i % 100 + 1);
            }
        }

        // 密集计算2：字符串操作
        for (int j = 0; j < 10000; j++) {
            std::string temp = "test_string_" + std::to_string(j);
            temp.append("_suffix_");
            temp += std::to_string(sum % 1000);
            volatile size_t len = temp.length();
            if (len > 50) {
                temp = temp.substr(0, 30);
            }
        }

        // 密集计算3：简单排序
        std::vector<int> nums;
        for (int k = 0; k < 5000; k++) {
            nums.push_back(rand() % 10000);
        }
        std::sort(nums.begin(), nums.end());
        sum += nums[nums.size() / 2]; // 使用中位数

        std::cout << "Round " << (round + 1) << " completed, sum=" << sum << std::endl;
    }

    // 现在初始化系统组件（这可能涉及SSL调用）
    std::cout << "Initializing system components..." << std::endl;

    // 现在初始化系统组件（这可能涉及SSL调用）
    std::cout << "Initializing system components..." << std::endl;

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

#ifdef HAVE_GPERFTOOLS
        // 即使初始化失败也要停止profiler
        ProfilerStop();
        std::cout << "CPU profiler stopped due to init failure" << std::endl;
#endif
        return 1;
    }

    std::cout << "System initialization completed" << std::endl;
    dcdn::MainManager* m = dcdn::MainManager::Singlet();
    logInfo << "Start MainManager";
    m->Start();

    std::shared_ptr<dcdn::DeployManager> deployMgr =
        std::dynamic_pointer_cast<dcdn::DeployManager>(m->GetDeployManager());

    nlohmann::json deployPayload;
    deployPayload["job_id"] = generate_uuid_v4(); // 下划线格式的job_id
    deployPayload["url"] = "https://d1.xia12345.com/video/202310/6524242c37926f1bd8c374d8/hd.mp4";
    deployPayload["file_hash"] = ""; // 可根据需要设置实际哈希值

    nlohmann::json blockInfo;
    blockInfo["hash"] = ""; // 块哈希（可选）
    blockInfo["start"] = 0; // 块起始位置（可选）
    blockInfo["end"] = 0; // 块结束位置（可选）
    deployPayload["block_info"] = blockInfo;

    nlohmann::json payload;
    payload["deploy_file"] = deployPayload;
    // 创建JSON类型事件（与接收端的ArgEvent<json>匹配）
    auto evt = std::make_shared<dcdn::ArgEvent<nlohmann::json>>(
        dcdn::EventType::DeployMsg,
        std::move(payload) // 传递JSON对象
    );

    deployMgr->PostEvent(evt);
    logInfo << "Sent deploy test event with job_id: " << deployPayload["job_id"];

    // 运行一段时间后停止profiler
    int count = 0;
    while (count < 10) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        count++;
    }

#ifdef HAVE_GPERFTOOLS
    // 停止CPU profiler
    ProfilerStop();
    std::cout << "CPU profiler stopped" << std::endl;
#endif

    return 0;
}