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

    std::shared_ptr<dcdn::DeployManager> deployMgr =
        std::dynamic_pointer_cast<dcdn::DeployManager>(m->GetDeployManager());

    nlohmann::json deployPayload;
    deployPayload["job_id"] = generate_uuid_v4(); // 下划线格式的job_id
    deployPayload["url"] = "https://d1.xia12345.com/video/202501/677c87f9e2519513f3edc65b/hd.mp4";
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

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}