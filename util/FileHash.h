#ifndef _DCDN_SDK_FILE_HASH_H_
#define _DCDN_SDK_FILE_HASH_H_

#include <fstream>
#include <string>
#include <vector>

#include "Base64.h"
#include "Blake2sHash.h"
#include "common/Common.h"
#include "plog/Log.h"

NS_BEGIN(dcdn)
NS_BEGIN(util)

// 文件哈希计算类（基于 BLAKE2s）
class FileHash
{
public:
    // 计算文件哈希并按指定格式返回
    // 流程：BLAKE2s哈希(前20字节) -> 加0x01前缀 -> URL安全Base64编码
    // 返回值：ErrorCodeOk 成功，其他值失败
    static int CalculateFileHash(const std::string& filePath, std::string& hashResult)
    {
        // 打开文件
        std::ifstream file(filePath, std::ios::binary);
        if (!file.is_open()) {
            logError << "Failed to open file: " << filePath;
            return ErrorCodeErr;
        }

        // 计算文件BLAKE2s哈希
        Blake2sHash hasher;
        const size_t bufferSize = 4096;
        unsigned char buffer[bufferSize];
        int ret = ErrorCodeOk;

        while (file.read(reinterpret_cast<char*>(buffer), bufferSize)) {
            if (hasher.Update(buffer, bufferSize) != 1) {
                logError << "Failed to update hash";
                ret = ErrorCodeErr;
                break;
            }
        }

        // 处理最后一块数据
        if (ret == ErrorCodeOk) {
            size_t lastRead = file.gcount();
            if (lastRead > 0 && hasher.Update(buffer, lastRead) != 1) {
                logError << "Failed to update hash with last block";
                ret = ErrorCodeErr;
            }
        }

        // 获取完整哈希值(32字节)
        std::vector<unsigned char> fullHash;
        if (ret == ErrorCodeOk && hasher.Final(fullHash) != 1) {
            logError << "Failed to finalize hash";
            ret = ErrorCodeErr;
        }

        // 验证哈希长度并截取前20字节
        if (ret == ErrorCodeOk) {
            if (fullHash.size() < 20) {
                logError << "Hash result too short, need at least 20 bytes";
                ret = ErrorCodeErr;
            } else {
                // 构造21字节数据(0x01前缀 + 20字节哈希)
                std::vector<unsigned char> data;
                data.reserve(21);
                data.push_back(0x01);
                data.insert(data.end(), fullHash.begin(), fullHash.begin() + 20);

                // 执行URL安全Base64编码
                hashResult = Base64::Encode(data.data(), data.size());
            }
        }

        return ret;
    }

    // 获取文件大小
    // 返回值：ErrorCodeOk 成功，其他值失败
    static int GetFileSize(const std::string& filePath, uint64_t& fileSize)
    {
        fileSize = 0; // 初始化输出参数

        // 以二进制模式打开文件并定位到末尾
        std::ifstream file(filePath, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            logError << "Failed to open file for size check: " << filePath;
            return ErrorCodeErr;
        }

        // 获取文件大小(当前位置即为文件末尾偏移量)
        std::streamoff size = file.tellg();
        if (size < 0) {
            logError << "Failed to get file size for: " << filePath;
            return ErrorCodeErr;
        }

        fileSize = static_cast<uint64_t>(size);
        logInfo << "File size for " << filePath << ": " << fileSize << " bytes";
        return ErrorCodeOk;
    }
};

NS_END
NS_END

#endif // _DCDN_SDK_FILE_HASH_H_