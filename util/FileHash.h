#ifndef _DCDN_SDK_FILE_HASH_H_
#define _DCDN_SDK_FILE_HASH_H_

#include <openssl/bio.h>
#include <openssl/evp.h>

#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "common/Common.h"
#include "plog/Log.h"

NS_BEGIN(dcdn)

// 文件哈希计算类（基于 BLAKE2s）
class FileHash
{
public:
    // 计算文件的BLAKE2s哈希值，返回前20字节
    // 返回值：ErrorCodeOk 成功，其他值失败
    static int calculateBlake2sHash(const std::string& filePath, std::vector<unsigned char>& hashResult)
    {
        // 打开文件
        std::ifstream file(filePath, std::ios::binary);
        if (!file.is_open()) {
            logError << "Failed to open file: " << filePath;
            return ErrorCodeErr;
        }

        // 初始化 BLAKE2s 哈希上下文
        EVP_MD_CTX* ctx = EVP_MD_CTX_new();
        if (!ctx) {
            logError << "Failed to create EVP_MD_CTX";
            return ErrorCodeErr;
        }

        if (EVP_DigestInit_ex(ctx, EVP_blake2s256(), nullptr) != 1) {
            logError << "Failed to initialize BLAKE2s context";
            EVP_MD_CTX_free(ctx);
            return ErrorCodeErr;
        }

        // 分块读取文件并更新哈希
        const size_t bufferSize = 4096;
        unsigned char buffer[bufferSize];
        int ret = ErrorCodeOk;

        while (file.read(reinterpret_cast<char*>(buffer), bufferSize)) {
            if (EVP_DigestUpdate(ctx, buffer, bufferSize) != 1) {
                logError << "Failed to update hash";
                ret = ErrorCodeErr;
                break;
            }
        }

        // 处理最后一块数据
        if (ret == ErrorCodeOk) {
            size_t lastRead = file.gcount();
            if (lastRead > 0 && EVP_DigestUpdate(ctx, buffer, lastRead) != 1) {
                logError << "Failed to update hash with last block";
                ret = ErrorCodeErr;
            }
        }

        // 获取完整哈希值（32字节）
        std::vector<unsigned char> fullHash(32);
        unsigned int hashLen = 0;
        if (ret == ErrorCodeOk) {
            if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1) {
                logError << "Failed to finalize hash";
                ret = ErrorCodeErr;
            } else if (hashLen != 32) {
                logError << "Invalid BLAKE2s hash length: " << hashLen;
                ret = ErrorCodeErr;
            }
        }

        // 释放哈希上下文
        EVP_MD_CTX_free(ctx);

        if (ret != ErrorCodeOk) {
            return ret;
        }

        // 验证哈希长度足够并截取前20字节
        if (fullHash.size() < 20) {
            logError << "Hash result too short, need at least 20 bytes";
            return ErrorCodeErr;
        }

        hashResult.assign(fullHash.begin(), fullHash.begin() + 20);
        return ErrorCodeOk;
    }

    // 对数据执行URL安全的Base64编码（使用-和_代替+和/）
    // 返回值：ErrorCodeOk 成功，其他值失败
    static int base64Encode(const std::vector<unsigned char>& data, std::string& encodedResult)
    {
        if (data.empty()) {
            logError << "Input data for base64 encoding is empty";
            return ErrorCodeErr;
        }

        // 创建Base64编码所需的BIO对象
        BIO* b64 = BIO_new(BIO_f_base64());
        BIO* bio = BIO_new(BIO_s_mem());
        if (!b64 || !bio) {
            logError << "Failed to create BIO objects for base64 encoding";
            if (b64)
                BIO_free(b64);
            if (bio)
                BIO_free(bio);
            return ErrorCodeErr;
        }

        // 链接BIO并设置标志
        bio = BIO_push(b64, bio);
        BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL); // 禁用换行

        // 写入数据进行编码
        if (BIO_write(bio, data.data(), data.size()) != static_cast<int>(data.size())) {
            logError << "Failed to write data to BIO for base64 encoding";
            BIO_free_all(bio);
            return ErrorCodeErr;
        }

        // 刷新以确保所有数据都被处理
        if (BIO_flush(bio) != 1) {
            logError << "Failed to flush BIO for base64 encoding";
            BIO_free_all(bio);
            return ErrorCodeErr;
        }

        // 获取编码结果
        char* encodedData;
        long length = BIO_get_mem_data(bio, &encodedData);
        if (length <= 0) {
            logError << "Failed to get base64 encoded data";
            BIO_free_all(bio);
            return ErrorCodeErr;
        }

        // 转换为URL安全的Base64（替换+为-，/为_）
        encodedResult.assign(encodedData, length);
        for (char& c : encodedResult) {
            if (c == '+')
                c = '-';
            else if (c == '/')
                c = '_';
        }

        // 释放资源
        BIO_free_all(bio);
        return ErrorCodeOk;
    }

    // 计算文件哈希并按指定格式返回（组合上述两个方法）
    // 返回值：ErrorCodeOk 成功，其他值失败
    static int calculate(const std::string& filePath, std::string& hashResult)
    {
        // 计算BLAKE2s哈希的前20字节
        std::vector<unsigned char> blakeHash;
        int ret = calculateBlake2sHash(filePath, blakeHash);
        if (ret != ErrorCodeOk) {
            return ret;
        }

        // 构造21字节数据（0x01前缀 + 20字节哈希）
        std::vector<unsigned char> data;
        data.reserve(21);
        data.push_back(0x01);
        data.insert(data.end(), blakeHash.begin(), blakeHash.end());

        // 执行URL安全的Base64编码
        return base64Encode(data, hashResult);
    }

    static int getFileSize(const std::string& filePath, uint64_t& fileSize)
    {
        fileSize = 0; // 初始化输出参数

        // 以二进制模式打开文件，定位到文件末尾
        std::ifstream file(filePath, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            logError << "Failed to open file for size check: " << filePath;
            return ErrorCodeErr;
        }

        // 获取文件大小（当前位置即为文件末尾偏移量）
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

#endif // _DCDN_SDK_FILE_HASH_H_
