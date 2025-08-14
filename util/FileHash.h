#ifndef FILE_HASH_H
#define FILE_HASH_H

#include <openssl/bio.h>
#include <openssl/evp.h>

#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

// BLAKE2s 哈希算法封装类
class Blake2sHash
{
public:
    Blake2sHash()
    {
        ctx_ = EVP_MD_CTX_new();
        if (!ctx_)
            throw std::runtime_error("Failed to create EVP_MD_CTX");

        if (EVP_DigestInit_ex(ctx_, EVP_blake2s256(), nullptr) != 1) {
            EVP_MD_CTX_free(ctx_);
            throw std::runtime_error("Failed to initialize BLAKE2s context");
        }
    }

    ~Blake2sHash()
    {
        if (ctx_)
            EVP_MD_CTX_free(ctx_);
    }

    // 禁止拷贝
    Blake2sHash(const Blake2sHash&) = delete;
    Blake2sHash& operator=(const Blake2sHash&) = delete;

    // 数据更新
    void update(const void* data, size_t len)
    {
        if (EVP_DigestUpdate(ctx_, data, len) != 1) {
            throw std::runtime_error("Failed to update hash");
        }
    }

    // 获取哈希结果，固定 32 字节
    std::vector<unsigned char> final()
    {
        std::vector<unsigned char> hash(32);
        unsigned int hashLen = 0;
        if (EVP_DigestFinal_ex(ctx_, hash.data(), &hashLen) != 1) {
            throw std::runtime_error("Failed to finalize hash");
        }
        return hash;
    }

    // 静态快速接口
    static std::vector<unsigned char> hash(const void* data, size_t len)
    {
        Blake2sHash h;
        h.update(data, len);
        return h.final();
    }

private:
    EVP_MD_CTX* ctx_;
};

// 文件哈希计算类（基于 BLAKE2s）
class FileHash
{
public:
    // 计算文件哈希并按指定格式返回
    static std::string calculate(const std::string& file_path)
    {
        // 打开文件
        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + file_path);
        }

        // 计算文件内容的 BLAKE2s 哈希
        Blake2sHash hasher;
        const size_t buffer_size = 4096;
        unsigned char buffer[buffer_size];

        // 分块读取并更新哈希
        while (file.read(reinterpret_cast<char*>(buffer), buffer_size)) {
            hasher.update(buffer, buffer_size);
        }
        // 处理最后一块数据
        hasher.update(buffer, file.gcount());

        // 获取完整哈希值（32字节）
        std::vector<unsigned char> full_hash = hasher.final();
        if (full_hash.size() < 20) {
            throw std::runtime_error("Hash result too short");
        }

        // 构造 21 字节数据（0x01 前缀 + 前 20 字节哈希）
        unsigned char data[21];
        data[0] = 0x01;
        std::memcpy(data + 1, full_hash.data(), 20);

        // Base64 编码
        BIO* b64 = BIO_new(BIO_f_base64());
        BIO* bio = BIO_new(BIO_s_mem());
        if (!b64 || !bio) {
            throw std::runtime_error("Failed to create BIO objects");
        }
        bio = BIO_push(b64, bio);
        BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL); // 禁用换行

        if (BIO_write(bio, data, 21) != 21) {
            BIO_free_all(bio);
            throw std::runtime_error("Failed to write data to BIO");
        }
        BIO_flush(bio);

        char* encoded_data;
        long length = BIO_get_mem_data(bio, &encoded_data);
        if (length != 28) {
            BIO_free_all(bio);
            throw std::runtime_error("Base64 encoding length mismatch");
        }

        std::string result(encoded_data, length);
        BIO_free_all(bio);

        return result;
    }
};

#endif // FILE_HASH_H