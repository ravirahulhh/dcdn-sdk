#ifndef _DCDN_SDK_JSON_TYPES_H_
#define _DCDN_SDK_JSON_TYPES_H_

#include <nlohmann/json.hpp>

#include <cstdint>
#include <string>
#include <vector>

#include "common/Common.h"

NS_BEGIN(dcdn)

struct JsonBlockInfo
{
    uint64_t start;
    uint64_t end;
    std::string hash;

    JsonBlockInfo() = default;

    JsonBlockInfo(uint64_t s, uint64_t e, const std::string& h): start(s), end(e), hash(h) {}

    nlohmann::json to_json() const
    {
        return nlohmann::json{{"start", start}, {"end", end}, {"hash", hash}};
    }

    static JsonBlockInfo from_json(const nlohmann::json& j)
    {
        return JsonBlockInfo{
            j.at("start").get<uint64_t>(), j.at("end").get<uint64_t>(), j.at("hash").get<std::string>()};
    }
};

struct JsonFileInfo
{
    std::string hash;
    std::string url;
    uint64_t size;
    std::vector<JsonBlockInfo> blocks;

    JsonFileInfo(): size(0) {}

    JsonFileInfo(const std::string& h, const std::string& u, uint64_t s): hash(h), url(u), size(s) {}

    void addBlock(uint64_t start, uint64_t end, const std::string& blockHash)
    {
        blocks.emplace_back(start, end, blockHash);
    }

    nlohmann::json to_json() const
    {
        nlohmann::json j{{"hash", hash}, {"url", url}, {"size", size}};

        nlohmann::json blocksArray = nlohmann::json::array();
        for (const auto& block : blocks) {
            blocksArray.push_back(block.to_json());
        }
        j["blocks"] = blocksArray;

        return j;
    }

    static JsonFileInfo from_json(const nlohmann::json& j)
    {
        JsonFileInfo fileInfo{
            j.at("hash").get<std::string>(), j.at("url").get<std::string>(), j.at("size").get<uint64_t>()};

        if (j.contains("blocks") && j["blocks"].is_array()) {
            for (const auto& blockJson : j["blocks"]) {
                fileInfo.blocks.push_back(JsonBlockInfo::from_json(blockJson));
            }
        }

        return fileInfo;
    }
};

struct JsonReportFileInfo
{
    std::vector<JsonFileInfo> add;
    std::vector<JsonFileInfo> del;

    JsonReportFileInfo() = default;

    void addFile(const JsonFileInfo& file)
    {
        add.push_back(file);
    }

    void delFile(const JsonFileInfo& file)
    {
        del.push_back(file);
    }

    void addFile(const std::string& hash, const std::string& url, uint64_t size)
    {
        add.emplace_back(hash, url, size);
    }

    void delFile(const std::string& hash, const std::string& url, uint64_t size)
    {
        del.emplace_back(hash, url, size);
    }

    nlohmann::json to_json() const
    {
        nlohmann::json addArray = nlohmann::json::array();
        for (const auto& file : add) {
            addArray.push_back(file.to_json());
        }

        nlohmann::json delArray = nlohmann::json::array();
        for (const auto& file : del) {
            delArray.push_back(file.to_json());
        }

        return nlohmann::json{{"add", addArray}, {"del", delArray}};
    }

    static JsonReportFileInfo from_json(const nlohmann::json& j)
    {
        JsonReportFileInfo report{};

        if (j.contains("add") && j["add"].is_array()) {
            for (const auto& fileJson : j["add"]) {
                report.add.push_back(JsonFileInfo::from_json(fileJson));
            }
        }

        if (j.contains("del") && j["del"].is_array()) {
            for (const auto& fileJson : j["del"]) {
                report.del.push_back(JsonFileInfo::from_json(fileJson));
            }
        }

        return report;
    }

    std::string to_json_string() const
    {
        return to_json().dump();
    }

    std::string to_pretty_json_string(int indent = 2) const
    {
        return to_json().dump(indent);
    }

    bool empty() const
    {
        return add.empty() && del.empty();
    }

    void clear()
    {
        add.clear();
        del.clear();
    }
};

inline void to_json(nlohmann::json& j, const JsonBlockInfo& block)
{
    j = block.to_json();
}

inline void from_json(const nlohmann::json& j, JsonBlockInfo& block)
{
    block = JsonBlockInfo::from_json(j);
}

inline void to_json(nlohmann::json& j, const JsonFileInfo& file)
{
    j = file.to_json();
}

inline void from_json(const nlohmann::json& j, JsonFileInfo& file)
{
    file = JsonFileInfo::from_json(j);
}

inline void to_json(nlohmann::json& j, const JsonReportFileInfo& report)
{
    j = report.to_json();
}

inline void from_json(const nlohmann::json& j, JsonReportFileInfo& report)
{
    report = JsonReportFileInfo::from_json(j);
}

NS_END

#endif
