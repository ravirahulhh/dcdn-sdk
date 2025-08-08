#ifndef _DCDN_SDK_JSON_TYPES_H_
#define _DCDN_SDK_JSON_TYPES_H_

#include "common/Common.h"
#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

NS_BEGIN(dcdn)

// JSON structures corresponding to gateway.proto messages
// These provide convenient C++ structures for building JSON payloads

struct JsonBlockInfo {
  uint64_t start;
  uint64_t end;
  std::string hash;

  // Default constructor
  JsonBlockInfo() = default;

  // Constructor with parameters
  JsonBlockInfo(uint64_t s, uint64_t e, const std::string &h)
      : start(s), end(e), hash(h) {}

  // Convert to JSON
  nlohmann::json to_json() const {
    return nlohmann::json{{"start", start}, {"end", end}, {"hash", hash}};
  }

  // Create from JSON
  static JsonBlockInfo from_json(const nlohmann::json &j) {
    return JsonBlockInfo{j.at("start").get<uint64_t>(),
                         j.at("end").get<uint64_t>(),
                         j.at("hash").get<std::string>()};
  }
};

struct JsonFileInfo {
  std::string hash;
  std::string url;
  uint64_t size;
  std::vector<JsonBlockInfo> blocks;

  // Default constructor
  JsonFileInfo() : size(0) {}

  // Constructor with parameters
  JsonFileInfo(const std::string &h, const std::string &u, uint64_t s)
      : hash(h), url(u), size(s) {}

  // Add a block to this file
  void addBlock(uint64_t start, uint64_t end, const std::string &block_hash) {
    blocks.emplace_back(start, end, block_hash);
  }

  // Convert to JSON
  nlohmann::json to_json() const {
    nlohmann::json j{{"hash", hash}, {"url", url}, {"size", size}};

    nlohmann::json blocks_array = nlohmann::json::array();
    for (const auto &block : blocks) {
      blocks_array.push_back(block.to_json());
    }
    j["blocks"] = blocks_array;

    return j;
  }

  // Create from JSON
  static JsonFileInfo from_json(const nlohmann::json &j) {
    JsonFileInfo file_info{j.at("hash").get<std::string>(),
                           j.at("url").get<std::string>(),
                           j.at("size").get<uint64_t>()};

    if (j.contains("blocks") && j["blocks"].is_array()) {
      for (const auto &block_json : j["blocks"]) {
        file_info.blocks.push_back(JsonBlockInfo::from_json(block_json));
      }
    }

    return file_info;
  }
};

struct JsonReportFileInfo {
  std::vector<JsonFileInfo> add;
  std::vector<JsonFileInfo> del;

  // Default constructor
  JsonReportFileInfo() = default;

  // Add a file to the add list
  void addFile(const JsonFileInfo &file) { add.push_back(file); }

  // Add a file to the delete list
  void delFile(const JsonFileInfo &file) { del.push_back(file); }

  // Helper to add file with basic info to add list
  void addFile(const std::string &hash, const std::string &url, uint64_t size) {
    add.emplace_back(hash, url, size);
  }

  // Helper to add file with basic info to del list
  void delFile(const std::string &hash, const std::string &url, uint64_t size) {
    del.emplace_back(hash, url, size);
  }

  // Convert to JSON
  nlohmann::json to_json() const {
    nlohmann::json add_array = nlohmann::json::array();
    for (const auto &file : add) {
      add_array.push_back(file.to_json());
    }

    nlohmann::json del_array = nlohmann::json::array();
    for (const auto &file : del) {
      del_array.push_back(file.to_json());
    }

    return nlohmann::json{{"add", add_array}, {"del", del_array}};
  }

  // Create from JSON
  static JsonReportFileInfo from_json(const nlohmann::json &j) {
    JsonReportFileInfo report{};

    if (j.contains("add") && j["add"].is_array()) {
      for (const auto &file_json : j["add"]) {
        report.add.push_back(JsonFileInfo::from_json(file_json));
      }
    }

    if (j.contains("del") && j["del"].is_array()) {
      for (const auto &file_json : j["del"]) {
        report.del.push_back(JsonFileInfo::from_json(file_json));
      }
    }

    return report;
  }

  // Helper method to create JSON string directly
  std::string to_json_string() const { return to_json().dump(); }

  // Helper method to create pretty JSON string
  std::string to_pretty_json_string(int indent = 2) const {
    return to_json().dump(indent);
  }

  // Check if report is empty
  bool empty() const { return add.empty() && del.empty(); }

  // Clear all files
  void clear() {
    add.clear();
    del.clear();
  }
};

// nlohmann::json ADL (Argument Dependent Lookup) support
// This allows direct usage like: json j = json_block_info;
inline void to_json(nlohmann::json &j, const JsonBlockInfo &block) {
  j = block.to_json();
}

inline void from_json(const nlohmann::json &j, JsonBlockInfo &block) {
  block = JsonBlockInfo::from_json(j);
}

inline void to_json(nlohmann::json &j, const JsonFileInfo &file) {
  j = file.to_json();
}

inline void from_json(const nlohmann::json &j, JsonFileInfo &file) {
  file = JsonFileInfo::from_json(j);
}

inline void to_json(nlohmann::json &j, const JsonReportFileInfo &report) {
  j = report.to_json();
}

inline void from_json(const nlohmann::json &j, JsonReportFileInfo &report) {
  report = JsonReportFileInfo::from_json(j);
}

NS_END

#endif // _DCDN_SDK_JSON_TYPES_H_
