//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_GLOBALS_H
#define NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include <spdlog/spdlog.h>
#include <experimental/filesystem>

namespace normal::connector {
  inline const std::experimental::filesystem::path MetadataPath = std::experimental::filesystem::current_path().parent_path().append("normal-ssb/metadata");
}

#endif //NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_GLOBALS_H
