//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_GLOBALS_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include "spdlog/spdlog.h"

namespace normal::cache {

  extern std::mutex replacementGlobalLock;
}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_GLOBALS_H
