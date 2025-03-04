//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_GLOBALS_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include <spdlog/spdlog.h>

namespace normal::ssb {
    void htapTest(int test_mode);
}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_GLOBALS_H
