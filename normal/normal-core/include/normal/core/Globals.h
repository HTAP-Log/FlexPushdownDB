//
// Created by matt on 5/3/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GLOBALS_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

/**
 * Setting the log level here will disable macros for levels below it
 */
#include <spdlog/spdlog.h>
#include <caf/all.hpp>
#include <caf/io/all.hpp>

namespace normal::core {

  // A global segment cache actor for actors from other actor systems to communicate, initiated in client.boot()
  inline caf::actor GlobalSegmentCacheActor_;

  void init_caf_global_meta_objects();
}



#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GLOBALS_H
