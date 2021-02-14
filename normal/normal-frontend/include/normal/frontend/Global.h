//
// Created by Yifei Yang on 2/9/21.
//

#ifndef NORMAL_FRONTEND_GLOBAL_H
#define NORMAL_FRONTEND_GLOBAL_H

#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>

namespace normal::frontend {
  inline constexpr size_t defaultCacheSize_ = 8L * 1024 * 1024 * 1024;
  inline constexpr int defaultModeType_ = 2;
  inline constexpr int defaultCachingPolicyType_ = 3;
  inline const std::string defaultBucketName_ = "pushdowndb";
  inline const std::string defaultDirPrefix = "ssb-sf10-sortlineorder/csv/";
}

#endif //NORMAL_FRONTEND_GLOBAL_H
