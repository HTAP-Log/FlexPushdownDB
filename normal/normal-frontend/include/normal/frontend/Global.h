//
// Created by Yifei Yang on 2/9/21.
//

#ifndef NORMAL_FRONTEND_GLOBAL_H
#define NORMAL_FRONTEND_GLOBAL_H

#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <caf/all.hpp>
#include <caf/io/all.hpp>

namespace normal::frontend {
  inline constexpr size_t DefaultCacheSize_ = 8L * 1024 * 1024 * 1024;
  inline constexpr int DefaultModeType_ = 4;
  inline constexpr int DefaultCachingPolicyType_ = 3;
  inline const std::string DefaultBucketName_ = "pushdowndb";
  inline const std::string DefaultDirPrefix_ = "ssb-sf1-sortlineorder/csv/";
  inline constexpr int DefaultServerPort_ = 4242;
}

#endif //NORMAL_FRONTEND_GLOBAL_H
