//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_GLOBALS_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#include <spdlog/spdlog.h>
#include <aws/s3/S3Client.h>
#include <normal/pushdown/AWSClient.h>
#include <normal/plan/placement/PlacementStrategy.h>

namespace normal::plan {

inline constexpr int NumRanges = 1;
inline constexpr int JoinParallelDegree = 4;
inline placement::PlacementStrategy PMStrategy = placement::UNIFORM_HASH;
}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_GLOBALS_H
