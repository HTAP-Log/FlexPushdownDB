//
// Created by matt on 5/3/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GLOBALS_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL

#include <aws/s3/S3Client.h>
#include "AWSClient.h"
#include "spdlog/spdlog.h"

namespace normal::pushdown {

inline int arrowConversionMode = 2;
inline bool performReqsInsplitReqMode = true;

/**
 * Default number of tuples operators should buffer before sending to consumers
 */
inline constexpr int DefaultBufferSize = 10000;
inline constexpr int DefaultS3ScanBufferSize = 100000;
inline constexpr int DefaultS3ConversionBufferSize = 128 * 1024;
inline constexpr uint64_t DefaultS3RangeSize = 15 * 1024 * 1024; // 15MB/s This value was tuned on c5n.9xlarge and
                                                 // may need to be retuned for different instances with many more cores

inline size_t NetworkLimit = 0;
inline constexpr bool RefinedWeightFunction = true;

/**
 * Parameters used in WLFU, with csv_150MB/ and 200 parallel reqs
 */
// c5a.8x
inline constexpr double vNetwork = 1.16320;     // unit: GB/s
inline constexpr double vS3Scan = 18.00891;     // unit: GB/s
inline constexpr double vS3Filter = 0.32719;    // unit: GPred/s

// These parameters are for running GET in parallel as a detached operation
// We only want to convert ~max cores results at a time since otherwise we get very bad cache thrashing
// that degrades system performance. Additionally setting a variable sleep retry interval appears to make parallel GET
// requests perform much faster than using a fixed interval.
inline constexpr int maxConcurrentArrowConversions = 36; // Set to ~#cores
inline constexpr int minimumSleepRetryTimeMS = 5;
inline constexpr int variableSleepRetryTimeMS = 15;
}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GLOBALS_H
