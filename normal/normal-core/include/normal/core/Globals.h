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

  inline constexpr ::caf::type_id_t OperatorActor_first_custom_type_id = ::caf::first_custom_type_id;
  inline constexpr ::caf::type_id_t OperatorActor2_first_custom_type_id = ::caf::first_custom_type_id + 100;
  inline constexpr ::caf::type_id_t SegmentCacheActor_first_custom_type_id = ::caf::first_custom_type_id + 200;
  inline constexpr ::caf::type_id_t Collate2_first_custom_type_id = ::caf::first_custom_type_id + 300;
  inline constexpr ::caf::type_id_t S3SelectScan2_first_custom_type_id = ::caf::first_custom_type_id + 400;
  inline constexpr ::caf::type_id_t ScanOperator_first_custom_type_id = ::caf::first_custom_type_id + 500;
  inline constexpr ::caf::type_id_t FileScan2_first_custom_type_id = ::caf::first_custom_type_id + 600;

  void init_caf_global_meta_objects();

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GLOBALS_H
