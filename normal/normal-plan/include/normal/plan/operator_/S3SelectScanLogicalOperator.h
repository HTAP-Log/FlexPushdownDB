//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_S3SELECTSCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_S3SELECTSCANLOGICALOPERATOR_H

#include <memory>
#include <string>

#include <normal/core/Operator.h>
#include <normal/pushdown/AWSClient.h>

#include "ScanLogicalOperator.h"
#include <normal/connector/s3/S3SelectPartitioningScheme.h>
#include <normal/pushdown/cache/CacheLoad.h>
#include <normal/pushdown/filter/Filter.h>

namespace normal::plan::operator_ {

class S3SelectScanLogicalOperator : public ScanLogicalOperator {

public:
  S3SelectScanLogicalOperator(const std::shared_ptr<S3SelectPartitioningScheme> &partitioningScheme);

  std::vector<std::pair<std::shared_ptr<core::Operator>, int>> toOperatorsWithPlacementsUniHash(int numNodes) override;
  std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> extractSegmentKeys() override;

  std::vector<std::pair<std::shared_ptr<core::Operator>, int>> toOperatorsFullPullupUniHash(int numNodes, int numRanges);
  std::vector<std::pair<std::shared_ptr<core::Operator>, int>> toOperatorsFullPushdownUniHash(int numNodes, int numRanges);
  std::vector<std::pair<std::shared_ptr<core::Operator>, int>> toOperatorsPullupCachingUniHash(int numNodes, int numRanges);
  std::vector<std::pair<std::shared_ptr<core::Operator>, int>> toOperatorsHybridCachingUniHash(int numNodes, int numRanges);
  std::vector<std::pair<std::shared_ptr<core::Operator>, int>> toOperatorsHybridCachingLastUniHash(int numNodes, int numRanges);

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_S3SELECTSCANLOGICALOPERATOR_H
