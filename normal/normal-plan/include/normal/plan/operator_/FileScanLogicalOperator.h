//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_FILESCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_FILESCANLOGICALOPERATOR_H

#include <string>

#include <normal/core/Operator.h>
#include <normal/connector/local-fs/LocalFilePartitioningScheme.h>

#include <normal/plan/operator_/ScanLogicalOperator.h>

namespace normal::plan::operator_ {

class FileScanLogicalOperator: public ScanLogicalOperator {

public:
  explicit FileScanLogicalOperator(const std::shared_ptr<LocalFilePartitioningScheme>& partitioningScheme);

  std::vector<std::pair<std::shared_ptr<core::Operator>, int>> toOperatorsWithPlacementsUniHash(int numNodes) override;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_FILESCANLOGICALOPERATOR_H
