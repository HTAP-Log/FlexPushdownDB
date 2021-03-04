//
// Created by matt on 7/4/20.
//

#include <normal/plan/operator_/FileScanLogicalOperator.h>

#include <normal/pushdown/file/FileScan.h>
#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

FileScanLogicalOperator::FileScanLogicalOperator(
	const std::shared_ptr<LocalFilePartitioningScheme> &partitioningScheme) :
	ScanLogicalOperator(partitioningScheme) {}

// TODO: need to create subsequent filters, like S3SelectScanLogicalOperator
std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>>
FileScanLogicalOperator::toOperatorsWithPlacementsUniHash(int numNodes) {
  std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> operatorsWithPlacements;
  int index = 0;
  for (const auto &partition: *getPartitioningScheme()->partitions()) {
    auto localFilePartition = std::static_pointer_cast<LocalFilePartition>(partition);

    const std::shared_ptr<pushdown::FileScan> &fileScanOperator =
      std::make_shared<normal::pushdown::FileScan>(localFilePartition->getPath(),
                             localFilePartition->getPath(),
                             getQueryId());


    operatorsWithPlacements.emplace_back(fileScanOperator, index % numNodes);
    index++;
  }
  return operatorsWithPlacements;
}
