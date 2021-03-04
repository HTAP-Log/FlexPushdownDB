//
// Created by matt on 1/4/20.
//

#include <normal/plan/operator_/CollateLogicalOperator.h>


#include <normal/pushdown/collate/Collate.h>

#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

CollateLogicalOperator::CollateLogicalOperator()
	: LogicalOperator(type::OperatorTypes::collateOperatorType()) {}


std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>>
        CollateLogicalOperator::toOperatorsWithPlacementsUniHash(int numNodes) {
  std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> operatorsWithPlacements;
  auto collate = std::make_shared<normal::pushdown::Collate>("collate", getQueryId());
  operatorsWithPlacements.emplace_back(collate, 0);
  return operatorsWithPlacements;
}

