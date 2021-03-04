//
// Created by matt on 14/4/20.
//

#include <normal/plan/operator_/ProjectLogicalOperator.h>


#include <normal/pushdown/Project.h>
#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

ProjectLogicalOperator::ProjectLogicalOperator(
	std::shared_ptr<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>> expressions,
  std::shared_ptr<LogicalOperator> producer) :
	LogicalOperator(type::OperatorTypes::projectOperatorType()),
	expressions_(std::move(expressions)),
	producer_(std::move(producer)) {}

const std::shared_ptr<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>> &ProjectLogicalOperator::expressions() const {
  return expressions_;
}


std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>>
ProjectLogicalOperator::toOperatorsWithPlacementsUniHash(int numNodes) {
  std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> operatorsWithPlacements;

  for (auto index = 0; index < numConcurrentUnits_; index++) {
    // FIXME: Defaulting to name -> proj
    auto project = std::make_shared<normal::pushdown::Project>(fmt::format("proj-{}", index),
                                                               *expressions_,
                                                               getQueryId());
    operatorsWithPlacements.emplace_back(project, index % numNodes);
  }

  return operatorsWithPlacements;
}

const std::shared_ptr<LogicalOperator> &ProjectLogicalOperator::getProducer() const {
  return producer_;
}

void ProjectLogicalOperator::setNumConcurrentUnits(int numConcurrentUnits) {
  numConcurrentUnits_ = numConcurrentUnits;
}
