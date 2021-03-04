//
// Created by Yifei Yang on 7/14/20.
//

#include "normal/plan/operator_/JoinLogicalOperator.h"
#include <normal/plan/operator_/type/OperatorTypes.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <normal/plan/Globals.h>

using namespace normal::plan::operator_;

JoinLogicalOperator::JoinLogicalOperator(const std::string &leftColumnName, const std::string &rightColumnName,
                                         const std::shared_ptr<LogicalOperator> &leftProducer,
                                         const std::shared_ptr<LogicalOperator> &rightProducer)
        : LogicalOperator(type::OperatorTypes::joinOperatorType()),
          leftColumnName_(leftColumnName), rightColumnName_(rightColumnName),
          leftProducer_(leftProducer), rightProducer_(rightProducer){}

std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>>
JoinLogicalOperator::toOperatorsWithPlacementsUniHash(int numNodes) {
  const int numConcurrentUnits = JoinParallelDegree;
  std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> operatorsWithPlacements;
  std::vector<std::shared_ptr<normal::core::Operator>> joinBuilds, joinProbes;

  // join build
  for (auto index = 0; index < numConcurrentUnits; index++) {
    auto joinBuild = std::make_shared<normal::pushdown::join::HashJoinBuild>(
            fmt::format("join-build-{}-{}-{}", leftColumnName_, rightColumnName_, index),
            leftColumnName_,
            getQueryId());
    operatorsWithPlacements.emplace_back(joinBuild, index % numNodes);
    joinBuilds.emplace_back(joinBuild);
  }

  // join probe
  for (auto index = 0; index < numConcurrentUnits; index++) {
    auto joinProbe = std::make_shared<normal::pushdown::join::HashJoinProbe>(
            fmt::format("join-probe-{}-{}-{}", leftColumnName_, rightColumnName_, index),
            normal::pushdown::join::JoinPredicate::create(leftColumnName_,rightColumnName_),
            neededColumnNames_,
            getQueryId());
    operatorsWithPlacements.emplace_back(joinProbe, index % numNodes);
    joinProbes.emplace_back(joinProbe);
  }

  // wire up internally
  for (size_t i = 0; i < joinBuilds.size(); i++) {
    joinBuilds[i]->produce(joinProbes[i]);
    joinProbes[i]->consume(joinBuilds[i]);
  }

  return operatorsWithPlacements;
}

const std::shared_ptr<LogicalOperator> &JoinLogicalOperator::getLeftProducer() const {
  return leftProducer_;
}

const std::shared_ptr<LogicalOperator> &JoinLogicalOperator::getRightProducer() const {
  return rightProducer_;
}

const std::string &JoinLogicalOperator::getLeftColumnName() const {
  return leftColumnName_;
}

const std::string &JoinLogicalOperator::getRightColumnName() const {
  return rightColumnName_;
}

void JoinLogicalOperator::setNeededColumnNames(const std::set<std::string> &neededColumnNames) {
  neededColumnNames_ = neededColumnNames;
}

const std::set<std::string> &JoinLogicalOperator::getNeededColumnNames() const {
  return neededColumnNames_;
}

