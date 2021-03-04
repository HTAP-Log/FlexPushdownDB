//
// Created by matt on 16/4/20.
//

#include <normal/plan/Planner.h>

#include <normal/plan/operator_/type/OperatorTypes.h>
#include <normal/plan/operator_/ScanLogicalOperator.h>
#include <normal/plan/operator_/JoinLogicalOperator.h>
#include <normal/pushdown/shuffle/Shuffle.h>
#include <normal/plan/operator_/AggregateLogicalOperator.h>
#include <normal/plan/operator_/ProjectLogicalOperator.h>
#include <normal/plan/operator_/GroupLogicalOperator.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/util/Util.h>

using namespace normal::plan;

// Not applicable for aggregate, project and group logical operators (their physical operators depend on their producers)
std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> toPhysicalOperators (
        std::shared_ptr<normal::plan::operator_::LogicalOperator> &logicalOperator,
        std::unordered_map<std::shared_ptr<normal::plan::operator_::LogicalOperator>,
                std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>>> &logicalToPhysical_map,
        std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> &allPhysicalOperators,
        int numNodes) {

  auto existPhysicalOperators = logicalToPhysical_map.find(logicalOperator);
  if (existPhysicalOperators == logicalToPhysical_map.end()) {
    auto physicalOperators = logicalOperator->toOperatorsWithPlacements(numNodes);
    logicalToPhysical_map.emplace(logicalOperator, physicalOperators);
    allPhysicalOperators.insert(allPhysicalOperators.end(), physicalOperators.begin(), physicalOperators.end());
    return physicalOperators;
  } else {
    return existPhysicalOperators->second;
  }
}

void wireUp (std::shared_ptr<normal::plan::operator_::LogicalOperator> &logicalProducer,
             std::shared_ptr<normal::plan::operator_::LogicalOperator> &logicalConsumer,
             std::unordered_map<std::shared_ptr<normal::plan::operator_::LogicalOperator>,
                     std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>>> &logicalToPhysical_map,
             std::unordered_set<std::shared_ptr<normal::plan::operator_::LogicalOperator>> &wiredLogicalProducers,
             std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> &allPhysicalOperators,
             const int numNodes){

  // if logicalProducer is already wired, return
  if (wiredLogicalProducers.find(logicalProducer) != wiredLogicalProducers.end())
    return;

  // extract stream-out physical operators
  std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> streamOutPhysicalOperators;

  if (logicalProducer->type()->is(operator_::type::OperatorTypes::joinOperatorType())){
    // get join physical operators
    auto joinPhysicalOperators = toPhysicalOperators(logicalProducer, logicalToPhysical_map, allPhysicalOperators, numNodes);

    // joinProbes are stream-out operators
    for (size_t index = joinPhysicalOperators.size() / 2; index < joinPhysicalOperators.size(); index++)
      streamOutPhysicalOperators.emplace_back(joinPhysicalOperators[index]);
  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::aggregateOperatorType())) {
    // get aggregate physical operators
    std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> aggregatePhysicalOperators;
    auto aggregatePhysicalOperators_pair = logicalToPhysical_map.find(logicalProducer);
    if (aggregatePhysicalOperators_pair == logicalToPhysical_map.end()) {
      auto aggregateLogicalOperator = std::static_pointer_cast<normal::plan::operator_::AggregateLogicalOperator>(logicalProducer);
      auto producerOfAggregateLogicalOperator = aggregateLogicalOperator->getProducer();
      wireUp(producerOfAggregateLogicalOperator,
             logicalProducer,
             logicalToPhysical_map,
             wiredLogicalProducers,
             allPhysicalOperators,
             numNodes);
      aggregatePhysicalOperators = logicalToPhysical_map.find(logicalProducer)->second;
    } else {
      aggregatePhysicalOperators = aggregatePhysicalOperators_pair->second;
    }

    // the single aggregate or aggregateReduce is stream-out operator
    if (aggregatePhysicalOperators.size() == 1) {
      streamOutPhysicalOperators = aggregatePhysicalOperators;
    } else {
      streamOutPhysicalOperators.emplace_back(aggregatePhysicalOperators.back());
    }
  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::groupOperatorType())) {
    // get group physical operators
    std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> groupPhysicalOperators;
    auto groupPhysicalOperators_pair = logicalToPhysical_map.find(logicalProducer);
    if (groupPhysicalOperators_pair == logicalToPhysical_map.end()) {
      auto groupLogicalOperator = std::static_pointer_cast<normal::plan::operator_::GroupLogicalOperator>(logicalProducer);
      auto producerOfGroupLogicalOperator = groupLogicalOperator->getProducer();
      wireUp(producerOfGroupLogicalOperator,
             logicalProducer,
             logicalToPhysical_map,
             wiredLogicalProducers,
             allPhysicalOperators,
             numNodes);
      groupPhysicalOperators = logicalToPhysical_map.find(logicalProducer)->second;
    } else {
      groupPhysicalOperators = groupPhysicalOperators_pair->second;
    }

    // the single group or groupReduce is stream-out operator
    if (groupPhysicalOperators.size() == 1) {
      streamOutPhysicalOperators = groupPhysicalOperators;
    } else {
      streamOutPhysicalOperators.emplace_back(groupPhysicalOperators.back());
    }
  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::projectOperatorType())) {
    // get project physical operators
    std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>> projectPhysicalOperators;
    auto projectPhysicalOperators_pair = logicalToPhysical_map.find(logicalProducer);
    if (projectPhysicalOperators_pair == logicalToPhysical_map.end()) {
      auto projectLogicalOperator = std::static_pointer_cast<normal::plan::operator_::ProjectLogicalOperator>(logicalProducer);
      auto producerOfProjectLogicalOperator = projectLogicalOperator->getProducer();
      wireUp(producerOfProjectLogicalOperator,
             logicalProducer,
             logicalToPhysical_map,
             wiredLogicalProducers,
             allPhysicalOperators,
             numNodes);
      projectPhysicalOperators = logicalToPhysical_map.find(logicalProducer)->second;
    } else {
      projectPhysicalOperators = projectPhysicalOperators_pair->second;
    }

    // all project physical operators are stream-out operators
    streamOutPhysicalOperators = projectPhysicalOperators;
  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::scanOperatorType())) {
    toPhysicalOperators(logicalProducer, logicalToPhysical_map, allPhysicalOperators, numNodes);
    auto scanLogicalOperator = std::static_pointer_cast<operator_::ScanLogicalOperator>(logicalProducer);
    streamOutPhysicalOperators = scanLogicalOperator->streamOutPhysicalOperators();
  }

  else {
    std::runtime_error("Bad logicalProducer type: '" + logicalProducer->type()->toString() + "', check logical plan");
  }


  // wire up to stream-in physical operators
  if (logicalConsumer->type()->is(operator_::type::OperatorTypes::joinOperatorType())) {
    auto joinPhysicalOperators = toPhysicalOperators(logicalConsumer, logicalToPhysical_map, allPhysicalOperators, numNodes);
    auto joinLogicalOperator = std::static_pointer_cast<normal::plan::operator_::JoinLogicalOperator>(logicalConsumer);
    auto leftColumnName = joinLogicalOperator->getLeftColumnName();
    auto rightColumnName = joinLogicalOperator->getRightColumnName();

    // joinBuilds and joinProbes
    auto joinBuilds = std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>>(
            joinPhysicalOperators.begin(), joinPhysicalOperators.begin() + joinPhysicalOperators.size() / 2);
    auto joinProbes = std::vector<std::pair<std::shared_ptr<normal::core::Operator>, int>>(
            joinPhysicalOperators.begin() + joinPhysicalOperators.size() / 2, joinPhysicalOperators.end());

    // check logicalComsumer is left consumer or right consumer
    if (logicalProducer == joinLogicalOperator->getLeftProducer()) {
      // if more than 1 join build/probe, need shuffle. Construct a shuffle physical operator for each stream-out operator
      if (joinBuilds.size() > 1) {
        for (const auto &streamOutPhysicalOperator: streamOutPhysicalOperators) {
          auto shuffle = normal::pushdown::shuffle::Shuffle::make(streamOutPhysicalOperator.first->name() + "-shuffle",
                                                                  leftColumnName,
                                                                  Planner::getQueryId());
          // wire up
          streamOutPhysicalOperator.first->produce(shuffle);
          shuffle->consume(streamOutPhysicalOperator.first);
          for (const auto &joinBuild: joinBuilds) {
            shuffle->produce(joinBuild.first);
            joinBuild.first->consume(shuffle);
          }
          allPhysicalOperators.emplace_back(shuffle, streamOutPhysicalOperator.second);
        }
      } else {
        auto joinBuild = joinBuilds[0];
        for (const auto &streamOutPhysicalOperator: streamOutPhysicalOperators) {
          streamOutPhysicalOperator.first->produce(joinBuild.first);
          joinBuild.first->consume(streamOutPhysicalOperator.first);
        }
      }
    }
    else {
      // if more than 1 join build/probe, need shuffle. Construct a shuffle physical operator for each stream-out operator
      if (joinProbes.size() > 1) {
        for (const auto &streamOutPhysicalOperator: streamOutPhysicalOperators) {
          auto shuffle = normal::pushdown::shuffle::Shuffle::make(streamOutPhysicalOperator.first->name() + "-shuffle",
                                                                  rightColumnName,
                                                                  Planner::getQueryId());
          // wire up
          streamOutPhysicalOperator.first->produce(shuffle);
          shuffle->consume(streamOutPhysicalOperator.first);
          for (const auto &joinProbe: joinProbes) {
            shuffle->produce(joinProbe.first);
            joinProbe.first->consume(shuffle);
          }
          allPhysicalOperators.emplace_back(shuffle, streamOutPhysicalOperator.second);
        }
      } else {
        auto joinProbe = joinProbes[0];
        for (const auto &streamOutPhysicalOperator: streamOutPhysicalOperators) {
          streamOutPhysicalOperator.first->produce(joinProbe.first);
          joinProbe.first->consume(streamOutPhysicalOperator.first);
        }
      }
    }
  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::aggregateOperatorType())){
    // get aggregate physical operators
    auto numConcurrentUnits = streamOutPhysicalOperators.size();
    auto aggregateLogicalOperator = std::static_pointer_cast<normal::plan::operator_::AggregateLogicalOperator>(logicalConsumer);
    aggregateLogicalOperator->setNumConcurrentUnits(numConcurrentUnits);
    auto aggregatePhysicalOperators = toPhysicalOperators(logicalConsumer, logicalToPhysical_map, allPhysicalOperators, numNodes);

    // One-to-one wire up for all except aggregateReduce
    for (size_t index = 0; index < numConcurrentUnits; index++) {
      auto streamOutPhysicalOperator = streamOutPhysicalOperators[index];
      auto streamInPhysicalOperator = aggregatePhysicalOperators[index];
      streamOutPhysicalOperator.first->produce(streamInPhysicalOperator.first);
      streamInPhysicalOperator.first->consume(streamOutPhysicalOperator.first);
    }
  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::groupOperatorType())) {
    // get group physical operators
    auto numConcurrentUnits = streamOutPhysicalOperators.size();
    auto groupLogicalOperator = std::static_pointer_cast<normal::plan::operator_::GroupLogicalOperator>(logicalConsumer);
    groupLogicalOperator->setNumConcurrentUnits(numConcurrentUnits);
    auto groupPhysicalOperators = toPhysicalOperators(logicalConsumer, logicalToPhysical_map, allPhysicalOperators, numNodes);

    // One-to-one wire up for all except groupReduce
    for (size_t index = 0; index < numConcurrentUnits; index++) {
      auto streamOutPhysicalOperator = streamOutPhysicalOperators[index];
      auto streamInPhysicalOperator = groupPhysicalOperators[index];
      streamOutPhysicalOperator.first->produce(streamInPhysicalOperator.first);
      streamInPhysicalOperator.first->consume(streamOutPhysicalOperator.first);
    }
  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::projectOperatorType())) {
    // get project physical operators
    auto numConcurrentUnits = streamOutPhysicalOperators.size();
    auto projectLogicalOperator = std::static_pointer_cast<normal::plan::operator_::ProjectLogicalOperator>(logicalConsumer);
    projectLogicalOperator->setNumConcurrentUnits(numConcurrentUnits);
    auto projectPhysicalOperators = toPhysicalOperators(logicalConsumer, logicalToPhysical_map, allPhysicalOperators, numNodes);

    // One-to-one wire up for all
    for (size_t index = 0; index < numConcurrentUnits; index++) {
      auto streamOutPhysicalOperator = streamOutPhysicalOperators[index];
      auto streamInPhysicalOperator = projectPhysicalOperators[index];
      streamOutPhysicalOperator.first->produce(streamInPhysicalOperator.first);
      streamInPhysicalOperator.first->consume(streamOutPhysicalOperator.first);
    }
  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::collateOperatorType())){
    auto collatePhysicalOperator = toPhysicalOperators(logicalConsumer, logicalToPhysical_map, allPhysicalOperators, numNodes)[0];
    for (const auto &streamOutPhysicalOperator: streamOutPhysicalOperators) {
      streamOutPhysicalOperator.first->produce(collatePhysicalOperator.first);
      collatePhysicalOperator.first->consume(streamOutPhysicalOperator.first);
    }
  }

  else {
    std::runtime_error("Bad logicalConsumer type: '" + logicalConsumer->type()->toString() + "', check logical plan");
  }

}

std::shared_ptr<PhysicalPlan> Planner::generate (const LogicalPlan &logicalPlan,
                                                 int numNodes) {
  const auto& logicalOperators = logicalPlan.getOperators();
  std::unordered_map<std::shared_ptr<normal::plan::operator_::LogicalOperator>,
          std::vector<std::pair<std::shared_ptr<core::Operator>, int>>> logicalToPhysical_map;
  std::unordered_set<std::shared_ptr<normal::plan::operator_::LogicalOperator>> wiredLogicalProducers;
  std::vector<std::pair<std::shared_ptr<core::Operator>, int>> allPhysicalOperators;

  for (auto &logicalProducer: *logicalOperators) {
    auto logicalConsumer = logicalProducer->getConsumer();
    if (logicalConsumer) {
      wireUp(logicalProducer,
             logicalConsumer,
             logicalToPhysical_map,
             wiredLogicalProducers,
             allPhysicalOperators,
             numNodes);
    }
  }

  auto physicalPlan = std::make_shared<PhysicalPlan>();
  for (const auto &physicalOperator: allPhysicalOperators) {
    physicalPlan->put(physicalOperator.first, physicalOperator.second);
  }

  return physicalPlan;
}

void Planner::setQueryId(long queryId) {
  queryId_ = queryId;
}

long Planner::getQueryId() {
  return queryId_;
}
