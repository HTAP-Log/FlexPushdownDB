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
#include <normal/plan/mode/Modes.h>
#include <normal/util/Util.h>

using namespace normal::plan;

// Not applicable for aggregate, project and group logical operators (their physical operators depend on their producers)
std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> toPhysicalOperators (
        std::shared_ptr<normal::plan::operator_::LogicalOperator> &logicalOperator,
        std::shared_ptr<std::unordered_map<
          std::shared_ptr<normal::plan::operator_::LogicalOperator>,
          std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>>> &logicalToPhysical_map,
        std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> &allPhysicalOperators) {

  auto existPhysicalOperators = logicalToPhysical_map->find(logicalOperator);
  if (existPhysicalOperators == logicalToPhysical_map->end()) {
    auto physicalOperators = logicalOperator->toOperators();
    logicalToPhysical_map->insert({logicalOperator, physicalOperators});
    allPhysicalOperators->insert(allPhysicalOperators->end(), physicalOperators->begin(), physicalOperators->end());
    return physicalOperators;
  } else {
    return existPhysicalOperators->second;
  }
}

void wireUp (std::shared_ptr<normal::plan::operator_::LogicalOperator> &logicalProducer,
             std::shared_ptr<normal::plan::operator_::LogicalOperator> &logicalConsumer,
             std::shared_ptr<std::unordered_map<
                     std::shared_ptr<normal::plan::operator_::LogicalOperator>,
                     std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>>> &logicalToPhysical_map,
             std::shared_ptr<std::vector<std::shared_ptr<normal::plan::operator_::LogicalOperator>>> &wiredLogicalProducers,
             std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> &allPhysicalOperators,
             std::shared_ptr<normal::plan::operator_::mode::Mode> mode){

  // if logicalProducer is already wired, return
  if (std::find(wiredLogicalProducers->begin(), wiredLogicalProducers->end(), logicalProducer) != wiredLogicalProducers->end()) {
    return;
  }

  // extract stream-out physical operators
  std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> streamOutPhysicalOperators;

  if (logicalProducer->type()->is(operator_::type::OperatorTypes::joinOperatorType())){
    // get join physical operators
    auto joinPhysicalOperators = toPhysicalOperators(logicalProducer, logicalToPhysical_map, allPhysicalOperators);

    // joinProbes are stream-out operators
    streamOutPhysicalOperators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>(
            joinPhysicalOperators->begin() + joinPhysicalOperators->size() / 2, joinPhysicalOperators->end());
  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::aggregateOperatorType())) {
    // get aggregate physical operators
    std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> aggregatePhysicalOperators;
    auto aggregatePhysicalOperators_pair = logicalToPhysical_map->find(logicalProducer);
    if (aggregatePhysicalOperators_pair == logicalToPhysical_map->end()) {
      auto aggregateLogicalOperator = std::static_pointer_cast<normal::plan::operator_::AggregateLogicalOperator>(logicalProducer);
      auto producerOfAggregateLogicalOperator = aggregateLogicalOperator->getProducer();
      wireUp(producerOfAggregateLogicalOperator,
             logicalProducer,
             logicalToPhysical_map,
             wiredLogicalProducers,
             allPhysicalOperators,
             mode);
      aggregatePhysicalOperators = logicalToPhysical_map->find(logicalProducer)->second;
    } else {
      aggregatePhysicalOperators = aggregatePhysicalOperators_pair->second;
    }

    // the single aggregate or aggregateReduce is stream-out operator
    if (aggregatePhysicalOperators->size() == 1) {
      streamOutPhysicalOperators = aggregatePhysicalOperators;
    } else {
      streamOutPhysicalOperators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
      streamOutPhysicalOperators->emplace_back(aggregatePhysicalOperators->back());
    }
  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::groupOperatorType())) {
    // get group physical operators
    std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> groupPhysicalOperators;
    auto groupPhysicalOperators_pair = logicalToPhysical_map->find(logicalProducer);
    if (groupPhysicalOperators_pair == logicalToPhysical_map->end()) {
      auto groupLogicalOperator = std::static_pointer_cast<normal::plan::operator_::GroupLogicalOperator>(logicalProducer);
      auto producerOfGroupLogicalOperator = groupLogicalOperator->getProducer();
      wireUp(producerOfGroupLogicalOperator,
             logicalProducer,
             logicalToPhysical_map,
             wiredLogicalProducers,
             allPhysicalOperators,
             mode);
      groupPhysicalOperators = logicalToPhysical_map->find(logicalProducer)->second;
    } else {
      groupPhysicalOperators = groupPhysicalOperators_pair->second;
    }

    // the single group or groupReduce is stream-out operator
    if (groupPhysicalOperators->size() == 1) {
      streamOutPhysicalOperators = groupPhysicalOperators;
    } else {
      streamOutPhysicalOperators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
      streamOutPhysicalOperators->emplace_back(groupPhysicalOperators->back());
    }
  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::projectOperatorType())) {
    // get project physical operators
    std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> projectPhysicalOperators;
    auto projectPhysicalOperators_pair = logicalToPhysical_map->find(logicalProducer);
    if (projectPhysicalOperators_pair == logicalToPhysical_map->end()) {
      auto projectLogicalOperator = std::static_pointer_cast<normal::plan::operator_::ProjectLogicalOperator>(logicalProducer);
      auto producerOfProjectLogicalOperator = projectLogicalOperator->getProducer();
      wireUp(producerOfProjectLogicalOperator, logicalProducer, logicalToPhysical_map, wiredLogicalProducers, allPhysicalOperators, mode);
      projectPhysicalOperators = logicalToPhysical_map->find(logicalProducer)->second;
    } else {
      projectPhysicalOperators = projectPhysicalOperators_pair->second;
    }

    // all project physical operators are stream-out operators
    streamOutPhysicalOperators = projectPhysicalOperators;
  }

  else if (logicalProducer->type()->is(operator_::type::OperatorTypes::scanOperatorType())) {
    switch (mode->id()) {
      case normal::plan::operator_::mode::FullPushdown:
        streamOutPhysicalOperators = toPhysicalOperators(logicalProducer, logicalToPhysical_map, allPhysicalOperators);
        break;

      case normal::plan::operator_::mode::FullPullup:
      case normal::plan::operator_::mode::PullupCaching:
      case normal::plan::operator_::mode::HybridCaching:
      case normal::plan::operator_::mode::HybridCachingLast: {
        // get scan physical operators
        toPhysicalOperators(logicalProducer, logicalToPhysical_map, allPhysicalOperators);
        // get stream-out operators
        auto scanLogicalOperator = std::static_pointer_cast<operator_::ScanLogicalOperator>(logicalProducer);
        streamOutPhysicalOperators = scanLogicalOperator->streamOutPhysicalOperators();
        break;
      }

      default:
        throw std::domain_error("Unrecognized mode '" + mode->toString() + "'");
    }
  }

  else {
    std::runtime_error("Bad logicalProducer type: '" + logicalProducer->type()->toString() + "', check logical plan");
  }


  // wire up to stream-in physical operators
  if (logicalConsumer->type()->is(operator_::type::OperatorTypes::joinOperatorType())) {
    auto joinPhysicalOperators = toPhysicalOperators(logicalConsumer, logicalToPhysical_map, allPhysicalOperators);
    auto joinLogicalOperator = std::static_pointer_cast<normal::plan::operator_::JoinLogicalOperator>(logicalConsumer);
    auto leftColumnName = joinLogicalOperator->getLeftColumnName();
    auto rightColumnName = joinLogicalOperator->getRightColumnName();

    // joinBuilds and joinProbes
    auto joinBuilds = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>(
            joinPhysicalOperators->begin(), joinPhysicalOperators->begin() + joinPhysicalOperators->size() / 2);
    auto joinProbes = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>(
            joinPhysicalOperators->begin() + joinPhysicalOperators->size() / 2, joinPhysicalOperators->end());

    // check logicalComsumer is left consumer or right consumer
    if (logicalProducer == joinLogicalOperator->getLeftProducer()) {
      // if more than 1 join build/probe, need shuffle. Construct a shuffle physical operator for each stream-out operator
      if (joinBuilds->size() > 1) {
        for (const auto &streamOutPhysicalOperator: *streamOutPhysicalOperators) {
          auto shuffle = normal::pushdown::shuffle::Shuffle::make(streamOutPhysicalOperator->name() + "-shuffle",
                                                                  leftColumnName,
                                                                  Planner::getQueryId());
          // wire up
          streamOutPhysicalOperator->produce(shuffle);
          shuffle->consume(streamOutPhysicalOperator);
          for (const auto &joinBuild: *joinBuilds) {
            shuffle->produce(joinBuild);
            joinBuild->consume(shuffle);
          }
          allPhysicalOperators->emplace_back(shuffle);
        }
      } else {
        auto joinBuild = joinBuilds->at(0);
        for (const auto &streamOutPhysicalOperator: *streamOutPhysicalOperators) {
          streamOutPhysicalOperator->produce(joinBuild);
          joinBuild->consume(streamOutPhysicalOperator);
        }
      }
    }
    else {
      // if more than 1 join build/probe, need shuffle. Construct a shuffle physical operator for each stream-out operator
      if (joinProbes->size() > 1) {
        for (const auto &streamOutPhysicalOperator: *streamOutPhysicalOperators) {
          auto shuffle = normal::pushdown::shuffle::Shuffle::make(streamOutPhysicalOperator->name() + "-shuffle",
                                                                  rightColumnName,
                                                                  Planner::getQueryId());
          // wire up
          streamOutPhysicalOperator->produce(shuffle);
          shuffle->consume(streamOutPhysicalOperator);
          for (const auto &joinProbe: *joinProbes) {
            shuffle->produce(joinProbe);
            joinProbe->consume(shuffle);
          }
          allPhysicalOperators->emplace_back(shuffle);
        }
      } else {
        auto joinProbe = joinProbes->at(0);
        for (const auto &streamOutPhysicalOperator: *streamOutPhysicalOperators) {
          streamOutPhysicalOperator->produce(joinProbe);
          joinProbe->consume(streamOutPhysicalOperator);
        }
      }
    }
  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::aggregateOperatorType())){
    // get aggregate physical operators
    auto numConcurrentUnits = streamOutPhysicalOperators->size();
    std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> aggregatePhysicalOperators;
    auto aggregatePhysicalOperators_pair = logicalToPhysical_map->find(logicalConsumer);
    if (aggregatePhysicalOperators_pair == logicalToPhysical_map->end()) {
      auto aggregateLogicalOperator = std::static_pointer_cast<normal::plan::operator_::AggregateLogicalOperator>(logicalConsumer);
      aggregateLogicalOperator->setNumConcurrentUnits(numConcurrentUnits);
      aggregatePhysicalOperators = aggregateLogicalOperator->toOperators();
      logicalToPhysical_map->insert({aggregateLogicalOperator, aggregatePhysicalOperators});
      allPhysicalOperators->insert(allPhysicalOperators->end(), aggregatePhysicalOperators->begin(), aggregatePhysicalOperators->end());
    } else {
      aggregatePhysicalOperators = aggregatePhysicalOperators_pair->second;
    }

    // One-to-one wire up for all except aggregateReduce
    for (size_t index = 0; index < numConcurrentUnits; index++) {
      auto streamOutPhysicalOperator = streamOutPhysicalOperators->at(index);
      auto streamInPhysicalOperator = aggregatePhysicalOperators->at(index);
      streamOutPhysicalOperator->produce(streamInPhysicalOperator);
      streamInPhysicalOperator->consume(streamOutPhysicalOperator);
    }
  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::groupOperatorType())) {
    // get group physical operators
    auto numConcurrentUnits = streamOutPhysicalOperators->size();
    std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> groupPhysicalOperators;
    auto groupPhysicalOperators_pair = logicalToPhysical_map->find(logicalConsumer);
    if (groupPhysicalOperators_pair == logicalToPhysical_map->end()) {
      auto groupLogicalOperator = std::static_pointer_cast<normal::plan::operator_::GroupLogicalOperator>(logicalConsumer);
      groupLogicalOperator->setNumConcurrentUnits(numConcurrentUnits);
      groupPhysicalOperators = groupLogicalOperator->toOperators();
      logicalToPhysical_map->insert({groupLogicalOperator, groupPhysicalOperators});
      allPhysicalOperators->insert(allPhysicalOperators->end(), groupPhysicalOperators->begin(), groupPhysicalOperators->end());
    } else {
      groupPhysicalOperators = groupPhysicalOperators_pair->second;
    }

    // One-to-one wire up for all except groupReduce
    for (size_t index = 0; index < numConcurrentUnits; index++) {
      auto streamOutPhysicalOperator = streamOutPhysicalOperators->at(index);
      auto streamInPhysicalOperator = groupPhysicalOperators->at(index);
      streamOutPhysicalOperator->produce(streamInPhysicalOperator);
      streamInPhysicalOperator->consume(streamOutPhysicalOperator);
    }
  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::projectOperatorType())) {
    // get project physical operators
    auto numConcurrentUnits = streamOutPhysicalOperators->size();
    std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> projectPhysicalOperators;
    auto projectPhysicalOperators_pair = logicalToPhysical_map->find(logicalConsumer);
    if (projectPhysicalOperators_pair == logicalToPhysical_map->end()) {
      auto projectLogicalOperator = std::static_pointer_cast<normal::plan::operator_::ProjectLogicalOperator>(logicalConsumer);
      projectLogicalOperator->setNumConcurrentUnits(numConcurrentUnits);
      projectPhysicalOperators = projectLogicalOperator->toOperators();
      logicalToPhysical_map->insert({projectLogicalOperator, projectPhysicalOperators});
      allPhysicalOperators->insert(allPhysicalOperators->end(), projectPhysicalOperators->begin(), projectPhysicalOperators->end());
    } else {
      projectPhysicalOperators = projectPhysicalOperators_pair->second;
    }

    // One-to-one wire up for all
    for (size_t index = 0; index < numConcurrentUnits; index++) {
      auto streamOutPhysicalOperator = streamOutPhysicalOperators->at(index);
      auto streamInPhysicalOperator = projectPhysicalOperators->at(index);
      streamOutPhysicalOperator->produce(streamInPhysicalOperator);
      streamInPhysicalOperator->consume(streamOutPhysicalOperator);
    }
  }

  else if (logicalConsumer->type()->is(operator_::type::OperatorTypes::collateOperatorType())){
    auto collatePhysicalOperator = toPhysicalOperators(logicalConsumer, logicalToPhysical_map, allPhysicalOperators)->at(0);
    for (const auto &streamOutPhysicalOperator: *streamOutPhysicalOperators) {
      streamOutPhysicalOperator->produce(collatePhysicalOperator);
      collatePhysicalOperator->consume(streamOutPhysicalOperator);
    }
  }

  else {
    std::runtime_error("Bad logicalConsumer type: '" + logicalConsumer->type()->toString() + "', check logical plan");
  }

}

// Uniform-hash placement
std::shared_ptr<PhysicalPlan> generateDistributedPlacements(std::unordered_map<std::string, std::shared_ptr<Operator>>& nameToOperators,
                                                 int numNodes) {
  auto physicalPlan = std::make_shared<PhysicalPlan>();
  std::vector<std::string> rest;

  while (!nameToOperators.empty()) {
    auto next = nameToOperators.begin();
    auto name = next->first;

    if (name == "collate" || name == "groupReduce" || name == "aggregateReduce") {
      physicalPlan->put(next->second, 0);
      nameToOperators.erase(name);
    }

    else if (name.substr(0, 6) == "group-" || name.substr(0, 10) == "aggregate-" || name.substr(0, 5) == "proj-") {
      name = (name.substr(0, 6) == "group-") ? "group-" : ((name.substr(0, 10) == "aggregate-") ? "aggregate-" : "proj-");
      for (int i = 0; ; i++) {
        std::string nameExt = name + std::to_string(i);
        auto entry = nameToOperators.find(nameExt);
        if (entry == nameToOperators.end()) break;
        physicalPlan->put(entry->second, i % numNodes);
        nameToOperators.erase(nameExt);
      }
    }

    else if (name.substr(0, 5) == "join-") {
      std::string joinAttribute = (name.substr(name.length() - 7, 7) == "shuffle") ?
              name.substr(11, name.length() - 11 - 7 - 1): name.substr(11);
      joinAttribute = joinAttribute.substr(0, joinAttribute.find_last_of('-'));
      for (int i = 0; ; i++) {
        std::string nameExtBuild = "join-build-" + joinAttribute + "-" + std::to_string(i);
        std::string nameExtProbe = "join-probe-" + joinAttribute + "-" + std::to_string(i);
        std::string nameExtShuffle = "join-probe-" + joinAttribute + "-" + std::to_string(i) + "-shuffle";
        if (nameToOperators.find(nameExtBuild) == nameToOperators.end()) break;
        physicalPlan->put(nameToOperators.find(nameExtBuild)->second, i % numNodes);
        nameToOperators.erase(nameExtBuild);
        physicalPlan->put(nameToOperators.find(nameExtProbe)->second, i % numNodes);
        nameToOperators.erase(nameExtProbe);
        if (nameToOperators.find(nameExtShuffle) != nameToOperators.end()) {
          physicalPlan->put(nameToOperators.find(nameExtShuffle)->second, i % numNodes);
          nameToOperators.erase(nameExtShuffle);
        }
      }
    }

    else if (name.substr(0, 10) == "s3select -") {
      // TODO: following is a start to support range scan inside a partition
      std::string scanTableName = (name.substr(name.length() - 7, 7) == "shuffle") ?
              name.substr(11, name.length() - 11 - 7 - 1): name.substr(11);
      scanTableName = scanTableName.substr(0, scanTableName.find_last_of('-'));
      int lastDotIndex = scanTableName.find_last_of('.');
      bool hasMultiPartition = normal::util::isInteger(scanTableName.substr(lastDotIndex + 1));
      scanTableName = hasMultiPartition ? scanTableName.substr(0, lastDotIndex) : scanTableName;
      int cntRange = 0;

      if (hasMultiPartition) {
        for (int i = 0;; i++) {      // partition level
          bool partitionExist = true;
          for (int j = 0;; j++) {    // range level
            std::string nameExtS3Select = "s3select - " + scanTableName + "." + std::to_string(i) + "-" + std::to_string(j);
            std::string nameExtShuffle = "s3select - " + scanTableName + "." + std::to_string(i) + "-" + std::to_string(j) + "-shuffle";
            if (nameToOperators.find(nameExtS3Select) == nameToOperators.end()) {
              if (j == 0) partitionExist = false;
              break;
            }
            physicalPlan->put(nameToOperators.find(nameExtS3Select)->second, cntRange % numNodes);
            nameToOperators.erase(nameExtS3Select);
            if (nameToOperators.find(nameExtShuffle) != nameToOperators.end()) {
              physicalPlan->put(nameToOperators.find(nameExtShuffle)->second, cntRange % numNodes);
              nameToOperators.erase(nameExtShuffle);
            }
            cntRange++;
          }
          if (!partitionExist) break;
        }
      }

      else {
        for (int i = 0;; i++) {      // range level
          std::string nameExtS3Select = "s3select - " + scanTableName + "-" + std::to_string(i);
          std::string nameExtShuffle = "s3select - " + scanTableName + "-" + std::to_string(i) + "-shuffle";
          if (nameToOperators.find(nameExtS3Select) == nameToOperators.end()) break;
          physicalPlan->put(nameToOperators.find(nameExtS3Select)->second, cntRange % numNodes);
          nameToOperators.erase(nameExtS3Select);
          if (nameToOperators.find(nameExtShuffle) != nameToOperators.end()) {
            physicalPlan->put(nameToOperators.find(nameExtShuffle)->second, cntRange % numNodes);
            nameToOperators.erase(nameExtShuffle);
          }
          cntRange++;
        }
      }
    }


    else {
      nameToOperators.erase(name);
      rest.emplace_back(name);
    }
  }

  return physicalPlan;
}

std::shared_ptr<PhysicalPlan> Planner::generate (const LogicalPlan &logicalPlan,
                                                 std::shared_ptr<normal::plan::operator_::mode::Mode> mode,
                                                 int numNodes) {
  auto logicalOperators = logicalPlan.getOperators();
  auto logicalToPhysical_map = std::make_shared<std::unordered_map<
          std::shared_ptr<normal::plan::operator_::LogicalOperator>,
          std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>>>();
  auto wiredLogicalProducers = std::make_shared<std::vector<std::shared_ptr<normal::plan::operator_::LogicalOperator>>>();
  auto allPhysicalOperators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();

  for (auto &logicalProducer: *logicalOperators) {
    auto logicalConsumer = logicalProducer->getConsumer();
    if (logicalConsumer) {
      wireUp(logicalProducer,
             logicalConsumer,
             logicalToPhysical_map,
             wiredLogicalProducers,
             allPhysicalOperators,
             mode);
    }
  }

  std::shared_ptr<PhysicalPlan> physicalPlan;
  if (numNodes == 1) {
    physicalPlan = std::make_shared<PhysicalPlan>();
    for (const auto &physicalOperator: *allPhysicalOperators) {
      physicalPlan->put(physicalOperator, 0);
    }
  }
  else {
    std::unordered_map<std::string, std::shared_ptr<Operator>> nameToOperators;
    for (const auto &physicalOperator: *allPhysicalOperators) {
      nameToOperators.emplace(physicalOperator->name(), physicalOperator);
    }
    physicalPlan = generateDistributedPlacements(nameToOperators, numNodes);
  }

  return physicalPlan;
}

void Planner::setQueryId(long queryId) {
  queryId_ = queryId;
}

long Planner::getQueryId() {
  return queryId_;
}
