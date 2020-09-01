//
// Created by matt on 8/7/20.
//

#include "normal/pushdown/cache/CacheLoad.h"

#include <normal/pushdown/cache/CacheHelper.h>
#include <normal/pushdown/TupleMessage.h>
#include <normal/pushdown/scan/ScanMessage.h>

#include <utility>

using namespace normal::pushdown::cache;
using namespace normal::core::message;
using namespace normal::pushdown::scan;

CacheLoad::CacheLoad(std::string name,
           std::vector<std::string> projectedColumnNames,
           std::vector<std::string> predicateColumnNames,
					 std::shared_ptr<Partition> Partition,
					 int64_t StartOffset,
					 int64_t FinishOffset,
					 bool useNewCacheLayout) :
					 Operator(std::move(name), "CacheLoad"),
					 projectedColumnNames_(projectedColumnNames),
					 predicateColumnNames_(predicateColumnNames),
					 partition_(std::move(Partition)),
					 startOffset_(StartOffset),
					 finishOffset_(FinishOffset),
					 useNewCacheLayout_(useNewCacheLayout) {
  auto allColumnNames_ = std::make_shared<std::vector<std::string>>();
  allColumnNames_->insert(allColumnNames_->end(), projectedColumnNames.begin(), projectedColumnNames.end());
  allColumnNames_->insert(allColumnNames_->end(), predicateColumnNames.begin(), predicateColumnNames.end());
  auto columnNameSet = std::make_shared<std::set<std::string>>(allColumnNames_->begin(), allColumnNames_->end());
  columnNames_.assign(columnNameSet->begin(), columnNameSet->end());
}

std::shared_ptr<CacheLoad> CacheLoad::make(const std::string &name,
                       std::vector<std::string> projectedColumnNames,
                       std::vector<std::string> predicateColumnNames,
										   const std::shared_ptr<Partition> &partition,
										   int64_t startOffset,
										   int64_t finishOffset,
										   bool useNewCacheLayout) {

  std::vector<std::string> canonicalProjectedColumnNames;
  std::vector<std::string> canonicalPredicateColumnNames;
  std::transform(projectedColumnNames.begin(), projectedColumnNames.end(),
                 std::back_inserter(canonicalProjectedColumnNames),
                 [](auto name) -> auto { return ColumnName::canonicalize(name); });
  std::transform(predicateColumnNames.begin(), predicateColumnNames.end(),
                 std::back_inserter(canonicalPredicateColumnNames),
                 [](auto name) -> auto { return ColumnName::canonicalize(name); });

  return std::make_shared<CacheLoad>(name,
									 canonicalProjectedColumnNames,
									 canonicalPredicateColumnNames,
									 partition,
									 startOffset,
									 finishOffset,
									 useNewCacheLayout);
}

void CacheLoad::onReceive(const Envelope &message) {
  if (message.message().type() == "StartMessage") {
	this->onStart();
  } else if (message.message().type() == "LoadResponseMessage") {
	auto loadResponseMessage = dynamic_cast<const LoadResponseMessage &>(message.message());
	this->onCacheLoadResponse(loadResponseMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void CacheLoad::onStart() {
  /**
   * We always have hitOperator_ and missOperatorToCache_
   * In hybrid caching, we also have missOperatorToPushdown_; in pullup caching, we don't
   */

  if (!hitOperator_)
	throw std::runtime_error("Hit consumer not set ");

  if (!missOperatorToCache_)
	throw std::runtime_error("Miss caching consumer not set");

  if (missOperatorToPushdown_) {
    SPDLOG_DEBUG("Starting operator  |  name: '{}', hitOperator: '{}', missOperatorToCache: '{}', missOperatorToPushdown: '{}'",
                 this->name(),
                 hitOperator_->name(),
                 missOperatorToCache_->name(),
                 missOperatorToPushdown_->name());
  } else {
    SPDLOG_DEBUG("Starting operator  |  name: '{}', hitOperator: '{}', missOperatorToCache: '{}'",
                 this->name(),
                 hitOperator_->name(),
                 missOperatorToCache_->name());
  }

  requestLoadSegmentsFromCache();
}

void CacheLoad::requestLoadSegmentsFromCache() {
  CacheHelper::requestLoadSegmentsFromCache(columnNames_, partition_, startOffset_, finishOffset_, name(), ctx());
}

void CacheLoad::onCacheLoadResponse(const LoadResponseMessage &Message) {
  std::vector<std::shared_ptr<Column>> hitColumns;
  std::vector<std::string> hitColumnNames;
  std::vector<std::string> missedCachingColumnNames;
  std::vector<std::string> missedPushdownColumnNames;

  // Gather missed caching segment columns
  auto segmentKeysToCache = Message.getSegmentKeysToCache();
  for (auto const &segmentKey: segmentKeysToCache) {
    missedCachingColumnNames.emplace_back(segmentKey->getColumnName());
  }

  auto hitSegments = Message.getSegments();
  SPDLOG_DEBUG("Loaded segments from cache  |  numRequested: {}, numHit: {}", columnNames_.size(), hitSegments.size());

  // Gather hit segment columns and missed pushdown segment columns
  for (const auto &columnName: columnNames_) {
    auto segmentKey = SegmentKey::make(partition_, columnName, SegmentRange::make(startOffset_, finishOffset_));
    auto segment = hitSegments.find(segmentKey);
    if (segment != hitSegments.end()) {
      hitColumns.emplace_back(segment->second->getColumn());
      hitColumnNames.emplace_back(columnName);
    } else if (useNewCacheLayout_) {
      if (std::find(missedCachingColumnNames.begin(), missedCachingColumnNames.end(), columnName)
               == missedCachingColumnNames.end()) {
        /**
         * A trick here: no need to check whether it's in projectedColumnNames_,
         * because if resultNeeded = true, missedPushdownColumnNames are all in projectedColumnNames_,
         * otherwise we make missedPushdownColumnNames as all projectedColumnNames_
         */
        missedPushdownColumnNames.emplace_back(columnName);
      }
	  } else {
      if (std::find(projectedColumnNames_.begin(), projectedColumnNames_.end(), columnName)
              != projectedColumnNames_.end()) {
        missedPushdownColumnNames.emplace_back(columnName);
      }
    }
  }

  // Create a tuple set from the hit segments
  auto hitTupleSet = TupleSet2::make(hitColumns);

  // Send the hit columns to the hit operator
  auto hitMessage = std::make_shared<TupleMessage>(hitTupleSet->toTupleSetV1(), this->name());
  ctx()->send(hitMessage, hitOperator_->name());

  if (useNewCacheLayout_) {
    /**
     * Caching result is not needed when:
     *    hitColumns + missCachingColumns don't cover all predicateColumns or
     *    hitColumns + missCachingColumns cover no projectedColumns
     */
    bool cachingResultNeeded = true;
    for (auto const &predicateColumnName: predicateColumnNames_) {
      if (std::find(hitColumnNames.begin(), hitColumnNames.end(), predicateColumnName) == hitColumnNames.end() &&
          std::find(missedCachingColumnNames.begin(), missedCachingColumnNames.end(), predicateColumnName) ==
          missedCachingColumnNames.end()) {
        cachingResultNeeded = false;
        break;
      }
    }
    if (cachingResultNeeded) {
      cachingResultNeeded = false;
      for (auto const &projectedColumnName: projectedColumnNames_) {
        if (std::find(hitColumnNames.begin(), hitColumnNames.end(), projectedColumnName) != hitColumnNames.end() ||
            std::find(missedCachingColumnNames.begin(), missedCachingColumnNames.end(), projectedColumnName) !=
            missedCachingColumnNames.end()) {
          cachingResultNeeded = true;
          break;
        }
      }
    }

    if (missOperatorToPushdown_) {
      // Send the missed caching column names to the miss caching operator
      auto missCachingMessage = std::make_shared<ScanMessage>(missedCachingColumnNames, this->name(), cachingResultNeeded);
      ctx()->send(missCachingMessage, missOperatorToCache_->name());

      // Send the missed pushdown column names to the miss pushdown operator
      std::shared_ptr<ScanMessage> missPushdownMessage;
      if (cachingResultNeeded) {
        missPushdownMessage = std::make_shared<ScanMessage>(missedPushdownColumnNames, this->name(), true);
      } else {
        missPushdownMessage = std::make_shared<ScanMessage>(projectedColumnNames_, this->name(), true);
      }
      ctx()->send(missPushdownMessage, missOperatorToPushdown_->name());
    } else {
      // Send the missed caching column names to the miss caching operator
      auto missCachingMessage = std::make_shared<ScanMessage>(missedCachingColumnNames, this->name(), true);
      ctx()->send(missCachingMessage, missOperatorToCache_->name());
    }
  }

  else {
    /**
     * Hit segments are useful when:
     *    hitColumns don't cover all predicateColumns or
     *    hitColumns cover no projectedColumns
     */
    bool hitSegmentsUseful = true;
    for (auto const &predicateColumnName: predicateColumnNames_) {
      if (std::find(hitColumnNames.begin(), hitColumnNames.end(), predicateColumnName) == hitColumnNames.end()) {
        hitSegmentsUseful = false;
        break;
      }
    }
    if (hitSegmentsUseful) {
      hitSegmentsUseful = false;
      for (auto const &projectedColumnName: projectedColumnNames_) {
        if (std::find(hitColumnNames.begin(), hitColumnNames.end(), projectedColumnName) != hitColumnNames.end()) {
          hitSegmentsUseful = true;
          break;
        }
      }
    }

    // Send the missed caching column names to the miss caching operator
    auto missCachingMessage = std::make_shared<ScanMessage>(missedCachingColumnNames, this->name(), false);
    ctx()->send(missCachingMessage, missOperatorToCache_->name());

    // Send the missed pushdown column names to the miss pushdown operator
    if (missOperatorToPushdown_) {
      std::shared_ptr<ScanMessage> missPushdownMessage;
      if (hitSegmentsUseful) {
        missPushdownMessage = std::make_shared<ScanMessage>(missedPushdownColumnNames, this->name(), true);
      } else {
        missPushdownMessage = std::make_shared<ScanMessage>(projectedColumnNames_, this->name(), true);
      }
      ctx()->send(missPushdownMessage, missOperatorToPushdown_->name());
    }
  }

  ctx()->notifyComplete();
}

void CacheLoad::setHitOperator(const std::shared_ptr<Operator> &op) {
  this->hitOperator_ = op;
  this->produce(op);
}

void CacheLoad::setMissOperatorToCache(const std::shared_ptr<Operator> &op) {
  this->missOperatorToCache_ = op;
  this->produce(op);
}

void CacheLoad::setMissOperatorToPushdown(const std::shared_ptr<Operator> &op) {
  this->missOperatorToPushdown_ = op;
  this->produce(op);
}
