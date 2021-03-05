//
// Created by matt on 8/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_CACHE_CACHELOAD_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_CACHE_CACHELOAD_H

#include <normal/core/Operator.h>
#include <normal/core/message/Envelope.h>
#include <normal/connector/partition/Partition.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/connector/serialization/PartitionSer.h>

using namespace normal::core;
using namespace normal::core::message;
using namespace normal::core::cache;

namespace normal::pushdown::cache {

class CacheLoad : public Operator {

public:
  explicit CacheLoad(std::string name,
					 std::vector<std::string> projectedColumnNames,
					 std::vector<std::string> predicateColumnNames,
					 std::shared_ptr<Partition> partition,
					 int64_t startOffset,
					 int64_t finishOffset,
					 bool useNewCacheLayout,
					 long queryId);
  CacheLoad() = default;
  CacheLoad(const CacheLoad&) = default;
  CacheLoad& operator=(const CacheLoad&) = default;
  ~CacheLoad() override = default;

  static std::shared_ptr<CacheLoad> make(const std::string &name,
                     std::vector<std::string> projectedColumnNames,
                     std::vector<std::string> predicateColumnNames,
										 const std::shared_ptr<Partition>& partition,
										 int64_t startOffset,
										 int64_t finishOffset,
										 bool useNewCacheLayout,
										 long queryId = 0);

  void onStart();
  void onReceive(const Envelope &msg) override;

  void setHitOperator(const std::shared_ptr<Operator> &op);
  void setMissOperatorToCache(const std::shared_ptr<Operator> &op);
  void setMissOperatorToPushdown(const std::shared_ptr<Operator> &op);

private:
  /**
   * columnNames = projectedColumnNames + predicateColumnNames
   */
  std::vector<std::string> columnNames_;
  std::vector<std::string> projectedColumnNames_;
  std::vector<std::string> predicateColumnNames_;

  std::shared_ptr<Partition> partition_;
  int64_t startOffset_;
  int64_t finishOffset_;

  std::optional<std::string> hitOperatorName_;
  std::optional<std::string> missOperatorToCacheName_;
  std::optional<std::string> missOperatorToPushdownName_;

  /**
   * whether to use the new cache layout after segments back or the last one without waiting
   */
  bool useNewCacheLayout_;

  void requestLoadSegmentsFromCache();
  void onCacheLoadResponse(const LoadResponseMessage &Message);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CacheLoad& op) {
    return f.object(op).fields(f.field("columnNames", op.columnNames_),
                               f.field("projectedColumnNames", op.projectedColumnNames_),
                               f.field("predicatedColumnNames", op.predicateColumnNames_),
                               f.field("partition", op.partition_),
                               f.field("startOffset", op.startOffset_),
                               f.field("finishOffset", op.finishOffset_),
                               f.field("hitOperatorName", op.hitOperatorName_),
                               f.field("missOperatorToCacheName", op.missOperatorToCacheName_),
                               f.field("missOperatorToPushdownName", op.missOperatorToPushdownName_),
                               f.field("useNewCacheLayout", op.useNewCacheLayout_),
                               f.field("name", op.name()),
                               f.field("type", op.getType()),
                               f.field("opContext", op.getOpContext()),
                               f.field("producers", op.getProducers()),
                               f.field("consumers", op.getConsumers()));
  }
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_CACHE_CACHELOAD_H
