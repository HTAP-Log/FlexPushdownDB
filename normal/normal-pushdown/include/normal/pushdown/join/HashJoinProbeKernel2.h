//
// Created by matt on 31/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL2_H


#include <memory>

#include "JoinPredicate.h"
#include "normal/pushdown/join/ATTIC/HashTable.h"
#include "normal/tuple/TupleSetIndex.h"
//#include "RecordBatchJoiner.h"
#include <set>

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::pushdown::join {

class HashJoinProbeKernel2 {

public:
  explicit HashJoinProbeKernel2(JoinPredicate pred, std::set<std::string> neededColumnNames);
  HashJoinProbeKernel2() = default;
  HashJoinProbeKernel2(const HashJoinProbeKernel2&) = default;
  HashJoinProbeKernel2& operator=(const HashJoinProbeKernel2&) = default;
  static HashJoinProbeKernel2 make(JoinPredicate pred, std::set<std::string> neededColumnNames);

  tl::expected<void, std::string> joinBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex>& tupleSetIndex);
  tl::expected<void, std::string> joinProbeTupleSet(const std::shared_ptr<TupleSet2>& tupleSet);
  const std::optional<std::shared_ptr<normal::tuple::TupleSet2>> &getBuffer() const;
  void clear();

private:
  JoinPredicate pred_;
  std::optional<std::shared_ptr<TupleSetIndex>> buildTupleSetIndex_;
  std::optional<std::shared_ptr<TupleSet2>> probeTupleSet_;
  std::set<std::string> neededColumnNames_;
  std::optional<std::shared_ptr<::arrow::Schema>> outputSchema_;
  std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice_;   // <true/false, i> -> ith column in build/probe table
  std::optional<std::shared_ptr<normal::tuple::TupleSet2>> buffer_;

  [[nodiscard]] tl::expected<void, std::string> putBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex>& tupleSetIndex);
  [[nodiscard]] tl::expected<void, std::string> putProbeTupleSet(const std::shared_ptr<TupleSet2>& tupleSet);
  [[nodiscard]] tl::expected<void, std::string> buffer(const std::shared_ptr<TupleSet2>& tupleSet);
  void bufferOutputSchema(const std::shared_ptr<TupleSetIndex> &tupleSetIndex, const std::shared_ptr<TupleSet2> &tupleSet);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinProbeKernel2& kernel) {
    return f.object(kernel).fields(f.field("pred", kernel.pred_),
                                   f.field("neededColumnNames", kernel.neededColumnNames_));
  }
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL2_H
