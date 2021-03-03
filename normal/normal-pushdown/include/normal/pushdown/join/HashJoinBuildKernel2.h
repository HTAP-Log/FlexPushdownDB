//
// Created by matt on 1/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINBUILDKERNEL2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINBUILDKERNEL2_H

#include <string>
#include <memory>

#include <normal/pushdown/join/ATTIC/HashTable.h>
#include "normal/tuple/TupleSetIndex.h"

namespace normal::pushdown::join {

/**
 * Kernel for creating the hash table on the build relation in a hash join
 */
class HashJoinBuildKernel2 {

public:
  explicit HashJoinBuildKernel2(std::string columnName);
  HashJoinBuildKernel2() = default;
  HashJoinBuildKernel2(const HashJoinBuildKernel2&) = default;
  HashJoinBuildKernel2& operator=(const HashJoinBuildKernel2&) = default;
  static HashJoinBuildKernel2 make(const std::string &columnName);

  [[nodiscard]] tl::expected<void,std::string> put(const std::shared_ptr<TupleSet2> &tupleSet);
  size_t size();
  void clear();
  std::optional<std::shared_ptr<TupleSetIndex>> getTupleSetIndex();

private:

  /**
   * The column to hash on
   */
  std::string columnName_;

  /**
   * The hashtable as an indexed tupleset
   */
  std::optional<std::shared_ptr<TupleSetIndex>> tupleSetIndex_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinBuildKernel2& kernel) {
    return f.object(kernel).fields(f.field("columnName", kernel.columnName_));
  }

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINBUILDKERNEL2_H
