//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_COLLATELOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_COLLATELOGICALOPERATOR_H

#include <memory>

#include <normal/core/Operator.h>
#include <normal/plan/operator_/LogicalOperator.h>

namespace normal::plan::operator_ {

class CollateLogicalOperator : public LogicalOperator {
public:

  CollateLogicalOperator();

  std::vector<std::pair<std::shared_ptr<core::Operator>, int> > toOperatorsWithPlacementsUniHash(int numNodes) override;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_COLLATELOGICALOPERATOR_H
