//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_COLLATELOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_SRC_AST_COLLATELOGICALOPERATOR_H

#include <memory>
#include <normal/core/Operator.h>
#include "logical/LogicalOperator.h"

class CollateLogicalOperator : public LogicalOperator {
public:
  std::shared_ptr<normal::core::Operator> toOperator() override;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_COLLATELOGICALOPERATOR_H
