//
// Created by matt on 11/6/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_AND_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_AND_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"
#include "BinaryExpression.h"

namespace normal::expression::gandiva {

class And : public BinaryExpression {

public:
  And(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);
  And() = default;
  And(const And&) = default;
  And& operator=(const And&) = default;

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, And& exp) {
    return f.object(exp).fields(f.field("left", exp.left_),
                                f.field("right", exp.right_),
                                f.field("expType", exp.expType_));
  }
};

std::shared_ptr<Expression> and_(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_AND_H
